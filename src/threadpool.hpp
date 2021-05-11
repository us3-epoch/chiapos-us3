#ifndef SRC_CPP_THREAD_POOL_HPP_
#define SRC_CPP_THREAD_POOL_HPP_

#include <fstream>
#include <functional>
#include <future>
#include <mutex>
#include <queue>
#include <thread>
#include <utility>
#include <vector>

class DiskProver;

template <typename T>
class SafeQueue {
private:
    std::queue<T> m_queue;
    std::mutex m_mutex;

public:
    SafeQueue() {}

    SafeQueue(SafeQueue& other) {}

    ~SafeQueue() {}

    bool empty()
    {
        std::unique_lock<std::mutex> lock(m_mutex);
        return m_queue.empty();
    }

    int size()
    {
        std::unique_lock<std::mutex> lock(m_mutex);
        return m_queue.size();
    }

    void enqueue(T& t)
    {
        std::unique_lock<std::mutex> lock(m_mutex);
        m_queue.push(t);
    }

    bool dequeue(T& t)
    {
        std::unique_lock<std::mutex> lock(m_mutex);
        if (m_queue.empty()) {
            return false;
        }
        t = std::move(m_queue.front());
        m_queue.pop();
        return true;
    }
};
struct TaskReq {
    std::string filename;
    uint64_t position;
    std::vector<uint64_t> table_begin_pointers;
    uint8_t depth;
    uint8_t k;
};

struct TaskResp {
    std::vector<Bits> value;
    int ec;
    string msg;
};

struct Task {
    std::packaged_task<TaskResp*(TaskReq*)> package;
    TaskReq* req;
};

std::vector<Bits> GetInputs(
    std::string filename,
    uint64_t position,
    std::vector<uint64_t>& table_begin_pointers,
    uint8_t depth,
    uint8_t k);
uint128_t ReadLinePoint(
    std::ifstream& disk_file,
    std::vector<uint64_t>& table_begin_pointers,
    uint8_t table_index,
    uint64_t position,
    uint8_t k);

class ThreadPool {
private:
    class ThreadWorker {
    private:
        int m_id;
        ThreadPool* m_pool;

    public:
        ThreadWorker(ThreadPool* pool, const int id) : m_pool(pool), m_id(id) {}

        void operator()()
        {
            Task* task;
            bool dequeued;

            while (!m_pool->m_shutdown) {
                {
                    std::unique_lock<std::mutex> lock(m_pool->m_conditional_mutex);

                    if (m_pool->m_queue.empty()) {
                        m_pool->m_conditional_lock.wait(lock);
                    }

                    dequeued = m_pool->m_queue.dequeue(task);
                }

                if (dequeued) {
                    task->package(task->req);
                    delete task;
                }
            }
        }
    };

    bool m_shutdown;

    SafeQueue<Task*> m_queue;
    std::vector<std::thread> m_threads;
    std::mutex m_conditional_mutex;
    std::condition_variable m_conditional_lock;

public:
    ThreadPool(const int n_threads)
        : m_threads(std::vector<std::thread>(n_threads)), m_shutdown(false)
    {
        init();
    }

    ~ThreadPool() {
        shutdown();
    }

    ThreadPool(const ThreadPool&) = delete;
    ThreadPool(ThreadPool&&) = delete;
    ThreadPool& operator=(const ThreadPool&) = delete;
    ThreadPool& operator=(ThreadPool&&) = delete;

    void init()
    {
        for (int i = 0; i < m_threads.size(); ++i) {
            m_threads[i] = std::thread(ThreadWorker(this, i));
        }
    }

    void shutdown()
    {
        m_shutdown = true;
        m_conditional_lock.notify_all();

        for (int i = 0; i < m_threads.size(); ++i) {
            if (m_threads[i].joinable()) {
                m_threads[i].join();
            }
        }
    }

    auto submit(
        std::string filename,
        uint64_t position,
        std::vector<uint64_t>& table_begin_pointers,
        uint8_t depth,
        uint8_t k)
    {
        TaskReq* req = new TaskReq();
        req->filename = filename;
        req->position = position;
        req->table_begin_pointers = table_begin_pointers;
        req->depth = depth;
        req->k = k;

        std::packaged_task<TaskResp*(TaskReq*)> package(work);
        std::future<TaskResp*>* fu = new std::future<TaskResp*>(package.get_future());
        Task* task = new Task();
        task->package = std::move(package);
        task->req = req;
        m_queue.enqueue(task);
        m_conditional_lock.notify_one();
        return fu;
    }

    static TaskResp* work(TaskReq* req)
    {
        TaskResp* rsp = new TaskResp();
        rsp->ec = 0;
        rsp->msg = "";
        try {
            rsp->value = GetInputs(
                req->filename, req->position, req->table_begin_pointers, req->depth, req->k);
        } catch (const std::exception& e) {
            rsp->ec = 1;
            rsp->msg = e.what();
        }

        delete req;
        return rsp;
    }
};

ThreadPool pool(64);
// by yeon.guo
// TODO. pool.shutdown

// GetInputs for thread pool
std::vector<Bits> GetInputs(
    std::string filename,
    uint64_t position,
    std::vector<uint64_t>& table_begin_pointers,
    uint8_t depth,
    uint8_t k)
{
    std::ifstream disk_file(filename, std::ios::in | std::ios::binary);

    if (!disk_file.is_open()) {
        throw std::invalid_argument("Invalid file " + filename);
    }

    uint128_t line_point = ReadLinePoint(disk_file, table_begin_pointers, depth, position, k);
    std::pair<uint64_t, uint64_t> xy = Encoding::LinePointToSquare(line_point);

    if (depth == 1) {
        // For table P1, the line point represents two concatenated x values.
        std::vector<Bits> ret;
        ret.emplace_back(xy.second, k);  // y
        ret.emplace_back(xy.first, k);   // x
        return ret;
    } else {
        auto fu_left = pool.submit(filename, xy.second, table_begin_pointers, depth - 1, k);
        auto fu_right = pool.submit(filename, xy.first, table_begin_pointers, depth - 1, k);
        auto rsp_left = fu_left->get();
        auto rsp_right = fu_right->get();

        if (rsp_left->ec != 0) {
            throw std::logic_error("get inputs from pool failed, error_msg " + rsp_left->msg); 
        }
        if (rsp_right->ec != 0) {
            throw std::logic_error("get inputs from pool failed, error_msg " + rsp_right->msg); 
        }
        std::vector<Bits> left = rsp_left->value;    // y
        std::vector<Bits> right = rsp_right->value;  // x
        left.insert(left.end(), right.begin(), right.end());

        return left;
    }
}

uint128_t ReadLinePoint(
    std::ifstream& disk_file,
    std::vector<uint64_t>& table_begin_pointers,
    uint8_t table_index,
    uint64_t position,
    uint8_t k)
{
    uint64_t total_size{0};

    uint64_t park_index = position / kEntriesPerPark;
    uint32_t park_size_bits = EntrySizes::CalculateParkSize(k, table_index) * 8;

    disk_file.seekg(table_begin_pointers[table_index] + (park_size_bits / 8) * park_index);

    // by yeon.guo
    auto* buffer = new uint8_t[10240];
    disk_file.read(reinterpret_cast<char*>(buffer), 10240);
    if (disk_file.eof() || disk_file.fail()) {
        throw std::invalid_argument("read disk file failed");
    }

    // This is the checkpoint at the beginning of the park
    uint16_t line_point_size = EntrySizes::CalculateLinePointSize(k);
    auto* line_point_bin = new uint8_t[line_point_size + 7];
    // disk_file.read(reinterpret_cast<char*>(line_point_bin), line_point_size);
    memcpy(
        reinterpret_cast<char*>(line_point_bin),
        reinterpret_cast<char*>(buffer) + total_size,
        line_point_size);
    uint128_t line_point = Util::SliceInt128FromBytes(line_point_bin, 0, k * 2);
    total_size += line_point_size;

    // Reads EPP stubs
    uint32_t stubs_size_bits = EntrySizes::CalculateStubsSize(k) * 8;
    auto* stubs_bin = new uint8_t[stubs_size_bits / 8 + 7];
    // disk_file.read(reinterpret_cast<char*>(stubs_bin), stubs_size_bits / 8);
    memcpy(
        reinterpret_cast<char*>(stubs_bin),
        reinterpret_cast<char*>(buffer) + total_size,
        stubs_size_bits / 8);
    total_size += stubs_size_bits / 8;

    // Reads EPP deltas
    uint32_t max_deltas_size_bits = EntrySizes::CalculateMaxDeltasSize(k, table_index) * 8;
    auto* deltas_bin = new uint8_t[max_deltas_size_bits / 8];

    // Reads the size of the encoded deltas object
    uint16_t encoded_deltas_size = 0;
    // disk_file.read(reinterpret_cast<char*>(&encoded_deltas_size), sizeof(uint16_t));
    memcpy(
        reinterpret_cast<char*>(&encoded_deltas_size),
        reinterpret_cast<char*>(buffer) + total_size,
        sizeof(uint16_t));
    total_size += sizeof(uint16_t);

    std::vector<uint8_t> deltas;

    if (0x8000 & encoded_deltas_size) {
        // Uncompressed
        encoded_deltas_size &= 0x7fff;
        deltas.resize(encoded_deltas_size);
        // disk_file.read(reinterpret_cast<char*>(deltas.data()), encoded_deltas_size);
        // memcpy(
        //     reinterpret_cast<char*>(deltas.data()),
        //     reinterpret_cast<char*>(buffer) + total_size,
        //     encoded_deltas_size);

        // by yeon.guo
        // uncompressed data cannot calculate length, read from disk
        disk_file.seekg(
            table_begin_pointers[table_index] + (park_size_bits / 8) * park_index + total_size);
        disk_file.read(reinterpret_cast<char*>(deltas.data()), encoded_deltas_size);
    } else {
        // Compressed
        // disk_file.read(reinterpret_cast<char*>(deltas_bin), encoded_deltas_size);
        memcpy(
            reinterpret_cast<char*>(deltas_bin),
            reinterpret_cast<char*>(buffer) + total_size,
            encoded_deltas_size);

        // Decodes the deltas
        double R = kRValues[table_index - 1];
        deltas = Encoding::ANSDecodeDeltas(deltas_bin, encoded_deltas_size, kEntriesPerPark - 1, R);
    }
    total_size += encoded_deltas_size;

    // std::cout << line_point_size << "," << stubs_size_bits / 8 << "," << sizeof(uint16_t) << "," << encoded_deltas_size << "," << total_size << std::endl;

    uint32_t start_bit = 0;
    uint8_t stub_size = k - kStubMinusBits;
    uint64_t sum_deltas = 0;
    uint64_t sum_stubs = 0;
    for (uint32_t i = 0;
         i < std::min((uint32_t)(position % kEntriesPerPark), (uint32_t)deltas.size());
         i++) {
        uint64_t stub = Util::EightBytesToInt(stubs_bin + start_bit / 8);
        stub <<= start_bit % 8;
        stub >>= 64 - stub_size;

        sum_stubs += stub;
        start_bit += stub_size;
        sum_deltas += deltas[i];
    }

    uint128_t big_delta = ((uint128_t)sum_deltas << stub_size) + sum_stubs;
    uint128_t final_line_point = line_point + big_delta;

    delete[] line_point_bin;
    delete[] stubs_bin;
    delete[] deltas_bin;
    delete[] buffer;

    return final_line_point;
}

#endif