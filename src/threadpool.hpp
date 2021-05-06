#include <functional>
#include <future>
#include <mutex>
#include <queue>
#include <thread>
#include <utility>
#include <vector>
#include <fstream>

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
    uint64_t length;
};
struct TaskResp {
    uint8_t *buffer;
    int ec;
};

struct Task {
    std::packaged_task<TaskResp *(TaskReq *)> package;
    TaskReq *req;
};

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
            Task *task;
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
                    // {
                    //     std::ifstream disk_file(task.filename, std::ios::in | std::ios::binary);

                    //     if (!disk_file.is_open()) {
                    //         // by yeon.guo
                    //         // TODO. dont throw execption
                    //         throw std::invalid_argument("Invalid file " + task.filename);
                    //     }

                    //     disk_file.seekg(task.position);

                    //     auto* buffer = new uint8_t[task.length];
                    //     disk_file.read(reinterpret_cast<char*>(buffer), task.length);
                    // }
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

    auto submit(std::string filename, uint64_t position, uint64_t length) {
        TaskReq *req = new TaskReq();
        req->filename = filename;
        req->position = position;
        req->length = length;

        std::packaged_task< TaskResp*(TaskReq*) > package(work);
        std::future<TaskResp *> *fu = new std::future<TaskResp *>(package.get_future());
        Task *task = new Task();
        task->package = std::move(package);
        task->req = req;
        m_queue.enqueue(task);
        m_conditional_lock.notify_one();
        return fu;
    }

    static TaskResp* work(TaskReq *task) {
        TaskResp *rsp = new TaskResp;
        rsp->ec = 0;
        {
            std::ifstream disk_file(task->filename, std::ios::in | std::ios::binary);

            if (!disk_file.is_open()) {
                // by yeon.guo
                // TODO. dont throw execption
                // throw std::invalid_argument("Invalid file " + task->filename);
                rsp->ec = 1;
                return rsp;
            }

            disk_file.seekg(task->position);

            rsp->buffer = new uint8_t[task->length];
            disk_file.read(reinterpret_cast<char*>(rsp->buffer), task->length);
        }
        return rsp;
    }
};

