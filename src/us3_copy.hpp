/**
 * Author: rick.wu@ucloud.cn
 * Date: 2021.4.22
 * Descripthion: fs::copy is replaced. Based on the N-party queue depth of single queue and 2, 
 * 		the queue is designed with lockless ring. Because only one thread reading local
 * 		file and one writing to us3fs mount path are used, the blocked read-write thread
 * 		is waken up by semaphore. This design has optimized the effect on all 
 * 		POSIX interface file systems.
 */
#include <cstdlib>
#include <filesystem>
#include <string.h>
#include <pthread.h>
#include <semaphore.h>
#include <fcntl.h>
#include <unistd.h>

namespace fs = std::filesystem;
const int BUFFER_SIZE = 32*1024*1024;
struct US3Buffer {
	char *buffer;
	int len;
};

struct US3RIOQuene {
	struct US3Buffer** Q;
	int size;
	volatile uint64_t readOffset;
	volatile uint64_t writeOffset;
	volatile bool eof;
	volatile bool blocking;
	volatile int ret; // 0: ok 
	sem_t sem;
	int fd;
};

struct US3RIOQuene* NewUS3RIOQuene(int rfd, int q_depth_pow) {
	if (q_depth_pow > 8) { q_depth_pow = 8;}
	if (q_depth_pow < 0) { q_depth_pow = 1;}
	int qsize = 1 << q_depth_pow; // 2^x
	US3RIOQuene* q = new US3RIOQuene;
	
	q->size = qsize;
	q->fd = rfd;
	q->Q = new US3Buffer*[qsize];
       	for ( int i = 0; i < qsize; i++) {
		US3Buffer* buff = new US3Buffer;
		buff->buffer = new char[BUFFER_SIZE];
		buff->len = 0;
		q->Q[i] = buff;
	}
	q->readOffset=0;
	q->writeOffset=0;
	q->eof = false;
	q->blocking = false;
	q->ret = 0;
	sem_init(&(q->sem),0,0);

	return q;
}

void FreeUS3RIOQuene(US3RIOQuene* q) {
       	for (int i = 0; i < q->size; i++) {
		delete q->Q[i];
	}
	delete q->Q;
	sem_destroy(&(q->sem));
	delete q;
}

void *RIOThreadFunc(void *ptr) {
	US3RIOQuene* q = (US3RIOQuene*)ptr;
	int mod = q->size-1;// 2^x-1
	while (true) {
		int count = 0;
		if (q->writeOffset >= q->readOffset) {
			count = (q->writeOffset - q->readOffset);
		} else {
			count = mod + q->writeOffset - q->readOffset;
		}
		if ( q->ret != 0) { break; } // for write error

		if (count >= mod) {
			q->blocking = true;
			sem_wait(&(q->sem));
			q->blocking = false;
			continue;
		}

		int wIdx = q->writeOffset&mod;
		volatile int len = 0;
		int try_times = 0;
		while (true) {
			int ret = read(q->fd, (q->Q[wIdx]->buffer)+len, BUFFER_SIZE-len);
			if (-1 == ret) {
				try_times++; 
				std::ostringstream ostr;
				ostr << "try_times: " << try_times <<" , but errno:" << errno << ", info:" << strerror(errno) <<  std::endl;
				if (try_times <= 3) { 
					usleep(try_times*500*1000); // sleep try_times * 500ms
					std::cerr << "us3 read err: " <<  ostr.str() << std::endl; 
				}
				else { q->ret = ret; break; }
			} else if (0 == ret) { // eof
				// last part
				q->eof = true;
				break;
			} else { len += ret; }
			
			// read full
			if (len >= BUFFER_SIZE) { break; }
		} // end of inside while

		if (try_times > 0) {
			if ( q->ret != 0) {  // for read error
				std::ostringstream ostr;
				ostr << " errno:" << errno << ", info:" << strerror(errno) <<  std::endl;
				std::cerr << "us3 write " <<  ostr.str() << std::endl; 
				break; 
			} else {
				std::ostringstream ostr;
				std::cout << "us3 read try_times: " << try_times <<" , succ!!" <<  std::endl;
			}
		}

		q->Q[wIdx]->len = len;
		q->writeOffset+=1;

		if (q->blocking) {
			q->blocking = false;
			// wakeup wio thread
			sem_post(&(q->sem));
		}
		
		if (q->ret != 0)  { break; } // for write error
		if (q->eof) { break; }
		
	} // end of external while
	// notify main thread over;
	return NULL;
}

US3Buffer* GetUS3Buffer(US3RIOQuene* q) {
	//int qsize = sizeof(q->Q)/sizeof(q->Q[0]); // 2^x
	int mod = q->size-1;
	while (true) {
		int count = 0;
		if (q->writeOffset >= q->readOffset) {
			count = (q->writeOffset - q->readOffset);
		} else {
			count = mod + q->writeOffset - q->readOffset;
		}

		if (count < 1) {
			if (q->eof) { return NULL; }
			q->blocking = true;
			sem_wait(&(q->sem));
			q->blocking = false;
			continue;
		}
		
		int rIdx = (q->readOffset)&mod;
		return q->Q[rIdx];
	}
}

void PutUS3Buffer(US3RIOQuene* q, US3Buffer* us3Buff /*make fashion*/) {
	q->readOffset += 1;
	if (q->blocking) {
		// wakeup rio thread
		sem_post(&(q->sem));
	}
}

int US3Copy(std::filesystem::path& from, std::filesystem::path& to) {
	int fromfd = open((char *)(from.string().c_str()), O_RDWR);
	if (-1 == fromfd) {
	    std::cerr << "open from file " << from <<  ", but errno:" << errno << ", info:" << strerror(errno) << std::endl;
	    return -1;
	}

	int tofd = open((char *)(to.string().c_str()), O_RDWR|O_CREAT, S_IRWXU|S_IRUSR|S_IXUSR|S_IROTH|S_IXOTH);
	if (-1 == tofd) {
	    close(fromfd);
	    std::cerr << "open to file " << to <<  ", but errno:" << errno << ", info:" << strerror(errno) << std::endl;
	    return -1;
	}
	
	US3RIOQuene* rioq = NewUS3RIOQuene(fromfd, 3);
	pthread_t t1;
	int ret = pthread_create(&t1, NULL, RIOThreadFunc, (void*) rioq);
	if (ret) {
	    std::cerr << "create thread , errno:" << errno << ", info:" << strerror(errno) << std::endl;
	    return ret;
	}

	while (true) {
		US3Buffer* us3Buff = GetUS3Buffer(rioq);
		if (us3Buff == NULL) { break; }
		volatile int len = 0;
		int try_times = 0;
		while (true) {
			int count = write(tofd, (us3Buff->buffer)+len, (us3Buff->len)-len);
			if (-1 == count) {
				std::ostringstream ostr;
				try_times++; 
				ostr << "try_times: " << try_times <<" , but errno:" << errno << ", info:" << strerror(errno) <<  std::endl;
				if (try_times <= 3) { 
					usleep(try_times*500*1000); // sleep try_times * 500ms
					std::cerr << "us3 write err: " <<  ostr.str() << std::endl; 
				} 
				else { rioq->ret = count; break; }
			} else if (0 == count) {
			} else { 
				len += count;
				break;
			}
		}

		if (try_times > 0) {
			if ( rioq->ret != 0) { // for write error
				std::ostringstream ostr;
				ostr << " errno:" << errno << ", info:" << strerror(errno) <<  std::endl;
				std::cerr << "us3 write " <<  ostr.str() << std::endl; 
				PutUS3Buffer(rioq, us3Buff); // for wakeup read
				break;
			} else {
				std::ostringstream ostr;
				std::cerr << "us3 write try_times: " << try_times <<" , succ!!" <<  std::endl;
			}
		} else {
			PutUS3Buffer(rioq, us3Buff);
		}

		if ( rioq->ret != 0) { break; } // for read error
	}
	
	pthread_join(t1,NULL);
	ret = rioq->ret;
	FreeUS3RIOQuene(rioq);
	rioq = NULL;
	close(fromfd);
	close(tofd);
	return ret;
}
