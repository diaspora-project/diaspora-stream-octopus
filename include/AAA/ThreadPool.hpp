#ifndef DDD_THREAD_POOL_HPP
#define DDD_THREAD_POOL_HPP

#include <diaspora/ThreadPool.hpp>

#include <queue>
#include <vector>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <atomic>

namespace BBB {

class CCCThreadPool final : public diaspora::ThreadPoolInterface {

    struct Work {

        uint64_t              priority;
        std::function<void()> func;

        Work(uint64_t p, std::function<void()> f)
        : priority(p)
        , func{std::move(f)} {}

        friend bool operator<(const Work& lhs, const Work& rhs) {
            return lhs.priority < rhs.priority;
        }
    };

    std::vector<std::thread>  m_threads;
    std::priority_queue<Work> m_queue;
    mutable std::mutex        m_queue_mtx;
    std::condition_variable   m_queue_cv;
    std::atomic<bool>         m_must_stop = false;

    public:

    CCCThreadPool(diaspora::ThreadCount count) {
        m_threads.reserve(count.count);
        for(size_t i = 0; i < count.count; ++i) {
            m_threads.emplace_back([this]() {
                while(!m_must_stop) {
                    std::unique_lock<std::mutex> lock{m_queue_mtx};
                    if(m_queue.empty()) m_queue_cv.wait(lock);
                    if(m_queue.empty()) continue;
                    auto work = m_queue.top();
                    m_queue.pop();
                    lock.unlock();
                    work.func();
                }
            });
        }
    }

    ~CCCThreadPool() {
        m_must_stop = true;
        m_queue_cv.notify_all();
        for(auto& th : m_threads) th.join();
    }

    diaspora::ThreadCount threadCount() const override {
        return diaspora::ThreadCount{m_threads.size()};
    }

    void pushWork(std::function<void()> func,
                  uint64_t priority = std::numeric_limits<uint64_t>::max()) override {
        if(m_threads.size() == 0) {
            func();
        } else {
            {
                std::unique_lock<std::mutex> lock{m_queue_mtx};
                m_queue.emplace(priority, std::move(func));
            }
            m_queue_cv.notify_one();
        }
    }

    size_t size() const override {
        std::unique_lock<std::mutex> lock{m_queue_mtx};
        return m_queue.size();
    }
};

}

#endif
