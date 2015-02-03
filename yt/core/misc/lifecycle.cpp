#include "lifecycle.h"

#include <mutex>
#include <queue>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

static std::once_flag GlobalCallbacks;

static std::atomic<TLifecycle*> GlobalLifecycle = ATOMIC_VAR_INIT(nullptr);

class TLifecycle::TImpl
{
public:
    TImpl(TLifecycle* this_, bool allowShadowing)
        : MinimalPriority_(0)
        , Shadowed_(nullptr)
        , This_(this_)
    {
        Push(allowShadowing);
    }

    ~TImpl()
    {
        Pop();
    }

    void RegisterAtExit(
        std::function<void()> callback,
        size_t priority = 0)
    {
        std::lock_guard<std::mutex> guard(Mutex_);
        YCHECK(priority >= MinimalPriority_);
        AtExitQueue_.emplace(TItem{
            std::move(callback),
            priority,
            AtExitQueue_.size()
        });
    }

    void RegisterAtFork(
        std::function<void()> prepareCallback,
        std::function<void()> parentCallback,
        std::function<void()> childCallback,
        size_t priority = 0)
    {
        std::lock_guard<std::mutex> guard(Mutex_);
        YCHECK(priority >= MinimalPriority_);
        if (prepareCallback) {
            AtForkPrepareQueue_.emplace(TItem{
                std::move(prepareCallback),
                priority,
                AtForkPrepareQueue_.size()
            });
        }
        if (parentCallback) {
            AtForkParentQueue_.emplace(TItem{
                std::move(parentCallback),
                priority,
                AtForkParentQueue_.size(),
            });
        }
        if (childCallback) {
            AtForkChildQueue_.emplace(TItem{
                std::move(childCallback),
                priority,
                AtForkChildQueue_.size()
            });
        }
    }

    void FireAtExit()
    {
        // Flush AtExit queue.
        while (true) {
            decltype(AtExitQueue_) queue;
            {
                std::lock_guard<std::mutex> guard(Mutex_);
                std::swap(AtExitQueue_, queue);
            }
            if (queue.empty()) {
                break;
            }
            while (!queue.empty()) {
                {
                    std::lock_guard<std::mutex> guard(Mutex_);
                    MinimalPriority_ = std::max(MinimalPriority_, queue.top().Priority);
                }
                queue.top().Callback();
                queue.pop();
            }
        }

        {
            std::lock_guard<std::mutex> guard(Mutex_);
            // Just clear AtFork queues.
            YCHECK(AtExitQueue_.empty());
            ClearQueue(AtForkPrepareQueue_);
            ClearQueue(AtForkParentQueue_);
            ClearQueue(AtForkChildQueue_);
            // Effectively reset this instance.
            MinimalPriority_ = 0;
        }
    }

    void FireAtForkPrepare()
    {
        Mutex_.lock();
        try {
            RunQueue(AtForkPrepareQueue_);
        } catch (...) {
            Mutex_.unlock();
            throw;
        }
    }

    void FireAtForkParent()
    {
        try {
            RunQueue(AtForkParentQueue_);
        } catch (...) {
            Mutex_.unlock();
            throw;
        }
        Mutex_.unlock();
    }

    void FireAtForkChild()
    {
        try {
            RunQueue(AtForkChildQueue_);
        } catch (...) {
            Mutex_.unlock();
            throw;
        }
        Mutex_.unlock();
    }

private:
    struct TItem
    {
        std::function<void()> Callback;
        size_t Priority;
        size_t SeqNo;
    };

    struct TDecreasingPriority
    {
        bool operator()(const TItem& lhs, const TItem& rhs) const
        {
            return lhs.Priority < rhs.Priority ||
                (lhs.Priority == rhs.Priority && lhs.SeqNo < rhs.SeqNo);
        }
    };

    struct TIncreasingPriority
    {
        bool operator()(const TItem& lhs, const TItem& rhs) const
        {
            return lhs.Priority > rhs.Priority ||
                (lhs.Priority == rhs.Priority && lhs.SeqNo > rhs.SeqNo);
        }
    };

    std::mutex Mutex_;
    size_t MinimalPriority_;
    std::priority_queue<TItem, std::vector<TItem>, TIncreasingPriority> AtExitQueue_;
    std::priority_queue<TItem, std::vector<TItem>, TDecreasingPriority> AtForkPrepareQueue_;
    std::priority_queue<TItem, std::vector<TItem>, TIncreasingPriority> AtForkParentQueue_;
    std::priority_queue<TItem, std::vector<TItem>, TIncreasingPriority> AtForkChildQueue_;

    TLifecycle* Shadowed_;
    TLifecycle* This_;

    template <class TQueue>
    void ClearQueue(TQueue& queue)
    {
        while (!queue.empty()) {
            queue.pop();
        }
    }

    template <class TQueue>
    void RunQueue(TQueue& queue)
    {
        std::vector<TItem> items;
        items.reserve(queue.size());
        while (!queue.empty()) {
            items.emplace_back(queue.top());
            queue.pop();
        }
        for (auto&& item : items) {
            item.Callback();
            queue.emplace(item);
        }
    }

    void Push(bool allowShadow)
    {
        do {
            Shadowed_ = GlobalLifecycle.load(std::memory_order_acquire);
            YCHECK(allowShadow || Shadowed_ == nullptr);
        } while (!GlobalLifecycle.compare_exchange_weak(
            Shadowed_,
            This_,
            std::memory_order_release));

        std::call_once(GlobalCallbacks, [] () {
            YCHECK(atexit(&TLifecycle::GlobalAtExitCallback) == 0);
#ifndef _win_
            YCHECK(pthread_atfork(
                &TLifecycle::GlobalAtForkPrepareCallback,
                &TLifecycle::GlobalAtForkParentCallback,
                &TLifecycle::GlobalAtForkChildCallback) == 0);
#endif
        });
    }

    void Pop()
    {
        YCHECK(GlobalLifecycle.compare_exchange_strong(This_, Shadowed_));
    }
};

////////////////////////////////////////////////////////////////////////////////

TLifecycle::TLifecycle()
    : Impl_(std::make_unique<TImpl>(this, false))
{ }

TLifecycle::TLifecycle(bool allowShadow)
    : Impl_(std::make_unique<TImpl>(this, allowShadow))
{ }

TLifecycle::~TLifecycle()
{
    Impl_->FireAtExit();
}

void TLifecycle::RegisterAtExit(std::function<void()> callback, size_t priority)
{
    auto manager = GlobalLifecycle.load(std::memory_order_acquire);
    YCHECK(manager);
    manager->Impl_->RegisterAtExit(std::move(callback), priority);
}

void TLifecycle::RegisterAtFork(
    std::function<void()> prepareCallback,
    std::function<void()> parentCallback,
    std::function<void()> childCallback,
    size_t priority)
{
    auto manager = GlobalLifecycle.load(std::memory_order_acquire);
    YCHECK(manager);
    manager->Impl_->RegisterAtFork(
        std::move(prepareCallback),
        std::move(parentCallback),
        std::move(childCallback),
        priority);
}

void TLifecycle::FireAtExit()
{
    Impl_->FireAtExit();
}

void TLifecycle::FireAtForkPrepare()
{
    Impl_->FireAtForkPrepare();
}

void TLifecycle::FireAtForkParent()
{
    Impl_->FireAtForkParent();
}

void TLifecycle::FireAtForkChild()
{
    Impl_->FireAtForkChild();
}

void TLifecycle::GlobalAtExitCallback()
{
    auto manager = GlobalLifecycle.load(std::memory_order_acquire);
    if (manager) {
        manager->FireAtExit();
    }
}

void TLifecycle::GlobalAtForkPrepareCallback()
{
    auto manager = GlobalLifecycle.load(std::memory_order_acquire);
    if (manager) {
        manager->FireAtForkPrepare();
    }
}

void TLifecycle::GlobalAtForkParentCallback()
{
    auto manager = GlobalLifecycle.load(std::memory_order_acquire);
    if (manager) {
        manager->FireAtForkParent();
    }
}

void TLifecycle::GlobalAtForkChildCallback()
{
    auto manager = GlobalLifecycle.load(std::memory_order_acquire);
    if (manager) {
        manager->FireAtForkChild();
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
