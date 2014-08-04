#include "at_exit_manager.h"

#include <mutex>
#include <queue>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

static std::once_flag GlobalCallbacks;

static std::atomic<TAtExitManager*> GlobalManager = ATOMIC_VAR_INIT(nullptr);

class TAtExitManager::TImpl
{
public:
    TImpl(TAtExitManager* this_, bool allowShadowing)
        : Shadowed_(nullptr)
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
        size_t priority = std::numeric_limits<size_t>::max())
    {
        std::lock_guard<std::mutex> guard(Mutex_);
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
        size_t priority = std::numeric_limits<size_t>::max())
    {
        std::lock_guard<std::mutex> guard(Mutex_);
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

    void AtExitHook()
    {
        std::lock_guard<std::mutex> guard(Mutex_);
        // Flush AtExit queue.
        while (!AtExitQueue_.empty()) {
            AtExitQueue_.top().Callback();
            AtExitQueue_.pop();
        }
        // Just clear AtFork queues.
        ClearQueue(AtForkPrepareQueue_);
        ClearQueue(AtForkParentQueue_);
        ClearQueue(AtForkChildQueue_);
    }

    void AtForkPrepareHook()
    {
        Mutex_.lock();
        try {
            RunQueue(AtForkPrepareQueue_);
        } catch (...) {
            Mutex_.unlock();
            throw;
        }
    }

    void AtForkParentHook()
    {
        try {
            RunQueue(AtForkParentQueue_);
        } catch (...) {
            Mutex_.unlock();
            throw;
        }
        Mutex_.unlock();
    }

    void AtForkChildHook()
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
    std::priority_queue<TItem, std::vector<TItem>, TIncreasingPriority> AtExitQueue_;
    std::priority_queue<TItem, std::vector<TItem>, TDecreasingPriority> AtForkPrepareQueue_;
    std::priority_queue<TItem, std::vector<TItem>, TIncreasingPriority> AtForkParentQueue_;
    std::priority_queue<TItem, std::vector<TItem>, TIncreasingPriority> AtForkChildQueue_;

    TAtExitManager* Shadowed_;
    TAtExitManager* This_;

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

    void Push(bool allowShadowing)
    {
        do {
            Shadowed_ = GlobalManager.load(std::memory_order_acquire);
            YCHECK(allowShadowing || Shadowed_ == nullptr);
        } while (!GlobalManager.compare_exchange_weak(
            Shadowed_,
            This_,
            std::memory_order_release));

        std::call_once(GlobalCallbacks, [] () {
            YCHECK(atexit(&TAtExitManager::GlobalAtExitCallback) == 0);
            YCHECK(pthread_atfork(
                &TAtExitManager::GlobalAtForkPrepareCallback,
                &TAtExitManager::GlobalAtForkParentCallback,
                &TAtExitManager::GlobalAtForkChildCallback) == 0);
        });
    }

    void Pop()
    {
        YCHECK(GlobalManager.compare_exchange_strong(This_, Shadowed_));
    }
};

////////////////////////////////////////////////////////////////////////////////

TAtExitManager::TAtExitManager()
    : Impl_(std::make_unique<TImpl>(this, false))
{ }

TAtExitManager::TAtExitManager(bool allowShadowing)
    : Impl_(std::make_unique<TImpl>(this, allowShadowing))
{ }

TAtExitManager::~TAtExitManager()
{ }

void TAtExitManager::RegisterAtExit(std::function<void()> callback, size_t priority)
{
    auto manager = GlobalManager.load(std::memory_order_acquire);
    YCHECK(manager);
    manager->Impl_->RegisterAtExit(std::move(callback), priority);
}

void TAtExitManager::RegisterAtFork(
    std::function<void()> prepareCallback,
    std::function<void()> parentCallback,
    std::function<void()> childCallback,
    size_t priority)
{
    auto manager = GlobalManager.load(std::memory_order_acquire);
    YCHECK(manager);
    manager->Impl_->RegisterAtFork(
        std::move(prepareCallback),
        std::move(parentCallback),
        std::move(childCallback),
        priority);
}

void TAtExitManager::FireAtExit()
{
    Impl_->AtExitHook();
}

void TAtExitManager::FireAtForkPrepare()
{
    Impl_->AtForkPrepareHook();
}

void TAtExitManager::FireAtForkParent()
{
    Impl_->AtForkParentHook();
}

void TAtExitManager::FireAtForkChild()
{
    Impl_->AtForkChildHook();
}

void TAtExitManager::GlobalAtExitCallback()
{
    auto manager = GlobalManager.load(std::memory_order_acquire);
    if (manager) {
        manager->FireAtExit();
    }
}

void TAtExitManager::GlobalAtForkPrepareCallback()
{
    auto manager = GlobalManager.load(std::memory_order_acquire);
    if (manager) {
        manager->FireAtForkPrepare();
    }
}

void TAtExitManager::GlobalAtForkParentCallback()
{
    auto manager = GlobalManager.load(std::memory_order_acquire);
    if (manager) {
        manager->FireAtForkParent();
    }
}

void TAtExitManager::GlobalAtForkChildCallback()
{
    auto manager = GlobalManager.load(std::memory_order_acquire);
    if (manager) {
        manager->FireAtForkChild();
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
