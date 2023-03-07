#include "hazard_ptr.h"

#include <yt/core/misc/lock_free_stack.h>
#include <yt/core/misc/small_vector.h>
#include <yt/core/misc/intrusive_linked_list.h>
#include <yt/core/misc/ring_queue.h>

#include <yt/core/concurrency/rw_spinlock.h>
#include <yt/core/concurrency/fiber_api.h>

#include <pthread.h>

namespace NYT {

/////////////////////////////////////////////////////////////////////////////

const NLogging::TLogger LockFreePtrLogger("LockFreeHelpers");

static const auto& Logger = LockFreePtrLogger;

////////////////////////////////////////////////////////////////////////////

using NConcurrency::TWriterGuard;
using NConcurrency::TReaderGuard;
using NConcurrency::TReaderWriterSpinLock;

thread_local std::atomic<void*> HazardPointer = {nullptr};

class THazardPointerManager
{
public:
    THazardPointerManager();
    ~THazardPointerManager();

    struct TRetiredPtr
    {
        void* Ptr;
        TDeleter Deleter;
    };

    struct TThreadState
    {
        TIntrusiveLinkedListNode<TThreadState> RegistryNode;

        std::atomic<void*>* const HazardPointer;
        TRingQueue<TRetiredPtr> DeleteList;
        SmallVector<void*, 64> ProtectedPointers;
        bool Scanning = false;

        explicit TThreadState(std::atomic<void*>* hazardPointer)
            : HazardPointer(hazardPointer)
        { }
    };

    struct TThreadStateToRegistryNode
    {
        auto operator() (TThreadState* state) const
        {
            return &state->RegistryNode;
        }
    };

    bool Scan(TThreadState* threadState);
    void DestroyThread(void* ptr);

    static inline TThreadState* GetThreadState();
    size_t GetThreadCount() const
    {
        return ThreadCount_.load(std::memory_order_relaxed);
    }

private:
    TThreadState* AllocateThread();

    std::atomic<size_t> ThreadCount_ = {0};
    static thread_local TThreadState* ThreadState_;

    TLockFreeStack<TRetiredPtr> DeleteQueue_;
    TReaderWriterSpinLock ThreadRegistryLock_;
    TIntrusiveLinkedList<TThreadState, TThreadStateToRegistryNode> ThreadRegistry_;
    pthread_key_t ThreadDtorKey_;
};

thread_local THazardPointerManager::TThreadState* THazardPointerManager::ThreadState_;

THazardPointerManager HazardPointerManager;

/////////////////////////////////////////////////////////////////////////////

THazardPointerManager::THazardPointerManager()
{
    pthread_key_create(&ThreadDtorKey_, [] (void* ptr) {
        HazardPointerManager.DestroyThread(ptr);
    });
}

THazardPointerManager::~THazardPointerManager()
{
    {
        NConcurrency::TWriterGuard guard(ThreadRegistryLock_);
        YT_VERIFY(ThreadRegistry_.GetSize() <= 1);

        if (ThreadRegistry_.GetSize() > 0) {
            auto* threadState = ThreadRegistry_.GetFront();

            YT_VERIFY(threadState->HazardPointer->load() == nullptr);

            while (!threadState->DeleteList.empty()) {
                DeleteQueue_.Enqueue(std::move(threadState->DeleteList.front()));
                threadState->DeleteList.pop();
            }
        }
    }

    DeleteQueue_.DequeueAll([] (TRetiredPtr& item) {
        item.Deleter(item.Ptr);
    });
}

THazardPointerManager::TThreadState* THazardPointerManager::GetThreadState()
{
    if (Y_UNLIKELY(!ThreadState_)) {
        ThreadState_ = HazardPointerManager.AllocateThread();
    }

    return ThreadState_;
}

THazardPointerManager::TThreadState* THazardPointerManager::AllocateThread()
{
    auto* threadState = new TThreadState(&HazardPointer);

    // Need to pass some non-null value for DestroyThread to be called.
    pthread_setspecific(ThreadDtorKey_, threadState);

    {
        NConcurrency::TWriterGuard guard(ThreadRegistryLock_);
        ThreadRegistry_.PushBack(threadState);
    }
    ++ThreadCount_;

    return threadState;
}

bool THazardPointerManager::Scan(TThreadState* threadState)
{
    threadState->Scanning = true;

    // Collect protected pointers.
    auto& protectedPointers = threadState->ProtectedPointers;
    YT_VERIFY(protectedPointers.empty());

    {
        TReaderGuard guard(ThreadRegistryLock_);
        for (
            auto* current = ThreadRegistry_.GetFront();
            current;
            current = current->RegistryNode.Next)
        {
            if (auto* hazardPtr = current->HazardPointer->load()) {
                protectedPointers.push_back(hazardPtr);
            }
        }
    }

    std::sort(protectedPointers.begin(), protectedPointers.end());

    auto& deleteList = threadState->DeleteList;

    // Append global DeleteQueue_ to local deleteList.
    DeleteQueue_.DequeueAll([&] (auto& item) {
        deleteList.push(std::move(item));
    });

    if (!protectedPointers.empty()) {
        YT_LOG_TRACE("Scanning hazard pointers (Candidates: %v, Protected: %v)",
            MakeFormattableView(TRingQueueIterableWrapper(deleteList), [&] (auto* builder, const auto& item) {
                builder->AppendFormat("%v", item.Ptr);
            }),
            MakeFormattableView(protectedPointers, [&] (auto* builder, const auto ptr) {
                builder->AppendFormat("%v", ptr);
            }));
    }

    size_t pushedCount = 0;
    auto popCount = deleteList.size();
    while (popCount-- > 0) {
        auto item = std::move(deleteList.front());
        deleteList.pop();

        void* ptr = reinterpret_cast<void*>(reinterpret_cast<uintptr_t>(item.Ptr) & PtrMask);

        if (std::binary_search(protectedPointers.begin(), protectedPointers.end(), ptr)) {
            deleteList.push(item);
            ++pushedCount;
        } else {
            item.Deleter(item.Ptr);
        }
    }

    protectedPointers.clear();

    threadState->Scanning = false;

    YT_VERIFY(pushedCount <= deleteList.size());
    return pushedCount < deleteList.size();
}

void THazardPointerManager::DestroyThread(void* ptr)
{
    auto* threadState = static_cast<TThreadState*>(ptr);

    {
        TWriterGuard guard(ThreadRegistryLock_);
        ThreadRegistry_.Remove(threadState);
        --ThreadCount_;
    }

    // Scan threadState->DeleteList and move to blocked elements to global DeleteQueue_.

    Scan(threadState);

    while (!threadState->DeleteList.empty()) {
        DeleteQueue_.Enqueue(std::move(threadState->DeleteList.front()));
        threadState->DeleteList.pop();
    }

    delete threadState;
}

//////////////////////////////////////////////////////////////////////////

void ScheduleObjectDeletion(void* ptr, TDeleter deleter)
{
    auto* threadState = HazardPointerManager.GetThreadState();

    threadState->DeleteList.push({ptr, deleter});

    if (threadState->Scanning) {
        return;
    }

    auto threadCount = HazardPointerManager.GetThreadCount();

    while (threadState->DeleteList.size() >= 2 * threadCount) {
        HazardPointerManager.Scan(threadState);
    }
}

bool ScanDeleteList()
{
    auto* threadState = HazardPointerManager.GetThreadState();

    YT_VERIFY(!threadState->Scanning);

    bool hasNewPointers = HazardPointerManager.Scan(threadState);

    return hasNewPointers || threadState->DeleteList.size() > HazardPointerManager.GetThreadCount();
}

void FlushDeleteList()
{
    while (ScanDeleteList());
}

THazardPtrFlushGuard::THazardPtrFlushGuard()
{
    NConcurrency::PushContextHandler(FlushDeleteList, nullptr);
}

THazardPtrFlushGuard::~THazardPtrFlushGuard()
{
    NConcurrency::PopContextHandler();
    FlushDeleteList();
}

/////////////////////////////////////////////////////////////////////////////

} // namespace NYT
