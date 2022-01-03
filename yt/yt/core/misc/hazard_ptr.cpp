#include "hazard_ptr.h"

#include <yt/yt/core/misc/free_list.h>
#include <yt/yt/core/misc/singleton.h>
#include <yt/yt/core/misc/compact_vector.h>
#include <yt/yt/core/misc/proc.h>
#include <yt/yt/core/misc/ring_queue.h>
#include <yt/yt/core/misc/shutdown.h>
#include <yt/yt/core/misc/finally.h>

#include <yt/yt/core/concurrency/scheduler_api.h>

#include <library/cpp/yt/threading/fork_aware_spin_lock.h>

#include <library/cpp/yt/containers/intrusive_linked_list.h>

#include <library/cpp/yt/threading/rw_spin_lock.h>

#include <pthread.h>

namespace NYT {

using namespace NConcurrency;

/////////////////////////////////////////////////////////////////////////////

inline const NLogging::TLogger LockFreePtrLogger("LockFreeHelpers");
static const auto& Logger = LockFreePtrLogger;

////////////////////////////////////////////////////////////////////////////

thread_local std::atomic<void*> HazardPointer;

////////////////////////////////////////////////////////////////////////////////

// Simple container based on free list which support only Enqueue and DequeueAll.

template <class T>
class TDeleteQueue
{
private:
    struct TNode
        : public TFreeListItemBase<TNode>
    {
        T Value;

        TNode() = default;

        explicit TNode(T&& value)
            : Value(std::move(value))
        { }
    };

    TFreeList<TNode> Impl_;

    void EraseList(TNode* node)
    {
        while (node) {
            auto* next = node->Next.load(std::memory_order_acquire);
            delete node;
            node = next;
        }
    }

public:
    ~TDeleteQueue()
    {
        EraseList(Impl_.ExtractAll());
    }

    template <typename TCallback>
    void DequeueAll(TCallback callback)
    {
        auto* head = Impl_.ExtractAll();

        auto cleanup = Finally([this, head] {
            EraseList(head);
        });

        auto* ptr = head;
        while (ptr) {
            callback(ptr->Value);
            ptr = ptr->Next;
        }
    }

    void Enqueue(T&& value)
    {
        Impl_.Put(new TNode(std::move(value)));
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TRetiredPtr
{
    void* Ptr;
    TDeleter Deleter;
};

struct THazardThreadState
{
    TIntrusiveLinkedListNode<THazardThreadState> RegistryNode;

    std::atomic<void*>* const HazardPointer;
    TRingQueue<TRetiredPtr> DeleteList;
    TCompactVector<void*, 64> ProtectedPointers;
    bool Scanning = false;

    explicit THazardThreadState(std::atomic<void*>* hazardPointer)
        : HazardPointer(hazardPointer)
    { }
};

thread_local THazardThreadState* HazardThreadState;

class THazardPointerManager
{
public:
    struct THazardThreadStateToRegistryNode
    {
        auto operator() (THazardThreadState* state) const
        {
            return &state->RegistryNode;
        }
    };

    static THazardPointerManager* Get()
    {
        return LeakySingleton<THazardPointerManager>();
    }

    bool Scan(THazardThreadState* threadState);
    void DestroyThread(void* ptr);

    static inline THazardThreadState* GetThreadState();

    size_t GetThreadCount() const
    {
        return ThreadCount_.load(std::memory_order_relaxed);
    }

private:
    std::atomic<int> ThreadCount_ = 0;

    TDeleteQueue<TRetiredPtr> DeleteQueue_;

    YT_DECLARE_SPIN_LOCK(NThreading::TReaderWriterSpinLock, ThreadRegistryLock_);
    TIntrusiveLinkedList<THazardThreadState, THazardThreadStateToRegistryNode> ThreadRegistry_;
    pthread_key_t ThreadDtorKey_;

    THazardPointerManager();

    static void ShutdownThunk();
    void Shutdown();

    THazardThreadState* AllocateThread();

    void BeforeFork();
    void AfterForkParent();
    void AfterForkChild();

    DECLARE_LEAKY_SINGLETON_FRIEND()
};

/////////////////////////////////////////////////////////////////////////////

static void* HazardPointerManagerInitializer = [] {
    THazardPointerManager::Get();
    return nullptr;
}();

/////////////////////////////////////////////////////////////////////////////

THazardPointerManager::THazardPointerManager()
{
    pthread_key_create(&ThreadDtorKey_, [] (void* ptr) {
        THazardPointerManager::Get()->DestroyThread(ptr);
    });

    NThreading::TForkAwareSpinLock::AtFork(
        this,
        [] (void* cookie) {
            static_cast<THazardPointerManager*>(cookie)->BeforeFork();
        },
        [] (void* cookie) {
            static_cast<THazardPointerManager*>(cookie)->AfterForkParent();
        },
        [] (void* cookie) {
            static_cast<THazardPointerManager*>(cookie)->AfterForkChild();
        });
}

void THazardPointerManager::ShutdownThunk()
{
    THazardPointerManager::Get()->Shutdown();
}

void THazardPointerManager::Shutdown()
{
    if (auto* logFile = GetShutdownLogFile()) {
        ::fprintf(logFile, "*** Hazard Pointer Manager shutdown started (ThreadCount: %d)\n",
            ThreadCount_.load());
    }

    int count = 0;
    DeleteQueue_.DequeueAll([&] (TRetiredPtr& item) {
        item.Deleter(item.Ptr);
        ++count;
    });

    if (auto* logFile = GetShutdownLogFile()) {
        ::fprintf(logFile, "*** Hazard Pointer Manager shutdown completed (DeletedPtrCount: %d)\n",
            count);
    }
}

THazardThreadState* THazardPointerManager::GetThreadState()
{
    if (Y_UNLIKELY(!HazardThreadState)) {
        HazardThreadState = THazardPointerManager::Get()->AllocateThread();
    }

    return HazardThreadState;
}

THazardThreadState* THazardPointerManager::AllocateThread()
{
    auto* threadState = new THazardThreadState(&HazardPointer);

    // Need to pass some non-null value for DestroyThread to be called.
    pthread_setspecific(ThreadDtorKey_, threadState);

    {
        auto guard = WriterGuard(ThreadRegistryLock_);
        ThreadRegistry_.PushBack(threadState);
        ++ThreadCount_;
    }

    if (auto* logFile = GetShutdownLogFile()) {
        ::fprintf(logFile, "*** Hazard Pointer Manager thread state allocated (ThreadId: %" PRISZT ")\n",
            GetCurrentThreadId());
    }

    return threadState;
}

bool THazardPointerManager::Scan(THazardThreadState* threadState)
{
    threadState->Scanning = true;

    // Collect protected pointers.
    auto& protectedPointers = threadState->ProtectedPointers;
    YT_VERIFY(protectedPointers.empty());

    {
        auto guard = ForkFriendlyReaderGuard(ThreadRegistryLock_);
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
    auto* threadState = static_cast<THazardThreadState*>(ptr);

    {
        auto guard = WriterGuard(ThreadRegistryLock_);
        ThreadRegistry_.Remove(threadState);
        --ThreadCount_;
    }

    // Scan threadState->DeleteList and move to blocked elements to global DeleteQueue_.

    Scan(threadState);

    int count = 0;
    while (!threadState->DeleteList.empty()) {
        DeleteQueue_.Enqueue(std::move(threadState->DeleteList.front()));
        threadState->DeleteList.pop();
        ++count;
    }

    if (auto* logFile = GetShutdownLogFile()) {
        ::fprintf(logFile, "*** Hazard Pointer Manager thread state destroyed (ThreadId: %" PRISZT ", DeletedPtrCount: %d)\n",
            GetCurrentThreadId(),
            count);
    }

    delete threadState;
}

void THazardPointerManager::BeforeFork()
{
    ThreadRegistryLock_.AcquireWriter();
}

void THazardPointerManager::AfterForkParent()
{
    ThreadRegistryLock_.ReleaseWriter();
}

void THazardPointerManager::AfterForkChild()
{
    ThreadRegistry_.Clear();
    ThreadCount_ = 0;

    if (HazardThreadState) {
        ThreadRegistry_.PushBack(HazardThreadState);
        ThreadCount_ = 1;
    }

    ThreadRegistryLock_.ReleaseWriter();
}

//////////////////////////////////////////////////////////////////////////

void ScheduleObjectDeletion(void* ptr, TDeleter deleter)
{
    auto* threadState = THazardPointerManager::Get()->GetThreadState();

    threadState->DeleteList.push({ptr, deleter});

    if (threadState->Scanning) {
        return;
    }

    auto threadCount = THazardPointerManager::Get()->GetThreadCount();
    while (threadState->DeleteList.size() >= 2 * threadCount) {
        THazardPointerManager::Get()->Scan(threadState);
    }
}

bool ScanDeleteList()
{
    auto* threadState = HazardThreadState;
    if (!threadState || threadState->DeleteList.empty()) {
        return false;
    }

    YT_VERIFY(!threadState->Scanning);

    bool hasNewPointers = THazardPointerManager::Get()->Scan(threadState);
    return
        hasNewPointers ||
        threadState->DeleteList.size() > THazardPointerManager::Get()->GetThreadCount();
}

void InitThreadState()
{
    THazardPointerManager::Get()->GetThreadState();
}

void FlushDeleteList()
{
    while (ScanDeleteList());
}

THazardPtrFlushGuard::THazardPtrFlushGuard()
{
    PushContextHandler(FlushDeleteList, nullptr);
}

THazardPtrFlushGuard::~THazardPtrFlushGuard()
{
    PopContextHandler();
    FlushDeleteList();
}

/////////////////////////////////////////////////////////////////////////////

} // namespace NYT
