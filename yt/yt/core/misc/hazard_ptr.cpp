#include "hazard_ptr.h"

#include <yt/yt/core/misc/free_list.h>
#include <yt/yt/core/misc/singleton.h>
#include <yt/yt/core/misc/proc.h>
#include <yt/yt/core/misc/ring_queue.h>
#include <yt/yt/core/misc/shutdown.h>
#include <yt/yt/core/misc/finally.h>

#include <yt/yt/core/concurrency/scheduler_api.h>

#include <library/cpp/yt/threading/at_fork.h>

#include <library/cpp/yt/containers/intrusive_linked_list.h>

#include <library/cpp/yt/small_containers/compact_vector.h>

#include <library/cpp/yt/threading/rw_spin_lock.h>

namespace NYT {

using namespace NConcurrency;

/////////////////////////////////////////////////////////////////////////////

inline const NLogging::TLogger LockFreePtrLogger("LockFree");
static const auto& Logger = LockFreePtrLogger;

////////////////////////////////////////////////////////////////////////////

namespace NDetail {

////////////////////////////////////////////////////////////////////////////

thread_local THazardPointerSet HazardPointers;

//! A simple container based on free list which supports only Enqueue and DequeueAll.
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
            auto* next = node->Next.load(std::memory_order::acquire);
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
    THazardPtrDeleter Deleter;
};

struct THazardThreadState
{
    THazardPointerSet* const HazardPointers;

    TIntrusiveLinkedListNode<THazardThreadState> RegistryNode;
    TRingQueue<TRetiredPtr> DeleteList;
    TCompactVector<void*, 64> ProtectedPointers;
    bool Scanning = false;

    explicit THazardThreadState(THazardPointerSet* hazardPointers)
        : HazardPointers(hazardPointers)
    { }
};

thread_local THazardThreadState* HazardThreadState;
thread_local bool HazardThreadStateDestroyed;

////////////////////////////////////////////////////////////////////////////////

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

    void InitThreadState();

    void ScheduleObjectDeletion(void* ptr, THazardPtrDeleter deleter);

    bool ScanDeleteList();
    void FlushDeleteList();

private:
    std::atomic<int> ThreadCount_ = 0;

    TDeleteQueue<TRetiredPtr> DeleteQueue_;

    YT_DECLARE_SPIN_LOCK(NThreading::TReaderWriterSpinLock, ThreadRegistryLock_);
    TIntrusiveLinkedList<THazardThreadState, THazardThreadStateToRegistryNode> ThreadRegistry_;

    THazardPointerManager();

    void Shutdown();

    bool Scan(THazardThreadState* threadState);

    THazardThreadState* AllocateThreadState();
    void DestroyThreadState(THazardThreadState* ptr);

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
    NThreading::RegisterAtForkHandlers(
        [this] { BeforeFork(); },
        [this] { AfterForkParent(); },
        [this] { AfterForkChild(); });
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

void THazardPointerManager::ScheduleObjectDeletion(void* ptr, THazardPtrDeleter deleter)
{
    auto* threadState = HazardThreadState;
    if (Y_UNLIKELY(!threadState)) {
        if (HazardThreadStateDestroyed) {
            // Looks like a global shutdown.
            deleter(ptr);
            return;
        }
        InitThreadState();
        threadState = HazardThreadState;
    }

    threadState->DeleteList.push({ptr, deleter});

    if (threadState->Scanning) {
        return;
    }

    int threadCount = ThreadCount_.load(std::memory_order_relaxed);
    while (std::ssize(threadState->DeleteList) >= std::max(2 * threadCount, 1)) {
        Scan(threadState);
    }
}

bool THazardPointerManager::ScanDeleteList()
{
    auto* threadState = HazardThreadState;
    if (!threadState || threadState->DeleteList.empty()) {
        return false;
    }

    YT_VERIFY(!threadState->Scanning);

    bool hasNewPointers = Scan(threadState);
    int threadCount = ThreadCount_.load(std::memory_order_relaxed);
    return
        hasNewPointers ||
        std::ssize(threadState->DeleteList) > threadCount;
}

void THazardPointerManager::FlushDeleteList()
{
    while (ScanDeleteList());
}

void THazardPointerManager::InitThreadState()
{
    if (!HazardThreadState) {
        YT_VERIFY(!HazardThreadStateDestroyed);
        HazardThreadState = AllocateThreadState();
    }
}

THazardThreadState* THazardPointerManager::AllocateThreadState()
{
    auto* threadState = new THazardThreadState(&HazardPointers);

    struct THazardThreadStateDestroyer
    {
        THazardThreadState* ThreadState;

        ~THazardThreadStateDestroyer()
        {
            THazardPointerManager::Get()->DestroyThreadState(ThreadState);
        }
    };

    // Unregisters thread from hazard ptr manager on thread exit.
    static thread_local THazardThreadStateDestroyer destroyer{threadState};

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
            for (const auto& hazardPointer : *current->HazardPointers) {
                if (auto* ptr = hazardPointer.load()) {
                    protectedPointers.push_back(ptr);
                }
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

void THazardPointerManager::DestroyThreadState(THazardThreadState* threadState)
{
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

    HazardThreadState = nullptr;
    HazardThreadStateDestroyed = true;
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

void InitHazardThreadState()
{
    THazardPointerManager::Get()->InitThreadState();
}

/////////////////////////////////////////////////////////////////////////////

} // namespace NDetail

void ScheduleObjectDeletion(void* ptr, THazardPtrDeleter deleter)
{
    NYT::NDetail::THazardPointerManager::Get()->ScheduleObjectDeletion(ptr, deleter);
}

bool ScanDeleteList()
{
    return NYT::NDetail::THazardPointerManager::Get()->ScanDeleteList();
}

void FlushDeleteList()
{
    NYT::NDetail::THazardPointerManager::Get()->FlushDeleteList();
}

/////////////////////////////////////////////////////////////////////////////

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
