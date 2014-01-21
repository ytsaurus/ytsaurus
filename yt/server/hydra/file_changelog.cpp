#include "stdafx.h"
#include "changelog.h"
#include "file_changelog.h"
#include "sync_file_changelog.h"
#include "config.h"
#include "private.h"

#include <core/misc/fs.h>
#include <core/misc/cache.h>

#include <core/concurrency/thread_affinity.h>

#include <core/logging/tagged_logger.h>

#include <util/system/thread.h>

#include <util/folder/dirut.h>

namespace NYT {
namespace NHydra {

////////////////////////////////////////////////////////////////////////////////

static auto& Logger = HydraLogger;
static auto& Profiler = HydraProfiler;

static const TDuration FlushThreadQuantum = TDuration::MilliSeconds(10);

////////////////////////////////////////////////////////////////////////////////

class TChangelogQueue
    : public TRefCounted
{
public:
    explicit TChangelogQueue(TSyncFileChangelogPtr changelog)
        : Changelog(changelog)
        , UseCount(0)
        , FlushedRecordCount(changelog->GetRecordCount())
        , ByteSize(0)
        , FlushPromise(NewPromise())
        , FlushForced(false)
        , SealPromise(NewPromise())
        , SealForced(false)
        , SealRecordCount(-1)
    { }


    void Lock()
    {
        VERIFY_THREAD_AFFINITY_ANY();

        AtomicIncrement(UseCount);
    }

    void Unlock()
    {
        VERIFY_THREAD_AFFINITY_ANY();

        AtomicDecrement(UseCount);
    }


    TFuture<void> Append(TSharedRef data)
    {
        VERIFY_THREAD_AFFINITY_ANY();

        TGuard<TSpinLock> guard(SpinLock);
        YCHECK(!SealForced);
        AppendQueue.push_back(std::move(data));
        ByteSize += data.Size();

        YCHECK(FlushPromise);
        return FlushPromise;
    }


    TFuture<void> AsyncFlush()
    {
        VERIFY_THREAD_AFFINITY_ANY();

        TGuard<TSpinLock> guard(SpinLock);

        if (FlushQueue.empty() && AppendQueue.empty()) {
            return MakeFuture();
        }

        FlushForced = true;
        return FlushPromise;
    }

    TFuture<void> AsyncSeal(int recordCount)
    {
        VERIFY_THREAD_AFFINITY_ANY();

        {
            TGuard<TSpinLock> guard(SpinLock);
            SealForced = true;
            SealRecordCount = recordCount;
        }

        return SealPromise;
    }


    bool HasPendingActions()
    {
        VERIFY_THREAD_AFFINITY_ANY();

        // Unguarded access seems OK.
        auto config = Changelog->GetConfig();
        if (ByteSize >= config->FlushBufferSize) {
            return true;
        }

        if (Changelog->GetLastFlushed() < TInstant::Now() - config->FlushPeriod) {
            return true;
        }

        if (FlushForced) {
            return true;
        }

        if (SealForced) {
            return true;
        }

        return false;
    }

    void RunPendingActions()
    {
        VERIFY_THREAD_AFFINITY(SyncThread);

        SyncFlush();
        SyncSeal();
    }

    bool TrySweep()
    {
        VERIFY_THREAD_AFFINITY_ANY();

        TPromise<void> promise;
        {
            TGuard<TSpinLock> guard(SpinLock);

            if (!AppendQueue.empty() || !FlushQueue.empty()) {
                return false;
            }

            if (SealForced && !SealPromise.IsSet()) {
                return false;
            }

            if (UseCount != 0) {
                return false;
            }

            promise = FlushPromise;
            FlushPromise.Reset();
            FlushForced = false;
        }

        promise.Set();

        return true;
    }
    

    std::vector<TSharedRef> Read(int recordId, int maxRecords, i64 maxBytes)
    {
        VERIFY_THREAD_AFFINITY_ANY();

        // First in-memory record index.
        int flushedRecordCount;
        
        std::vector<TSharedRef> records;

        // First take in-memory records (tail part).
        PROFILE_TIMING ("/changelog_read_copy_time") {
            TGuard<TSpinLock> guard(SpinLock);
            flushedRecordCount = FlushedRecordCount;

            CopyRecords(
                FlushedRecordCount,
                FlushQueue,
                recordId,
                maxRecords,
                &records);

            CopyRecords(
                FlushedRecordCount + FlushQueue.size(),
                AppendQueue,
                recordId,
                maxRecords,
                &records);
        }

        // Then take on-disk records, if needed (head part).
        PROFILE_TIMING ("/changelog_read_io_time") {
            if (recordId < flushedRecordCount) {
                int neededRecordCount = std::min(maxRecords, flushedRecordCount - recordId);
                auto diskResult = Changelog->Read(recordId, neededRecordCount, maxBytes);
                // Combine head + tail.
                diskResult.insert(diskResult.end(), records.begin(), records.end());
                records.swap(diskResult);
            }
        }

        // Trim to enforce size limit.
        i64 actualBytes = 0;
        {
            auto it = records.begin();
            while (it != records.end() && actualBytes <= maxBytes) {
                actualBytes += it->Size();
                ++it;
            }
            records.erase(it, records.end());
        }

        Profiler.Enqueue("/changelog_read_record_count", records.size());
        Profiler.Enqueue("/changelog_read_size", actualBytes);

        return std::move(records);
    }

private:
    TSyncFileChangelogPtr Changelog;

    TSpinLock SpinLock;
    TAtomic UseCount;
    int FlushedRecordCount;
    i64 ByteSize;
    std::vector<TSharedRef> AppendQueue;
    std::vector<TSharedRef> FlushQueue;

    TPromise<void> FlushPromise;
    bool FlushForced;

    TPromise<void> SealPromise;
    bool SealForced;
    int SealRecordCount;


    DECLARE_THREAD_AFFINITY_SLOT(SyncThread);


    static void CopyRecords(
        int firstRecordId,
        const std::vector<TSharedRef>& records,
        int neededFirstRecordId,
        int neededRecordCount,
        std::vector<TSharedRef>* result)
    {
        int size = records.size();
        int beginIndex = neededFirstRecordId - firstRecordId;
        int endIndex = neededFirstRecordId + neededRecordCount - firstRecordId;
        auto beginIt = records.begin() + std::min(std::max(beginIndex, 0), size);
        auto endIt = records.begin() + std::min(std::max(endIndex, 0), size);
        if (endIt != beginIt) {
            result->insert(
                result->end(),
                beginIt,
                endIt);
        }
    }


    void SyncFlush()
    {
        TPromise<void> flushPromise;
        {
            TGuard<TSpinLock> guard(SpinLock);

            YCHECK(FlushQueue.empty());
            FlushQueue.swap(AppendQueue);
            ByteSize = 0;

            YCHECK(FlushPromise);
            flushPromise = FlushPromise;
            FlushPromise = NewPromise();
            FlushForced = false;
        }

        if (!FlushQueue.empty()) {
            PROFILE_TIMING("/changelog_flush_io_time") {
                Changelog->Append(FlushedRecordCount, FlushQueue);
                Changelog->Flush();
            }
        }

        {
            TGuard<TSpinLock> guard(SpinLock);
            FlushedRecordCount += FlushQueue.size();
            FlushQueue.clear();
        }

        flushPromise.Set();
    }

    void SyncSeal()
    {
        TPromise<void> sealPromise;
        {
            TGuard<TSpinLock> guard(SpinLock);
            if (!SealForced)
                return;
            sealPromise = SealPromise;
            SealForced = false;
        }

        while (true) {
            {
                TGuard<TSpinLock> guard(SpinLock);
                if (AppendQueue.empty())
                    break;
            }
            SyncFlush();
        }

        PROFILE_TIMING("/changelog_seal_io_time") {
            Changelog->Seal(SealRecordCount);
        }

        sealPromise.Set();
    }

};

typedef TIntrusivePtr<TChangelogQueue> TChangelogQueuePtr;

////////////////////////////////////////////////////////////////////////////////

class TChangelogDispatcher
{
public:
    static TChangelogDispatcher* Get()
    {
        return Singleton<TChangelogDispatcher>();
    }

    TFuture<void> Append(
        TSyncFileChangelogPtr changelog,
        const TSharedRef& record)
    {
        auto queue = GetQueueAndLock(changelog);
        auto result = queue->Append(record);
        queue->Unlock();
        WakeupEvent.Signal();

        Profiler.Increment(RecordCounter);
        Profiler.Increment(SizeCounter, record.Size());

        return result;
    }

    std::vector<TSharedRef> Read(
        TSyncFileChangelogPtr changelog,
        int recordId,
        int maxRecords,
        i64 maxBytes)
    {
        YCHECK(recordId >= 0);
        YCHECK(maxRecords >= 0);

        if (maxRecords == 0) {
            return std::vector<TSharedRef>();
        }

        auto queue = FindQueueAndLock(changelog);
        if (queue) {
            auto records = queue->Read(recordId, maxRecords, maxBytes);
            queue->Unlock();
            return std::move(records);
        } else {
            PROFILE_TIMING ("/changelog_read_io_time") {
                return changelog->Read(recordId, maxRecords, maxBytes);
            }
        }
    }

    TFuture<void> Flush(TSyncFileChangelogPtr changelog)
    {
        auto queue = FindQueue(changelog);
        return queue ? queue->AsyncFlush() : MakeFuture();
    }

    void Close(TSyncFileChangelogPtr changelog)
    {
        RemoveQueue(changelog);
        changelog->Close();
    }

    TFuture<void> Seal(TSyncFileChangelogPtr changelog, int recordCount)
    {
        auto queue = GetQueueAndLock(changelog);
        auto result = queue->AsyncSeal(recordCount);
        queue->Unlock();
        WakeupEvent.Signal();

        return result;
    }

    void Shutdown()
    {
        Finished = true;
        WakeupEvent.Signal();
        Thread.Join();
    }

private:
    friend TChangelogDispatcher* ::SingletonInt<TChangelogDispatcher>();
    friend void ::Destroyer<TChangelogDispatcher>(void*);

    TChangelogDispatcher()
        : Thread(ThreadFunc, static_cast<void*>(this))
        , WakeupEvent(Event::rManual)
        // XXX(babenko): VS2013 Nov CTP does not have a proper ctor :(
        // , Finished(false)
        , RecordCounter("/record_rate")
        , SizeCounter("/record_throughput")
    {
        Finished = false;
        Thread.Start();
    }

    ~TChangelogDispatcher()
    {
        Shutdown();
    }


    TChangelogQueuePtr FindQueue(TSyncFileChangelogPtr changelog) const
    {
        TGuard<TSpinLock> guard(Spinlock);
        auto it = QueueMap.find(changelog);
        return it == QueueMap.end() ? nullptr : it->second;
    }

    TChangelogQueuePtr FindQueueAndLock(TSyncFileChangelogPtr changelog) const
    {
        TGuard<TSpinLock> guard(Spinlock);
        auto it = QueueMap.find(changelog);
        if (it == QueueMap.end()) {
            return nullptr;
        }

        auto queue = it->second;
        queue->Lock();
        return queue;
    }

    TChangelogQueuePtr GetQueueAndLock(TSyncFileChangelogPtr changelog)
    {
        TGuard<TSpinLock> guard(Spinlock);
        TChangelogQueuePtr queue;

        auto it = QueueMap.find(changelog);
        if (it != QueueMap.end()) {
            queue = it->second;
        } else {
            queue = New<TChangelogQueue>(changelog);
            YCHECK(QueueMap.insert(std::make_pair(changelog, queue)).second);
        }

        queue->Lock();
        return queue;
    }

    void RemoveQueue(TSyncFileChangelogPtr changelog)
    {
        TGuard<TSpinLock> guard(Spinlock);
        QueueMap.erase(changelog);
    }

    void FlushQueues()
    {
        // Take a snapshot.
        std::vector<TChangelogQueuePtr> queues;
        {
            TGuard<TSpinLock> guard(Spinlock);
            for (const auto& pair : QueueMap) {
                const auto& queue = pair.second;
                if (queue->HasPendingActions()) {
                    queues.push_back(queue);
                }
            }
        }

        // Flush and seal the changelogs.
        for (auto queue : queues) {
            queue->RunPendingActions();
        }
    }

    void SweepQueues()
    {
        TGuard<TSpinLock> guard(Spinlock);
        auto it = QueueMap.begin();
        while (it != QueueMap.end()) {
            auto jt = it++;
            auto queue = jt->second;
            if (queue->TrySweep()) {
                QueueMap.erase(jt);
            }
        }
    }


    void ProcessQueues()
    {
        FlushQueues();
        SweepQueues();
    }


    static void* ThreadFunc(void* param)
    {
        auto* this_ = (TChangelogDispatcher*) param;
        this_->ThreadMain();
        return nullptr;
    }

    void ThreadMain()
    {
        NConcurrency::SetCurrentThreadName("ChangelogFlush");

        while (!Finished) {
            ProcessQueues();
            WakeupEvent.Reset();
            WakeupEvent.WaitT(FlushThreadQuantum);
        }
    }


    TSpinLock Spinlock;
    yhash_map<TSyncFileChangelogPtr, TChangelogQueuePtr> QueueMap;

    TThread Thread;
    Event WakeupEvent;
    std::atomic_bool Finished;

    NProfiling::TRateCounter RecordCounter;
    NProfiling::TRateCounter SizeCounter;

};

// TODO(babenko): get rid of this
void ShutdownChangelogs()
{
    TChangelogDispatcher::Get()->Shutdown();
}

////////////////////////////////////////////////////////////////////////////////

class TFileChangelog
    : public IChangelog
{
public:
    TFileChangelog(
        TFileChangelogConfigPtr config,
        TSyncFileChangelogPtr changelog)
        : Config(config)
        , SyncChangelog(changelog)
        , RecordCount(changelog->GetRecordCount())
    { }

    virtual int GetId() override
    {
        return SyncChangelog->GetId();
    }

    virtual int GetRecordCount() override
    {
        return static_cast<int>(RecordCount);
    }

    virtual int GetPrevRecordCount() override
    {
        return SyncChangelog->GetPrevRecordCount();
    }

    virtual bool IsSealed() override
    {
        return SyncChangelog->IsSealed();
    }

    virtual TFuture<void> Append(const TSharedRef& data) override
    {
        AtomicIncrement(RecordCount);
        return TChangelogDispatcher::Get()->Append(
            SyncChangelog,
            data);
    }

    virtual TFuture<void> Flush() override
    {
        return TChangelogDispatcher::Get()->Flush(
            SyncChangelog);
    }

    virtual void Close() override
    {
        return TChangelogDispatcher::Get()->Close(
            SyncChangelog);
    }

    virtual std::vector<TSharedRef> Read(
        int firstRecordId,
        int maxRecords,
        i64 maxBytes) override
    {
        return TChangelogDispatcher::Get()->Read(
            SyncChangelog,
            firstRecordId,
            maxRecords,
            maxBytes);
    }

    virtual TFuture<void> Seal(int recordCount) override
    {
        YCHECK(recordCount <= RecordCount);
        AtomicSet(RecordCount, recordCount);

        return TChangelogDispatcher::Get()->Seal(
            SyncChangelog,
            recordCount);
    }

    virtual void Unseal() override
    {
        SyncChangelog->Unseal();
    }

private:
    TFileChangelogConfigPtr Config;
    TSyncFileChangelogPtr SyncChangelog;

    TAtomic RecordCount;

};

IChangelogPtr CreateFileChangelog(
    const Stroka& path,
    int id,
    const TChangelogCreateParams& params,
    TFileChangelogConfigPtr config)
{
    auto syncChangelog = New<TSyncFileChangelog>(
        path,
        id,
        config);
    syncChangelog->Create(params);
    return New<TFileChangelog>(
        config,
        syncChangelog);
}

IChangelogPtr OpenFileChangelog(
    const Stroka& path,
    int id,
    TFileChangelogConfigPtr config)
{
    auto syncChangelog = New<TSyncFileChangelog>(
        path,
        id,
        config);
    syncChangelog->Open();
    return New<TFileChangelog>(
        config,
        syncChangelog);
}

////////////////////////////////////////////////////////////////////////////////

class TCachedFileChangelog
    : public TCacheValueBase<int, TCachedFileChangelog>
    , public TFileChangelog
{
public:
    explicit TCachedFileChangelog(
        TFileChangelogConfigPtr config,
        TSyncFileChangelogPtr changelog)
        : TCacheValueBase(changelog->GetId())
        , TFileChangelog(config, changelog)
    { }

};

class TFileChangelogStore
    : public TSizeLimitedCache<int, TCachedFileChangelog>
    , public IChangelogStore
{
public:
    TFileChangelogStore(
        const TCellGuid& cellGuid,
        TFileChangelogStoreConfigPtr config)
        : TSizeLimitedCache(config->MaxCachedChangelogs)
        , CellGuid(cellGuid)
        , Config(config)
        , Logger(HydraLogger)
    {
        Logger.AddTag(Sprintf("Path: %s", ~Config->Path));
    }

    void Start()
    {
        LOG_DEBUG("Preparing changelog store");

        NFS::ForcePath(Config->Path);
        NFS::CleanTempFiles(Config->Path);
    }

    virtual const TCellGuid& GetCellGuid() const override
    {
        return CellGuid;
    }

    virtual IChangelogPtr CreateChangelog(
        int id,
        const TChangelogCreateParams& params) override
    {
        TInsertCookie cookie(id);
        if (!BeginInsert(&cookie)) {
            LOG_FATAL("Trying to create an already existing changelog %d",
                id);
        }

        auto path = GetChangelogPath(id);

        try {
            auto changelog = New<TSyncFileChangelog>(
                path,
                id,
                Config);
            changelog->Create(params);
            cookie.EndInsert(New<TCachedFileChangelog>(
                Config,
                changelog));
        } catch (const std::exception& ex) {
            LOG_FATAL(ex, "Error creating changelog %d", id);
        }

        return cookie.GetValue().Get().GetValue();
    }

    virtual IChangelogPtr TryOpenChangelog(int id) override
    {
        TInsertCookie cookie(id);
        if (BeginInsert(&cookie)) {
            auto path = GetChangelogPath(id);
            if (!isexist(~path)) {
                cookie.Cancel(TError(
                    NHydra::EErrorCode::NoSuchChangelog,
                    "No such changelog %d",
                    id));
            } else {
                try {
                    auto changelog = New<TSyncFileChangelog>(
                        path,
                        id,
                        Config);
                    changelog->Open();
                    cookie.EndInsert(New<TCachedFileChangelog>(
                        Config,
                        changelog));
                } catch (const std::exception& ex) {
                    LOG_FATAL(ex, "Error opening changelog %d", id);
                }
            }
        }

        auto changelogOrError = cookie.GetValue().Get();
        return changelogOrError.IsOK() ? changelogOrError.GetValue() : nullptr;
    }

    virtual int GetLatestChangelogId(int initialId) override
    {
        for (int id = initialId; ; ++id) {
            auto path = GetChangelogPath(id);
            if (!isexist(~path)) {
                return id == initialId ? NonexistingSegmentId : id - 1;
            }
        }
    }

private:
    TCellGuid CellGuid;
    TFileChangelogStoreConfigPtr Config;

    NLog::TTaggedLogger Logger;

    Stroka GetChangelogPath(int id)
    {
        return NFS::CombinePaths(Config->Path, Sprintf("%09d", id) + LogSuffix);
    }

};

IChangelogStorePtr CreateFileChangelogStore(
    const TCellGuid& cellGuid,
    TFileChangelogStoreConfigPtr config)
{
    auto store = New<TFileChangelogStore>(
        cellGuid,
        config);
    store->Start();
    return store;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NHydra
} // namespace NYT

