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

#include <util/generic/singleton.h>

#include <atomic>

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

        ++UseCount;
    }

    void Unlock()
    {
        VERIFY_THREAD_AFFINITY_ANY();

        --UseCount;
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

            if (UseCount.load() > 0) {
                return false;
            }

            promise = FlushPromise;
            FlushPromise.Reset();
            FlushForced = false;
        }

        promise.Set();

        return true;
    }
    

    std::vector<TSharedRef> Read(int firstRecordId, int maxRecords, i64 maxBytes)
    {
        VERIFY_THREAD_AFFINITY_ANY();

        std::vector<TSharedRef> records;
        int currentRecordId = firstRecordId;
        int needRecords = maxRecords;
        i64 needBytes = maxBytes;
        i64 readBytes = 0;

        auto appendRecord = [&] (const TSharedRef& record) {
            records.push_back(record);
            --needRecords;
            needBytes -= record.Size();
            readBytes += record.Size();
        };

        auto needMore = [&] () {
            return needRecords > 0 && needBytes > 0;
        };

        while (needMore()) {
            TGuard<TSpinLock> guard(SpinLock);
            if (currentRecordId < FlushedRecordCount) {
                // Read from disk, w/o spinlock.
                guard.Release();

                PROFILE_TIMING ("/changelog_read_io_time") {
                    auto diskRecords = Changelog->Read(currentRecordId, needRecords, needBytes);
                    for (const auto& record : diskRecords) {
                        appendRecord(record);
                    }
                }
            } else {
                // Read from memory, w/ spinlock.

                auto readFromMemory = [&] (const std::vector<TSharedRef>& memoryRecords, int firstMemoryRecordId) {
                    if (!needMore())
                        return;
                    int memoryIndex = currentRecordId - firstMemoryRecordId;
                    YCHECK(memoryIndex >= 0);
                    while (memoryIndex < static_cast<int>(memoryRecords.size()) && needMore()) {
                        appendRecord(memoryRecords[memoryIndex]);
                    }
                };

                PROFILE_TIMING ("/changelog_read_copy_time") {
                    readFromMemory(FlushQueue, FlushedRecordCount);
                    readFromMemory(AppendQueue, FlushedRecordCount + FlushQueue.size());
                }

                // Break since we don't except more records beyond this point.
                break;
            }
        }

        Profiler.Enqueue("/changelog_read_record_count", records.size());
        Profiler.Enqueue("/changelog_read_size", readBytes);

        return records;
    }

private:
    TSyncFileChangelogPtr Changelog;

    TSpinLock SpinLock;
    std::atomic<int> UseCount;

    //! Number of records flushed to the underlying sync changelog.
    int FlushedRecordCount;
    //! These records are currently being flushed to the underlying sync changelog and
    //! immediately follow the flushed part.
    std::vector<TSharedRef> FlushQueue;
    //! Newly appended records to here. These records immediately follow the flush part.
    std::vector<TSharedRef> AppendQueue;

    i64 ByteSize;

    TPromise<void> FlushPromise;
    bool FlushForced;

    TPromise<void> SealPromise;
    bool SealForced;
    int SealRecordCount;


    DECLARE_THREAD_AFFINITY_SLOT(SyncThread);


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
    DECLARE_SINGLETON_FRIEND(TChangelogDispatcher)

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
        TGuard<TSpinLock> guard(SpinLock);
        auto it = QueueMap.find(changelog);
        return it == QueueMap.end() ? nullptr : it->second;
    }

    TChangelogQueuePtr FindQueueAndLock(TSyncFileChangelogPtr changelog) const
    {
        TGuard<TSpinLock> guard(SpinLock);
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
        TGuard<TSpinLock> guard(SpinLock);
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
        TGuard<TSpinLock> guard(SpinLock);
        QueueMap.erase(changelog);
    }

    void FlushQueues()
    {
        // Take a snapshot.
        std::vector<TChangelogQueuePtr> queues;
        {
            TGuard<TSpinLock> guard(SpinLock);
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
        TGuard<TSpinLock> guard(SpinLock);
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


    TSpinLock SpinLock;
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
        : Config_(config)
        , SyncChangelog_(changelog)
        , RecordCount_(changelog->GetRecordCount())
        , DataSize_(changelog->GetDataSize())
    { }

    virtual int GetId() const override
    {
        return SyncChangelog_->GetId();
    }

    virtual int GetRecordCount() const override
    {
        return RecordCount_;
    }

    virtual i64 GetDataSize() const override
    {
        return DataSize_;
    }

    virtual int GetPrevRecordCount() const override
    {
        return SyncChangelog_->GetPrevRecordCount();
    }

    virtual bool IsSealed() const override
    {
        return SyncChangelog_->IsSealed();
    }

    virtual TFuture<void> Append(const TSharedRef& data) override
    {
        RecordCount_ += 1;
        DataSize_ += data.Size();

        return TChangelogDispatcher::Get()->Append(
            SyncChangelog_,
            data);
    }

    virtual TFuture<void> Flush() override
    {
        return TChangelogDispatcher::Get()->Flush(
            SyncChangelog_);
    }

    virtual void Close() override
    {
        return TChangelogDispatcher::Get()->Close(
            SyncChangelog_);
    }

    virtual std::vector<TSharedRef> Read(
        int firstRecordId,
        int maxRecords,
        i64 maxBytes) const override
    {
        return TChangelogDispatcher::Get()->Read(
            SyncChangelog_,
            firstRecordId,
            maxRecords,
            maxBytes);
    }

    virtual TFuture<void> Seal(int recordCount) override
    {
        YCHECK(recordCount <= RecordCount_);
        RecordCount_.store(recordCount);

        return TChangelogDispatcher::Get()->Seal(
            SyncChangelog_,
            recordCount);
    }

    virtual void Unseal() override
    {
        SyncChangelog_->Unseal();
    }

private:
    TFileChangelogConfigPtr Config_;
    TSyncFileChangelogPtr SyncChangelog_;

    std::atomic<int> RecordCount_;
    std::atomic<i64> DataSize_;

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
        , CellGuid_(cellGuid)
        , Config_(config)
        , Logger(HydraLogger)
    {
        Logger.AddTag(Sprintf("Path: %s", ~Config_->Path));
    }

    void Start()
    {
        LOG_DEBUG("Preparing changelog store");

        NFS::ForcePath(Config_->Path);
        NFS::CleanTempFiles(Config_->Path);
    }

    virtual const TCellGuid& GetCellGuid() const override
    {
        return CellGuid_;
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
                Config_);
            changelog->Create(params);
            cookie.EndInsert(New<TCachedFileChangelog>(
                Config_,
                changelog));
        } catch (const std::exception& ex) {
            LOG_FATAL(ex, "Error creating changelog %d", id);
        }

        return cookie.GetValue().Get().Value();
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
                        Config_);
                    changelog->Open();
                    cookie.EndInsert(New<TCachedFileChangelog>(
                        Config_,
                        changelog));
                } catch (const std::exception& ex) {
                    LOG_FATAL(ex, "Error opening changelog %d", id);
                }
            }
        }

        auto changelogOrError = cookie.GetValue().Get();
        return changelogOrError.IsOK() ? changelogOrError.Value() : nullptr;
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
    TCellGuid CellGuid_;
    TFileChangelogStoreConfigPtr Config_;

    NLog::TTaggedLogger Logger;

    Stroka GetChangelogPath(int id)
    {
        return NFS::CombinePaths(Config_->Path, Sprintf("%09d", id) + LogSuffix);
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

