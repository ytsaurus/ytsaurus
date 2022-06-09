#pragma once

#include "private.h"

#include <yt/yt/server/lib/io/io_engine.h>

#include <yt/yt/core/actions/future.h>

#include <yt/yt/core/misc/memory_usage_tracker.h>

#include <library/cpp/yt/threading/rw_spin_lock.h>

#include <atomic>

namespace NYT::NHydra {

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EFileChangelogIndexOpenResult,
    (MissingCreated)
    (ExistingRecreatedBrokenIndexHeader)
    (ExistingRecreatedBrokenSegmentHeader)
    (ExistingRecreatedBrokenSegmentChecksum)
    (ExistingOpened)
    (ExistingTruncatedBrokenSegment)
);


//! Maintains an in-memory index of all changelog records.
/*!
 *  The instances are single-threaded unless noted otherwise.
 */
class TFileChangelogIndex
    : public TRefCounted
{
public:
    TFileChangelogIndex(
        NIO::IIOEnginePtr ioEngine,
        IMemoryUsageTrackerPtr memoryUsageTracker,
        TString fileName,
        TFileChangelogConfigPtr config);

    EFileChangelogIndexOpenResult Open();
    void Create();

    void Close();

    /*
     *  \note
     *  Thread affinity: any
     */
    int GetRecordCount() const;

    /*
     *  \note
     *  Thread affinity: any
     */
    int GetFlushedDataRecordCount() const;

    /*
     *  \note
     *  Thread affinity: any
     */
    std::pair<i64, i64> GetRecordRange(int recordIndex) const;

    /*x
     *  \note
     *  Thread affinity: any
     */
    std::pair<i64, i64> GetRecordsRange(
        int firstRecordIndex,
        int maxRecords,
        i64 maxBytes) const;

    void AppendRecord(int recordIndex, std::pair<i64, i64> range);
    void SetFlushedDataRecordCount(int count);

    TFuture<void> Flush();
    bool CanFlush() const;

private:
    const NIO::IIOEnginePtr IOEngine_;
    const TString FileName_;
    const TFileChangelogConfigPtr Config_;

    const NLogging::TLogger Logger;

    TMemoryUsageTrackerGuard MemoryUsageTrackerGuard_;

    NIO::TIOEngineHandlePtr Handle_;
    i64 IndexFilePosition_ = 0;

    std::atomic<int> RecordCount_ = 0;
    std::atomic<int> FlushedDataRecordCount_ = 0;
    int FlushedIndexRecordCount_ = 0;
    i64 DataFileLength_ = -1;

    std::atomic<bool> Flushing_ = false;
    TFuture<void> FlushFuture_ = VoidFuture;

    struct TRecord
    {
        i64 Offset = -1;
    };

    static constexpr int RecordsPerChunk = 10240;
    using TChunk = TSharedMutableRange<TRecord>;

    YT_DECLARE_SPIN_LOCK(NThreading::TReaderWriterSpinLock, ChunkListLock_);
    std::vector<TChunk> ChunkList_;


    void Clear();
    TChunk AllocateChunk();
    static std::pair<int, int> GetRecordChunkIndexes(int recordIndex);
    i64 GetRecordOffset(int recordIndex) const;
};

DEFINE_REFCOUNTED_TYPE(TFileChangelogIndex)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHydra
