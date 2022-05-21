#pragma once

#include "private.h"

#include <yt/yt/server/lib/io/io_engine.h>

#include <yt/yt/core/actions/future.h>

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

class TFileChangelogIndex
    : public TRefCounted
{
public:
    TFileChangelogIndex(
        NIO::IIOEnginePtr ioEngine,
        TString fileName,
        TFileChangelogConfigPtr config);

    EFileChangelogIndexOpenResult Open();
    void Create();

    void Close();

    int GetRecordCount() const;
    i64 GetDataFileLength() const;
    std::pair<i64, i64> GetRecordRange(int index) const;
    std::pair<i64, i64> GetRecordsRange(
        int firstRecordIndex,
        int maxRecords,
        i64 maxBytes) const;

    void AppendRecord(int index, std::pair<i64, i64> range);
    void SetFlushedDataRecordCount(int count);

    TFuture<void> Flush();
    bool CanFlush() const;

private:
    const NIO::IIOEnginePtr IOEngine_;
    const TString FileName_;
    const TFileChangelogConfigPtr Config_;

    const NLogging::TLogger Logger;

    NIO::TIOEngineHandlePtr Handle_;
    i64 IndexFilePosition_ = 0;

    std::vector<i64> RecordOffsets_;
    int FlushedDataRecordCount_ = 0;
    int FlushedIndexRecordCount_ = 0;
    i64 DataFileLength_ = -1;

    std::atomic<bool> Flushing_ = false;
    TFuture<void> FlushFuture_ = VoidFuture;


    void Clear();
};

DEFINE_REFCOUNTED_TYPE(TFileChangelogIndex)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHydra
