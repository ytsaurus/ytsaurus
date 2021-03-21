#pragma once

#include "format.h"
#include "file_helpers.h"

#include <yt/yt/server/lib/io/public.h>

#include <yt/yt/ytlib/chunk_client/public.h>

#include <yt/yt/core/actions/future.h>

namespace NYT::NHydra {

////////////////////////////////////////////////////////////////////////////////

class TIndexBucket
    : public TRefCounted
{
public:
    TIndexBucket(size_t capacity, i64 offset);

    void PushHeader();
    void Push(const TChangelogIndexRecord& record);

    TFuture<void> Write(const std::shared_ptr<TFileHandle>& file, const NIO::IIOEnginePtr& ioEngine) const;
    void UpdateRecordCount(int newRecordCount);
    i64 GetOffset() const;
    int GetCurrentIndexId() const;
    bool HasSpace() const;

private:
    const size_t Capacity_;
    const i64 Offset_;

    int CurrentIndexId_ = 0;
    TSharedMutableRef Data_;
    TChangelogIndexRecord* Index_;
    TChangelogIndexHeader* Header_ = nullptr;
};

////////////////////////////////////////////////////////////////////////////////

class TAsyncFileChangelogIndex
{
public:
    TAsyncFileChangelogIndex(
        const NIO::IIOEnginePtr& ioEngine,
        const TString& fileName,
        i64 indexBlockSize,
        bool enableSync);

    void Create();
    TFuture<void> FlushData();
    void Close();

    void Append(int firstRecordId, i64 filePosition, const std::vector<int>& appendSizes);
    void Append(int recordId, i64 filePosition, int recordSize);
    bool IsEmpty() const;
    const TChangelogIndexRecord& LastRecord() const;
    const std::vector<TChangelogIndexRecord>& Records() const;

    void Search(
        TChangelogIndexRecord* lowerBound,
        TChangelogIndexRecord* upperBound,
        int firstRecordId,
        int lastRecordId,
        i64 maxBytes = -1) const;

    void Read(std::optional<int> truncatedRecordCount = {});
    void TruncateInvalidRecords(i64 validPrefixSize);

private:
    const NIO::IIOEnginePtr IOEngine_;
    const TString IndexFileName_;
    const i64 IndexBlockSize_;
    const int MaxIndexRecordsPerBucket_;
    const bool EnableSync_;

    std::vector<TChangelogIndexRecord> Index_;
    std::shared_ptr<TFileHandle> IndexFile_;

    i64 CurrentBlockSize_ = 0;

    TIntrusivePtr<TIndexBucket> FirstIndexBucket_;
    TIntrusivePtr<TIndexBucket> CurrentIndexBucket_;

    std::vector<TIntrusivePtr<TIndexBucket>> DirtyBuckets_;
    bool HasDirtyBuckets_ = false;


    void ProcessRecord(int recordId, i64 filePosition, int recordSize);
    TFuture<void> FlushDirtyBuckets();
    void UpdateIndexBuckets();
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHydra
