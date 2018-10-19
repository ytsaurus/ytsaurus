#pragma once

#include "format.h"
#include "file_helpers.h"

#include <yt/core/actions/future.h>

#include <yt/ytlib/chunk_client/public.h>

#include <util/system/align.h>

namespace NYT {
namespace NHydra {

////////////////////////////////////////////////////////////////////////////////

class TIndexBucket
    : public TRefCounted
{
public:
    TIndexBucket(size_t capacity, i64 alignment, i64 offset);

    void PushHeader();
    void Push(const TChangelogIndexRecord& record);

    TFuture<void> Write(const std::shared_ptr<TFileHandle>& file, const NChunkClient::IIOEnginePtr& io) const;
    void UpdateRecordCount(int newRecordCount);
    i64 GetOffset() const;
    int GetCurrentIndexId() const;
    bool HasSpace() const;

private:
    size_t Capacity_;
    int CurrentIndexId_ = 0;
    TSharedMutableRef Data_;
    i64 Offset_;
    TChangelogIndexRecord* Index_;
    TChangelogIndexHeader* Header_ = nullptr;
};

class TAsyncFileChangelogIndex
{
public:
    TAsyncFileChangelogIndex(
        const NChunkClient::IIOEnginePtr& IOEngine,
        const TString& fileName,
        i64 alignment,
        i64 indexBlockSize);

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

    void Read(const TNullable<i32>& truncatedRecordCount = Null);
    void TruncateInvalidRecords(i64 correctPrefixSize);

    template <class TTag>
    static TSharedMutableRef AllocateAligned(size_t size, bool initializeStorage, size_t alignment)
    {
        auto data = TSharedMutableRef::Allocate<TTag>(size + alignment, initializeStorage);
        data = data.Slice(AlignUp(data.Begin(), alignment), data.End());
        data = data.Slice(data.Begin(), data.Begin() + size);
        return data;
    }

private:
    void ProcessRecord(int recordId, i64 filePosition, int recordSize);
    TFuture<void> FlushDirtyBuckets();
    void UpdateIndexBuckets();

    const NChunkClient::IIOEnginePtr IOEngine_;
    const TString IndexFileName_;
    const i64 Alignment_;
    const i64 IndexBlockSize_;
    const int MaxIndexRecordsPerBucket_;

    std::vector<TChangelogIndexRecord> Index_;
    std::shared_ptr<TFileHandle> IndexFile_;

    i64 CurrentBlockSize_ = 0;

    TIntrusivePtr<TIndexBucket> FirstIndexBucket_;
    TIntrusivePtr<TIndexBucket> CurrentIndexBucket_;

    std::vector<TIntrusivePtr<TIndexBucket>> DirtyBuckets_;
    bool HasDirtyBuckets_ = false;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NHydra
} // namespace NYT
