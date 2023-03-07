#include "async_file_changelog_index.h"
#include "format.h"
#include "private.h"

#include <yt/core/misc/fs.h>

#include <yt/ytlib/chunk_client/io_engine.h>

namespace NYT::NHydra {

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = HydraLogger;

////////////////////////////////////////////////////////////////////////////////

namespace {

// This method uses forward iterator instead of reverse because they work faster.
// Asserts if last not greater element is absent.
bool CompareRecordIds(const TChangelogIndexRecord& lhs, const TChangelogIndexRecord& rhs)
{
    return lhs.RecordId < rhs.RecordId;
}

bool CompareFilePositions(const TChangelogIndexRecord& lhs, const TChangelogIndexRecord& rhs)
{
    return lhs.FilePosition < rhs.FilePosition;
}

template <class T>
typename std::vector<T>::const_iterator LastNotGreater(
    const std::vector<T>& vec,
    const T& value)
{
    auto res = std::upper_bound(vec.begin(), vec.end(), value);
    YT_VERIFY(res != vec.begin());
    --res;
    return res;
}

template <class T, class TComparator>
typename std::vector<T>::const_iterator LastNotGreater(
    const std::vector<T>& vec,
    const T& value,
    TComparator comparator)
{
    auto res = std::upper_bound(vec.begin(), vec.end(), value, comparator);
    YT_VERIFY(res != vec.begin());
    --res;
    return res;
}

template <class T>
typename std::vector<T>::const_iterator FirstGreater(
    const std::vector<T>& vec,
    const T& value)
{
    return std::upper_bound(vec.begin(), vec.end(), value);
}

template <class T, class TComparator>
typename std::vector<T>::const_iterator FirstGreater(
    const std::vector<T>& vec,
    const T& value,
    TComparator comparator)
{
    return std::upper_bound(vec.begin(), vec.end(), value, comparator);
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

struct TIndexBucketDataTag
{ };

TIndexBucket::TIndexBucket(size_t capacity, i64 alignment, i64 offset)
    : Capacity_(capacity)
    , Offset_(offset)
    , Data_(TAsyncFileChangelogIndex::AllocateAligned<TIndexBucketDataTag>(capacity * sizeof(TChangelogIndexRecord), true, alignment))
{
    auto maxCurrentIndexRecords = alignment / sizeof(TChangelogIndexRecord);
    Index_ = reinterpret_cast<TChangelogIndexRecord*>(Data_.Begin());

    for (int i = 0; i < maxCurrentIndexRecords; ++i) {
        Index_[i].FilePosition = -1;
        Index_[i].RecordId = -1;
        Index_[i].Padding = 0;
    }
}

TFuture<void> TIndexBucket::Write(const std::shared_ptr<TFileHandle>& file, const NChunkClient::IIOEnginePtr& ioEngine) const
{
    return ioEngine->Pwrite(file, Data_, Offset_);
}

void TIndexBucket::Push(const TChangelogIndexRecord& record)
{
    Index_[CurrentIndexId_++] = record;
}

void TIndexBucket::PushHeader()
{
    YT_VERIFY(CurrentIndexId_ == 0);

    static_assert(sizeof(TChangelogIndexHeader) <= sizeof(TChangelogIndexRecord),
        "sizeof(TChangelogIndexHeader) <= sizeof(TChangelogIndexRecord)");

    CurrentIndexId_++;
    Header_ = reinterpret_cast<TChangelogIndexHeader*>(Data_.Begin());
    Zero(*Header_);
    Header_->Signature = TChangelogIndexHeader::ExpectedSignature;
}

void TIndexBucket::UpdateRecordCount(int newRecordCount)
{
    YT_VERIFY(Offset_ == 0);
    Header_->IndexRecordCount = newRecordCount;
}

i64 TIndexBucket::GetOffset() const
{
    return Offset_;
}

int TIndexBucket::GetCurrentIndexId() const
{
    return CurrentIndexId_;
}

bool TIndexBucket::HasSpace() const
{
    return CurrentIndexId_ < Capacity_;
}

////////////////////////////////////////////////////////////////////////////////

TAsyncFileChangelogIndex::TAsyncFileChangelogIndex(
    const NChunkClient::IIOEnginePtr& IOEngine,
    const TString& name,
    i64 alignment,
    i64 indexBlockSize)
    : IOEngine_(IOEngine)
    , IndexFileName_(name)
    , Alignment_(alignment)
    , IndexBlockSize_(indexBlockSize)
    , MaxIndexRecordsPerBucket_(Alignment_ / sizeof(TChangelogIndexRecord))
    , FirstIndexBucket_(New<TIndexBucket>(MaxIndexRecordsPerBucket_, Alignment_, 0))
    , CurrentIndexBucket_(FirstIndexBucket_)
{
    FirstIndexBucket_->PushHeader();
    YT_VERIFY(Alignment_ % sizeof(TChangelogIndexRecord) == 0);
}

//! Creates an empty index file.
void TAsyncFileChangelogIndex::Create()
{
    NTracing::TNullTraceContextGuard nullTraceContextGuard;

    auto tempFileName = IndexFileName_ + NFS::TempFileSuffix;
    TFile tempFile(tempFileName, WrOnly|CreateAlways);

    TChangelogIndexHeader header;
    Zero(header);
    header.Signature = TChangelogIndexHeader::ExpectedSignature;
    WritePod(tempFile, header);

    tempFile.FlushData();
    tempFile.Close();

    NFS::Replace(tempFileName, IndexFileName_);

    IndexFile_ = IOEngine_->Open(IndexFileName_, WrOnly | CloseOnExec).Get().ValueOrThrow();
}

void TAsyncFileChangelogIndex::Read(std::optional<int> truncatedRecordCount)
{
    NTracing::TNullTraceContextGuard nullTraceContextGuard;

    // Create index if it is missing.
    if (!NFS::Exists(IndexFileName_) ||
        TFile(IndexFileName_, RdOnly).GetLength() < sizeof(TChangelogIndexHeader))
    {
        Create();
    }

    // Read the existing index.
    {
        TMappedFileInput indexStream(IndexFileName_);

        // Read and check index header.
        TChangelogIndexHeader indexHeader;
        indexStream.Load(&indexHeader, 12);

        YT_LOG_FATAL_UNLESS(indexHeader.Signature == TChangelogIndexHeader::ExpectedSignature,
            "Invalid changelog index signature %" PRIx64,
            indexHeader.Signature);
        YT_VERIFY(indexHeader.IndexRecordCount >= 0);
        if (indexHeader.Signature == indexHeader.ExpectedSignature) {
            indexStream.Skip(sizeof(indexHeader.Padding));
        }

        // Read index records.
        for (int i = 0; i < indexHeader.IndexRecordCount; ++i) {
            if (indexStream.Avail() < sizeof(TChangelogIndexRecord)) {
                break;
            }

            TChangelogIndexRecord indexRecord;
            ReadPod(indexStream, indexRecord);
            if (truncatedRecordCount && indexRecord.RecordId >= *truncatedRecordCount) {
                break;
            }
            Index_.push_back(indexRecord);
        }
    }
}

void TAsyncFileChangelogIndex::TruncateInvalidRecords(i64 validPrefixSize)
{
    YT_VERIFY(validPrefixSize <= Index_.size());
    YT_LOG_WARNING_IF(
        validPrefixSize < Index_.size(),
        "Changelog index contains invalid records, truncated (ValidPrefixSize: %v, IndexSize: %v)",
        validPrefixSize,
        Index_.size());
    Index_.resize(validPrefixSize);

    FirstIndexBucket_->UpdateRecordCount(Index_.size());

    auto totalRecordCount = Index_.size();
    // first bucket contains header
    auto firstRecordId = FirstIndexBucket_->GetCurrentIndexId();
    auto firstBlockRecordCount = std::min<int>((MaxIndexRecordsPerBucket_ - firstRecordId), totalRecordCount);

    for (int index = 0; index < firstBlockRecordCount; ++index) {
        FirstIndexBucket_->Push(Index_[index]);
    }

    if ((totalRecordCount + firstRecordId) >= MaxIndexRecordsPerBucket_) {
        // [FirstIndexBucket][IndexBucket][IndexBucket]...[CurrentIndexBucket]
        // fill CurrentIndexBucket from Index

        // calculate record index of the first record of CurrentIndexBucket
        auto recordIndex = (totalRecordCount + firstRecordId) / MaxIndexRecordsPerBucket_;
        recordIndex *= MaxIndexRecordsPerBucket_;
        recordIndex -= firstRecordId;

        // calculate file offset of CurrentIndexBucket
        auto indexOffset = (recordIndex + firstRecordId) * sizeof(TChangelogIndexRecord);

        YT_VERIFY(indexOffset % Alignment_ == 0);

        CurrentIndexBucket_ = New<TIndexBucket>(MaxIndexRecordsPerBucket_, Alignment_, indexOffset);
        auto recordCount = totalRecordCount - recordIndex;

        YT_VERIFY(recordCount < MaxIndexRecordsPerBucket_);

        for (int index = 0; index < recordCount; ++index) {
            CurrentIndexBucket_->Push(Index_[recordIndex + index]);
        }
    }

    {
        NTracing::TNullTraceContextGuard nullTraceContextGuard;
        IndexFile_ = IOEngine_->Open(IndexFileName_, WrOnly | CloseOnExec).Get().ValueOrThrow();
        IndexFile_->Resize(sizeof(TChangelogIndexHeader) + Index_.size() * sizeof(TChangelogIndexRecord));
    }
}

void TAsyncFileChangelogIndex::Search(
    TChangelogIndexRecord* lowerBound,
    TChangelogIndexRecord* upperBound,
    int firstRecordId,
    int lastRecordId,
    i64 maxBytes) const
{
    YT_VERIFY(!Index_.empty());

    TChangelogIndexRecord firstRecordPivot;
    firstRecordPivot.RecordId = firstRecordId;
    *lowerBound = *LastNotGreater(Index_, firstRecordPivot, CompareRecordIds);

    TChangelogIndexRecord lastRecordPivot;
    lastRecordPivot.RecordId = lastRecordId;
    auto it = FirstGreater(Index_, lastRecordPivot, CompareRecordIds);
    
    if (maxBytes != -1) {
        TChangelogIndexRecord maxPositionPivot;
        maxPositionPivot.FilePosition = lowerBound->FilePosition + maxBytes;
        it = std::min(it, FirstGreater(Index_, maxPositionPivot, CompareFilePositions));
    }

    if (it != Index_.end()) {
        *upperBound = *it;
    }
}

TFuture<void> TAsyncFileChangelogIndex::FlushDirtyBuckets()
{
    if (!HasDirtyBuckets_) {
        return VoidFuture;
    }

    std::vector<TFuture<void>> asyncResults;
    asyncResults.reserve(DirtyBuckets_.size() + 2);

    if (FirstIndexBucket_ != CurrentIndexBucket_) {
        asyncResults.push_back(FirstIndexBucket_->Write(IndexFile_, IOEngine_));
    }

    for (const auto& block : DirtyBuckets_) {
        asyncResults.push_back(block->Write(IndexFile_, IOEngine_));
    }

    asyncResults.push_back(CurrentIndexBucket_->Write(IndexFile_, IOEngine_));

    DirtyBuckets_.clear();
    HasDirtyBuckets_ = false;

    return Combine(asyncResults);
}

void TAsyncFileChangelogIndex::UpdateIndexBuckets()
{
    auto& indexRecord = Index_.back();

    CurrentIndexBucket_->Push(indexRecord);

    if (!CurrentIndexBucket_->HasSpace()) {
        auto bucketOffset = CurrentIndexBucket_->GetOffset() + MaxIndexRecordsPerBucket_ * sizeof(TChangelogIndexRecord);

        if (CurrentIndexBucket_ != FirstIndexBucket_) {
            DirtyBuckets_.push_back(std::move(CurrentIndexBucket_));
        }

        CurrentIndexBucket_ = New<TIndexBucket>(MaxIndexRecordsPerBucket_, Alignment_, bucketOffset);
    }

    FirstIndexBucket_->UpdateRecordCount(Index_.size());

    HasDirtyBuckets_ = true;
}

//! Processes records that are being or written.
/*!
 *  Checks record id for correctness, updates index, record count,
 *  current block size and current file position.
 */
void TAsyncFileChangelogIndex::ProcessRecord(int recordId, i64 currentFilePosition, int totalSize)
{
    YT_VERIFY(CurrentBlockSize_ >= 0);

    // We add a new index record
    // 1) for the very first data record; or
    // 2) if the size of data records added since last index record exceeds IndexBlockSize.
    if (recordId == 0 || CurrentBlockSize_ >= IndexBlockSize_) {
        YT_VERIFY(Index_.empty() || Index_.back().RecordId < recordId);

        CurrentBlockSize_ = 0;

        auto& record = Index_.emplace_back();
        Zero(record);
        record.RecordId = recordId;
        record.FilePosition = currentFilePosition;

        YT_LOG_DEBUG("Changelog index record added (RecordId: %v, Offset: %v)",
            recordId,
            currentFilePosition);

        UpdateIndexBuckets();
    }
    // Record appended successfully.
    CurrentBlockSize_ += totalSize;
}

void TAsyncFileChangelogIndex::Append(int firstRecordId, i64 filePosition, const std::vector<int>& appendSizes)
{
    int totalRecords = appendSizes.size();
    for (int index = 0; index < totalRecords; ++index) {
        auto recordSize = appendSizes[index];
        ProcessRecord(firstRecordId + index, filePosition, recordSize);
        filePosition += recordSize;
    }
}

void TAsyncFileChangelogIndex::Append(int firstRecordId, i64 filePosition, int recordSize)
{
    return Append(firstRecordId, filePosition, std::vector{recordSize});
}

TFuture<void> TAsyncFileChangelogIndex::FlushData()
{
    if (HasDirtyBuckets_) {
        std::vector<TFuture<void>> asyncResults;
        asyncResults.reserve(2);
        asyncResults.push_back(FlushDirtyBuckets());
        asyncResults.push_back(IOEngine_->FlushData(IndexFile_).As<void>());
        return Combine(asyncResults);
    } else {
        return VoidFuture;
    }
}

void TAsyncFileChangelogIndex::Close()
{
    if (!IndexFile_) {
        return;
    }

    {
        NTracing::TNullTraceContextGuard nullTraceContextGuard;
        IndexFile_->FlushData() ;
        IndexFile_->Close();
    }
}

const std::vector<TChangelogIndexRecord>& TAsyncFileChangelogIndex::Records() const
{
    return Index_;
}

const TChangelogIndexRecord& TAsyncFileChangelogIndex::LastRecord() const
{
    YT_VERIFY(!Index_.empty());
    return Index_.back();
}

bool TAsyncFileChangelogIndex::IsEmpty() const
{
    return Index_.empty();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHydra
