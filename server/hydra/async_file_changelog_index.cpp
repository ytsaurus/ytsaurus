#include "async_file_changelog_index.h"
#include "format.h"
#include "private.h"

#include <yt/core/misc/fs.h>

namespace NYT {
namespace NHydra {

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = HydraLogger;

////////////////////////////////////////////////////////////////////////////////

namespace {

template <class T>
void ValidateSignature(const T& header)
{
    LOG_FATAL_UNLESS(header.Signature == T::ExpectedSignature || header.Signature == T::ExpectedSignatureOld,
        "Invalid signature: expected %" PRIx64 ", got %" PRIx64,
        T::ExpectedSignature,
        header.Signature);
}

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
    YCHECK(res != vec.begin());
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
    YCHECK(res != vec.begin());
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

TAsyncFileChangelogIndex::TAsyncFileChangelogIndex(
    const NChunkClient::IIOEnginePtr& IOEngine,
    const TString& name,
    i64 indexBlockSize)
    : IOEngine_(IOEngine)
    , IndexFileName_(name)
    , IndexBlockSize_(indexBlockSize)
{ }

//! Creates an empty index file.
void TAsyncFileChangelogIndex::Create()
{
    auto tempFileName = IndexFileName_ + NFS::TempFileSuffix;
    TFile tempFile(tempFileName, WrOnly|CreateAlways);

    TChangelogIndexHeader header(0);
    WritePod(tempFile, header);

    tempFile.FlushData();
    tempFile.Close();

    NFS::Replace(tempFileName, IndexFileName_);

    IndexFile_ = std::make_unique<TFile>(IndexFileName_, RdWr);
    IndexFile_->Seek(0, sEnd);
}

void TAsyncFileChangelogIndex::Read(const TNullable<i32>& truncatedRecordCount)
{
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
        static_assert(sizeof(indexHeader) >= 12, "Sizeof index header must be >= 12");
        indexStream.Load(&indexHeader, 12);
        ValidateSignature(indexHeader);
        YCHECK(indexHeader.IndexRecordCount >= 0);
        if (indexHeader.Signature == indexHeader.ExpectedSignature) {
            indexStream.Skip(sizeof(indexHeader.Padding));
        }

        // COMPAT(aozeritsky): old format
        if (indexHeader.Signature == indexHeader.ExpectedSignatureOld) {
            OldFormat_ = true;
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

void TAsyncFileChangelogIndex::TruncateInvalidRecords(i64 correctPrefixSize)
{
    LOG_WARNING_IF(correctPrefixSize < Index_.size(), "Changelog index contains invalid records, truncated");
    YCHECK(correctPrefixSize <= Index_.size());
    Index_.resize(correctPrefixSize);

    // COMPAT(aozeritsky): old format
    if (OldFormat_) {
        return;
    }

    IndexFile_.reset(new TFile(IndexFileName_, RdWr | Seq | OpenAlways | CloseOnExec));
    IndexFile_->Resize(sizeof(TChangelogIndexHeader) + Index_.size() * sizeof(TChangelogIndexRecord));
    IndexFile_->Seek(0, sEnd);
}

void TAsyncFileChangelogIndex::Search(
    TChangelogIndexRecord* lowerBound,
    TChangelogIndexRecord* upperBound,
    int firstRecordId,
    int lastRecordId,
    i64 maxBytes) const
{
    YCHECK(!Index_.empty());

    *lowerBound = *LastNotGreater(Index_, TChangelogIndexRecord(firstRecordId, -1), CompareRecordIds);

    auto it = FirstGreater(Index_, TChangelogIndexRecord(lastRecordId, -1), CompareRecordIds);
    if (maxBytes != -1) {
        i64 maxFilePosition = lowerBound->FilePosition + maxBytes;
        it = std::min(it, FirstGreater(Index_, TChangelogIndexRecord(-1, maxFilePosition), CompareFilePositions));
    }

    if (it != Index_.end()) {
        *upperBound = *it;
    }
}

//! Rewrites index header.
void TAsyncFileChangelogIndex::UpdateIndexHeader()
{
    NFS::ExpectIOErrors([&] () {
        IndexFile_->FlushData();
        i64 oldPosition = IndexFile_->GetPosition();
        IndexFile_->Seek(0, sSet);
        TChangelogIndexHeader header(Index_.size());
        WritePod(*IndexFile_, header);
        IndexFile_->Seek(oldPosition, sSet);
    });
}

//! Processes records that are being or written.
/*!
 *  Checks record id for correctness, updates index, record count,
 *  current block size and current file position.
 */
void TAsyncFileChangelogIndex::ProcessRecord(int recordId, i64 currentFilePosition, int totalSize)
{
    YCHECK(CurrentBlockSize_ >= 0);

    // We add a new index record
    // 1) for the very first data record; or
    // 2) if the size of data records added since last index record exceeds IndexBlockSize.
    if (recordId == 0 || CurrentBlockSize_ >= IndexBlockSize_) {
        YCHECK(Index_.empty() || Index_.back().RecordId < recordId);

        CurrentBlockSize_ = 0;
        Index_.emplace_back(recordId, currentFilePosition);

        // COMPAT(aozeritsky): old format
        if (!OldFormat_) {
            NFS::ExpectIOErrors([&]() {
                WritePod(*IndexFile_, Index_.back());
            });

            UpdateIndexHeader();
        }

        LOG_DEBUG("Changelog index record added (RecordId: %v, Offset: %v)",
            recordId,
            currentFilePosition);
    }
    // Record appended successfully.
    CurrentBlockSize_ += totalSize;
}

void TAsyncFileChangelogIndex::Append(int firstRecordId, i64 filePosition, const std::vector<int>& appendSizes)
{
    std::vector<TFuture<void>> futures;
    int totalRecords = appendSizes.size();
    for (int index = 0; index < totalRecords; ++index) {
        auto recordSize = appendSizes[index];
        ProcessRecord(firstRecordId + index, filePosition, recordSize);
        filePosition += recordSize;
    }
}

void TAsyncFileChangelogIndex::Append(int firstRecordId, i64 filePosition, int recordSize)
{
    return Append(firstRecordId, filePosition, std::vector<int>({recordSize}));
}

void TAsyncFileChangelogIndex::FlushData()
{
    YCHECK(!OldFormat_);

    IndexFile_->FlushData();
}

void TAsyncFileChangelogIndex::Close()
{
    NFS::ExpectIOErrors([&] () {
        IndexFile_->FlushData() ;
        IndexFile_->Close();
    });
}

const std::vector<TChangelogIndexRecord>& TAsyncFileChangelogIndex::Records() const
{
    return Index_;
}

const TChangelogIndexRecord& TAsyncFileChangelogIndex::LastRecord() const
{
    YCHECK(!Index_.empty());
    return Index_.back();
}

bool TAsyncFileChangelogIndex::IsEmpty() const
{
    return Index_.empty();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NHydra
} // namespace NYT
