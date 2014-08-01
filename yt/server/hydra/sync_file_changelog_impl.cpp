#include "stdafx.h"
#include "sync_file_changelog_impl.h"
#include "config.h"
#include "changelog.h"

#include <core/misc/fs.h>
#include <core/misc/string.h>
#include <core/misc/serialize.h>

namespace NYT {
namespace NHydra {

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = HydraLogger;

const ui64 TChangelogHeader::ExpectedSignature = 0x3330303044435459ull; // YTCD0003
const ui64 TChangelogIndexHeader::ExpectedSignature = 0x3330303049435459ull; // YTCI0003

////////////////////////////////////////////////////////////////////////////////

namespace {

//! Removes #destination if it exists. Then renames #destination into #source.
void ReplaceFile(const Stroka& source, const Stroka& destination)
{
    if (NFS::Exists(destination)) {
        NFS::Remove(destination);
    }

    NFS::Rename(source, destination);
}

template <class T>
void ValidateSignature(const T& header)
{
    LOG_FATAL_UNLESS(header.Signature == T::ExpectedSignature,
        "Invalid signature: expected %v, got %v",
        T::ExpectedSignature,
        header.Signature);
}

struct TRecordInfo
{
    TRecordInfo()
        : Id(-1)
        , TotalSize(-1)
    { }

    TRecordInfo(int id, int totalSize)
        : Id(id)
        , TotalSize(totalSize)
    { }

    int Id;
    int TotalSize;

};

//! Tries to read one record from the file.
//! Returns Null if failed.
template <class TInput>
TNullable<TRecordInfo> ReadRecord(TInput& input)
{
    int readSize = 0;
    TChangelogRecordHeader header;
    readSize += ReadPodPadded(input, header);
    if (!input.Success() || header.DataSize <= 0) {
        return Null;
    }

    struct TSyncChangelogRecordTag { };
    auto data = TSharedRef::Allocate<TSyncChangelogRecordTag>(header.DataSize, false);
    readSize += ReadPadded(input, data);
    if (!input.Success()) {
        return Null;
    }

    auto checksum = GetChecksum(data);
    LOG_FATAL_UNLESS(header.Checksum == checksum,
        "Incorrect checksum of record %v", header.RecordId);
    return TRecordInfo(header.RecordId, readSize);
}

// Computes the length of the maximal valid prefix of index records sequence.
size_t ComputeValidIndexPrefix(
    const std::vector<TChangelogIndexRecord>& index,
    const TChangelogHeader& header,
    TBufferedFile* file)
{
    // Validate index records.
    size_t result = 0;
    for (int i = 0; i < index.size(); ++i) {
        const auto& record = index[i];
        bool correct;
        if (i == 0) {
            correct =
                record.FilePosition == header.HeaderSize &&
                record.RecordId == 0;
        } else {
            const auto& prevRecord = index[i - 1];
            correct =
                record.FilePosition > prevRecord.FilePosition &&
                record.RecordId > prevRecord.RecordId;
        }
        if (!correct) {
            break;
        }
        ++result;
    }

    // Truncate invalid records.
    i64 fileLength = file->GetLength();
    while (result > 0 && index[result - 1].FilePosition > fileLength) {
        --result;
    }

    if (result == 0) {
        return 0;
    }

    // Truncate the last index entry if the corresponding changelog record is corrupt.
    file->Seek(index[result - 1].FilePosition, sSet);
    TCheckedReader<TBufferedFile> changelogReader(*file);
    if (!ReadRecord(changelogReader)) {
        --result;
    }

    return result;
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

////////////////////////////////////////////////////////////////////////////////

TSyncFileChangelog::TImpl::TImpl(
    const Stroka& path,
    TFileChangelogConfigPtr config)
    : FileName_(path)
    , IndexFileName_(path + "." + ChangelogIndexExtension)
    , Config_(config)
    , Open_(false)
    , RecordCount_(-1)
    , SealedRecordCount_(TChangelogHeader::UnsealedRecordCount)
    , CurrentBlockSize_(-1)
    , CurrentFilePosition_(-1)
    , LastFlushed_(TInstant::Now())
    , Logger(HydraLogger)
{
    Logger.AddTag("Path: %v", path);
}

TFileChangelogConfigPtr TSyncFileChangelog::TImpl::GetConfig() const
{
    return Config_;
}

const Stroka& TSyncFileChangelog::TImpl::GetFileName() const
{
    return FileName_;
}

void TSyncFileChangelog::TImpl::CreateIndexFile()
{
    auto tempFileName = IndexFileName_ + NFS::TempFileSuffix;
    TFile tempFile(tempFileName, WrOnly|CreateAlways);

    TChangelogIndexHeader header(0);
    WritePod(tempFile, header);

    tempFile.Flush();
    tempFile.Close();

    ReplaceFile(tempFileName, IndexFileName_);

    IndexFile_ = std::make_unique<TFile>(IndexFileName_, RdWr);
    IndexFile_->Flock(LOCK_EX | LOCK_NB);
    IndexFile_->Seek(0, sEnd);
}

void TSyncFileChangelog::TImpl::Create(const TSharedRef& meta)
{
    YCHECK(!Open_);

    LOG_DEBUG("Creating changelog");

    Meta_ = meta;
    RecordCount_ = 0;
    Open_ = true;

    {
        TGuard<TMutex> guard(Mutex_);

        // Data file.
        i64 currentFilePosition;
        {
            auto tempFileName = FileName_ + NFS::TempFileSuffix;
            TFile tempFile(tempFileName, WrOnly|CreateAlways);

            TChangelogHeader header(
                Meta_.Size(),
                TChangelogHeader::UnsealedRecordCount);
            WritePod(tempFile, header);

            WritePadded(tempFile, Meta_);

            currentFilePosition = tempFile.GetPosition();
            YCHECK(currentFilePosition == header.HeaderSize);

            tempFile.Flush();
            tempFile.Close();

            ReplaceFile(tempFileName, FileName_);

            DataFile_ = std::make_unique<TBufferedFile>(FileName_, RdWr);
            DataFile_->Flock(LOCK_EX | LOCK_NB);
            DataFile_->Seek(0, sEnd);
        }

        // Index file.
        CreateIndexFile();

        CurrentFilePosition_ = currentFilePosition;
        CurrentBlockSize_ = 0;
    }

    LOG_DEBUG("Changelog created");
}

void TSyncFileChangelog::TImpl::Open()
{
    YCHECK(!Open_);

    LOG_DEBUG("Opening changelog");

    {
        TGuard<TMutex> guard(Mutex_);

        DataFile_.reset(new TBufferedFile(FileName_, RdWr|Seq));
        DataFile_->Flock(LOCK_EX | LOCK_NB);

        // Read and check changelog header.
        TChangelogHeader header;
        ReadPod(*DataFile_, header);
        ValidateSignature(header);

        // Read meta.
        Meta_ = TSharedRef::Allocate(header.MetaSize);
        ReadPadded(*DataFile_, Meta_);

        Open_ = true;
        SealedRecordCount_ = header.SealedRecordCount;

        ReadIndex(header);
        ReadChangelogUntilEnd(header);

        if (IsSealed() && SealedRecordCount_ != RecordCount_) {
            THROW_ERROR_EXCEPTION(
                "Incorrect changelog: number or records (%v) less than sealed number (%v)",
                RecordCount_,
                SealedRecordCount_);
        }
    }


    LOG_DEBUG("Changelog opened (RecordCount: %v, Sealed: %v)",
        RecordCount_,
        ~FormatBool(IsSealed()));
}

void TSyncFileChangelog::TImpl::Close()
{
    if (!Open_)
        return;

    {
        TGuard<TMutex> guard(Mutex_);
        DataFile_->Close();
        IndexFile_->Close();
    }

    LOG_DEBUG("Changelog closed");

    Open_ = false;
}

void TSyncFileChangelog::TImpl::Append(
    int firstRecordId,
    const std::vector<TSharedRef>& records)
{
    YCHECK(Open_);
    YCHECK(!IsSealed());
    YCHECK(firstRecordId == RecordCount_);

    LOG_DEBUG("Appending %v records to changelog",
        records.size());

    {
        TGuard<TMutex> guard(Mutex_);
        for (const auto& record : records) {
            DoAppend(record);
        }
    }
}

void TSyncFileChangelog::TImpl::DoAppend(const TRef& record)
{
    YCHECK(record.Size() != 0);

    int recordId = RecordCount_;
    TChangelogRecordHeader header(recordId, record.Size(), GetChecksum(record));

    int readSize = 0;
    readSize += AppendPodPadded(*DataFile_, header);
    readSize += AppendPadded(*DataFile_, record);

    ProcessRecord(recordId, readSize);
}

std::vector<TSharedRef> TSyncFileChangelog::TImpl::Read(
    int firstRecordId,
    int maxRecords,
    i64 maxBytes)
{
    // Sanity check.
    YCHECK(firstRecordId >= 0);
    YCHECK(maxRecords >= 0);
    YCHECK(Open_);

    LOG_DEBUG("Reading up to %v records and up to %v" " bytes from record %v",
        maxRecords,
        maxBytes,
        firstRecordId);

    std::vector<TSharedRef> records;

    // Prevent search in empty index.
    if (Index_.empty()) {
        return std::move(records);
    }

    maxRecords = std::min(maxRecords, RecordCount_ - firstRecordId);
    int lastRecordId = firstRecordId + maxRecords;

    // Read envelope piece of changelog.
    auto envelope = ReadEnvelope(firstRecordId, lastRecordId, std::min(Index_.back().FilePosition, maxBytes));

    // Read records from envelope data and save them to the records.
    i64 readSize = 0;
    TMemoryInput inputStream(envelope.Blob.Begin(), envelope.GetLength());
    for (int recordId = envelope.GetStartRecordId();
         recordId < envelope.GetEndRecordId();
         ++recordId)
    {
        // Read and check header.
        TChangelogRecordHeader header;
        ReadPodPadded(inputStream, header);
        YCHECK(header.RecordId == recordId);

        // Save and pad data.
        auto data = envelope.Blob.Slice(TRef(const_cast<char*>(inputStream.Buf()), header.DataSize));
        inputStream.Skip(AlignUp(header.DataSize));

        // Add data to the records.
        if (recordId >= firstRecordId && recordId < lastRecordId) {
            records.push_back(data);
            readSize += data.Size();
        }
    }

    return records;
}

TSharedRef TSyncFileChangelog::TImpl::GetMeta() const
{
    return Meta_;
}

int TSyncFileChangelog::TImpl::GetRecordCount() const
{
    return RecordCount_;
}

i64 TSyncFileChangelog::TImpl::GetDataSize() const
{
    return CurrentFilePosition_;
}

bool TSyncFileChangelog::TImpl::IsSealed() const
{
    return SealedRecordCount_ != TChangelogHeader::UnsealedRecordCount;
}

void TSyncFileChangelog::TImpl::Seal(int recordCount)
{
    YCHECK(Open_);
    YCHECK(!IsSealed());
    YCHECK(recordCount >= 0);

    LOG_DEBUG("Sealing changelog with %v records", recordCount);
    
    auto oldRecordCount = RecordCount_;

    SealedRecordCount_ = RecordCount_ = recordCount;

    {
        TGuard<TMutex> guard(Mutex_);
        UpdateLogHeader();
    }

    if (oldRecordCount != recordCount) {
        auto envelope = ReadEnvelope(recordCount, recordCount);
        if (recordCount == 0) {
            Index_.clear();
        } else {
            auto cutBound =
                envelope.LowerBound.RecordId == recordCount
                ? envelope.LowerBound
                : envelope.UpperBound;
            auto indexPosition =
                std::lower_bound(Index_.begin(), Index_.end(), cutBound, CompareRecordIds) -
                Index_.begin();
            Index_.resize(indexPosition);
        }

        i64 readSize = 0;
        TMemoryInput inputStream(envelope.Blob.Begin(), envelope.GetLength());
        for (int index = envelope.GetStartRecordId(); index < recordCount; ++index) {
            TChangelogRecordHeader header;
            readSize += ReadPodPadded(inputStream, header);
            auto alignedSize = AlignUp(header.DataSize);
            inputStream.Skip(alignedSize);
            readSize += alignedSize;
        }

        CurrentBlockSize_ = readSize;
        CurrentFilePosition_ = envelope.GetStartPosition() + readSize;

        {
            TGuard<TMutex> guard(Mutex_);
            
            IndexFile_->Resize(sizeof(TChangelogIndexHeader) + Index_.size() * sizeof(TChangelogIndexRecord));
            UpdateIndexHeader();

            DataFile_->Resize(CurrentFilePosition_);
            DataFile_->Flush();
            DataFile_->Seek(0, sEnd);
        }
    }

    LOG_DEBUG("Changelog sealed");
}

void TSyncFileChangelog::TImpl::Unseal()
{
    YCHECK(Open_);
    YCHECK(IsSealed());

    LOG_DEBUG("Unsealing changelog");

    SealedRecordCount_ = TChangelogHeader::UnsealedRecordCount;

    {
        TGuard<TMutex> guard(Mutex_);
        UpdateLogHeader();
    }
    
    LOG_DEBUG("Changelog unsealed");
}

void TSyncFileChangelog::TImpl::Flush()
{
    LOG_DEBUG("Flushing changelog");

    {
        TGuard<TMutex> guard(Mutex_);
        DataFile_->Flush();
        IndexFile_->Flush();
        LastFlushed_ = TInstant::Now();
    }

    LOG_DEBUG("Changelog flushed");
}

TInstant TSyncFileChangelog::TImpl::GetLastFlushed()
{
    return LastFlushed_;
}

void TSyncFileChangelog::TImpl::ProcessRecord(int recordId, int readSize)
{
    if (CurrentBlockSize_ >= Config_->IndexBlockSize || RecordCount_ == 0) {
        // Add index record in two cases:
        // 1) processing first record;
        // 2) size of records since previous index record is more than IndexBlockSize.
        YCHECK(Index_.empty() || Index_.back().RecordId != recordId);

        CurrentBlockSize_ = 0;
        Index_.push_back(TChangelogIndexRecord(recordId, CurrentFilePosition_));
        {
            TGuard<TMutex> guard(Mutex_);
            WritePod(*IndexFile_, Index_.back());
            UpdateIndexHeader();
        }
        LOG_DEBUG("Changelog index record added (RecordId: %v, Offset: %v" ")",
            recordId,
            CurrentFilePosition_);
    }
    // Record appended successfully.
    CurrentBlockSize_ += readSize;
    CurrentFilePosition_ += readSize;
    RecordCount_ += 1;
}

void TSyncFileChangelog::TImpl::ReadIndex(const TChangelogHeader& header)
{
    // Create index if it is missing.
    if (!NFS::Exists(IndexFileName_)) {
        CreateIndexFile();
    }

    // Read the existing index.
    {
        TMappedFileInput indexStream(IndexFileName_);

        // Read and check index header.
        TChangelogIndexHeader indexHeader;
        ReadPod(indexStream, indexHeader);
        ValidateSignature(indexHeader);
        YCHECK(indexHeader.IndexRecordCount >= 0);

        // Read index records.
        for (int i = 0; i < indexHeader.IndexRecordCount; ++i) {
            TChangelogIndexRecord indexRecord;
            ReadPod(indexStream, indexRecord);
            if (IsSealed() && indexRecord.RecordId >= SealedRecordCount_) {
                break;
            }
            Index_.push_back(indexRecord);
        }
    }
    // Compute the maximum correct prefix and truncate the index.
    {
        auto correctPrefixSize = ComputeValidIndexPrefix(Index_, header, &*DataFile_);
        LOG_ERROR_IF(correctPrefixSize < Index_.size(), "Changelog index contains invalid records, truncated");
        Index_.resize(correctPrefixSize);

        IndexFile_.reset(new TFile(IndexFileName_, RdWr|Seq|CloseOnExec|OpenAlways));
        IndexFile_->Flock(LOCK_EX | LOCK_NB);
        IndexFile_->Resize(sizeof(TChangelogIndexHeader) + Index_.size() * sizeof(TChangelogIndexRecord));
        IndexFile_->Seek(0, sEnd);
    }
}

void TSyncFileChangelog::TImpl::UpdateLogHeader()
{
    i64 oldPosition = DataFile_->GetPosition();
    DataFile_->Seek(0, sSet);
    TChangelogHeader header(
        Meta_.Size(),
        SealedRecordCount_);
    WritePod(*DataFile_, header);
    DataFile_->Seek(oldPosition, sSet);
}

void TSyncFileChangelog::TImpl::UpdateIndexHeader()
{
    i64 oldPosition = IndexFile_->GetPosition();
    IndexFile_->Seek(0, sSet);
    TChangelogIndexHeader header(Index_.size());
    WritePod(*IndexFile_, header);
    IndexFile_->Seek(oldPosition, sSet);
}

void TSyncFileChangelog::TImpl::ReadChangelogUntilEnd(const TChangelogHeader& header)
{
    // Extract changelog properties from index.
    i64 fileLength = DataFile_->GetLength();
    CurrentBlockSize_ = 0;
    if (Index_.empty()) {
        RecordCount_ = 0;
        CurrentFilePosition_ = header.HeaderSize;
    } else {
        // Record count would be set below.
        CurrentFilePosition_ = Index_.back().FilePosition;
    }

    // Seek to proper position in file, initialize checkable reader.
    DataFile_->Seek(CurrentFilePosition_, sSet);
    TCheckedReader<TBufferedFile> dataReader(*DataFile_);

    TNullable<TRecordInfo> recordInfo;
    if (!Index_.empty()) {
        // Skip first record.
        recordInfo = ReadRecord(dataReader);
        // It should be correct because we have already check index.
        YASSERT(recordInfo);
        RecordCount_ = Index_.back().RecordId + 1;
        CurrentFilePosition_ += recordInfo->TotalSize;
    }

    while (CurrentFilePosition_ < fileLength) {
        // Record size also counts size of record header.
        recordInfo = ReadRecord(dataReader);
        if (!recordInfo || recordInfo->Id != RecordCount_ || RecordCount_ == SealedRecordCount_) {
            // Broken changelog case.
            if (!recordInfo || recordInfo->Id != RecordCount_) {
                LOG_ERROR("Broken record found, changelog trimmed (RecordId: %v, Offset: %v" ")",
                    RecordCount_,
                    CurrentFilePosition_);
            } else {
                LOG_ERROR("Excess records found, sealed changelog trimmed (RecordId: %v, Offset: %v" ")",
                    RecordCount_,
                    CurrentFilePosition_);
            }
            DataFile_->Resize(CurrentFilePosition_);
            DataFile_->Seek(0, sEnd);
            break;
        }
        ProcessRecord(recordInfo->Id, recordInfo->TotalSize);
    }
}

TSyncFileChangelog::TImpl::TEnvelopeData TSyncFileChangelog::TImpl::ReadEnvelope(
    int firstRecordId,
    int lastRecordId,
    i64 maxBytes)
{
    YCHECK(!Index_.empty());

    // Index can be changed during Append.
    TGuard<TMutex> guard(Mutex_);

    TEnvelopeData result;
    result.LowerBound = *LastNotGreater(Index_, TChangelogIndexRecord(firstRecordId, -1), CompareRecordIds);
    
    auto it = FirstGreater(Index_, TChangelogIndexRecord(lastRecordId, -1), CompareRecordIds);
    if (maxBytes != -1) {
        i64 maxFilePosition = result.LowerBound.FilePosition + maxBytes;
        it = std::min(it, FirstGreater(Index_, TChangelogIndexRecord(-1, maxFilePosition), CompareFilePositions));
    }
    result.UpperBound =
        it != Index_.end() ?
        *it :
        TChangelogIndexRecord(RecordCount_, CurrentFilePosition_);

    struct TSyncChangelogEnvelopeTag { };
    result.Blob = TSharedRef::Allocate<TSyncChangelogEnvelopeTag>(result.GetLength(), false);

    size_t bytesRead = DataFile_->Pread(
        result.Blob.Begin(),
        result.GetLength(),
        result.GetStartPosition());
    YCHECK(bytesRead == result.GetLength());

    return result;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NHydra
} // namespace NYT
