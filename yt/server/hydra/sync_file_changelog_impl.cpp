#include "stdafx.h"
#include "sync_file_changelog_impl.h"
#include "config.h"
#include "changelog.h"

#include <core/misc/fs.h>
#include <core/misc/string.h>
#include <core/misc/serialize.h>

#include <util/folder/dirut.h>

namespace NYT {
namespace NHydra {

////////////////////////////////////////////////////////////////////////////////

static auto& Logger = HydraLogger;

////////////////////////////////////////////////////////////////////////////////

namespace {

//! Removes #destination if it exists. Then renames #destination into #source.
void ReplaceFile(const Stroka& source, const Stroka& destination)
{
    if (isexist(~destination)) {
        if (!NFS::Remove(~destination)) {
            LOG_FATAL("Failed to remove %s",
                ~destination.Quote());
        }
    }

    if (!NFS::Rename(~source, ~destination)) {
        LOG_FATAL("Failed to rename %s into %s",
            ~source.Quote(),
            ~destination.Quote());
    }
}

template <class T>
void ValidateSignature(const T& header)
{
    LOG_FATAL_UNLESS(header.Signature == T::ExpectedSignature,
        "Invalid signature: expected %" PRIx64 ", got %" PRIx64,
        T::ExpectedSignature,
        header.Signature);
}

template <class TFile, class THeader>
void AtomicWriteHeader(
    const Stroka& fileName,
    const THeader& header,
    std::unique_ptr<TFile>* fileHolder)
{
    Stroka tempFileName(fileName + NFS::TempFileSuffix);
    TFile tempFile(tempFileName, WrOnly|CreateAlways);
    WritePod(tempFile, header);
    tempFile.Close();

    ReplaceFile(tempFileName, fileName);
    fileHolder->reset(new TFile(fileName, RdWr));
    (*fileHolder)->Seek(0, sEnd);
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
        "Incorrect checksum of record %d", header.RecordId);
    return TRecordInfo(header.RecordId, readSize);
}

// Calculates maximal correct prefix of index.
size_t GetMaxCorrectIndexPrefix(
    const std::vector<TChangelogIndexRecord>& index,
    TBufferedFile* changelogFile)
{
    // Check adequacy of index.
    size_t correctPrefixLength = 0;
    for (int i = 0; i < index.size(); ++i) {
        bool correct;
        if (i == 0) {
            correct = index[i].FilePosition == sizeof(TChangelogHeader) && index[i].RecordId == 0;
        } else {
            correct =
                index[i].FilePosition > index[i - 1].FilePosition &&
                index[i].RecordId > index[i - 1].RecordId;
        }
        if (!correct) {
            break;
        }
        correctPrefixLength += 1;
    }

    // Truncate excess index records
    i64 fileLength = changelogFile->GetLength();
    while (correctPrefixLength > 0 && index[correctPrefixLength - 1].FilePosition > fileLength) {
        correctPrefixLength -= 1;
    }

    if (correctPrefixLength == 0) {
        return 0;
    }

    // Truncate last index record if changelog file is corrupted
    changelogFile->Seek(index[correctPrefixLength - 1].FilePosition, sSet);
    TCheckedReader<TBufferedFile> changelogReader(*changelogFile);
    if (!ReadRecord(changelogReader)) {
        correctPrefixLength -= 1;
    }

    return correctPrefixLength;
}

// This method uses forward iterator instead of reverse because they work faster.
// Asserts if last not greater element is absent.
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

template <class T>
typename std::vector<T>::const_iterator FirstGreater(
    const std::vector<T>& vec,
    const T& value)
{
    return std::upper_bound(vec.begin(), vec.end(), value);
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

TSyncFileChangelog::TImpl::TImpl(
    const Stroka& path,
    int id,
    TFileChangelogConfigPtr config)
    : Id_(id)
    , FileName_(path)
    , IndexFileName_(path + IndexSuffix)
    , Config_(config)
    , IsOpen_(false)
    , IsSealed_(false)
    , RecordCount_(-1)
    , CurrentBlockSize_(-1)
    , CurrentFilePosition_(-1)
    , LastFlushed_(TInstant::Now())
    , PrevRecordCount_(-1)
    , Logger(HydraLogger)
{
    YCHECK(Id_ >= 0);

    Logger.AddTag(Sprintf("Path: %s", ~path));
}

TFileChangelogConfigPtr TSyncFileChangelog::TImpl::GetConfig()
{
    return Config_;
}

void TSyncFileChangelog::TImpl::Create(const TChangelogCreateParams& params)
{
    YCHECK(!IsOpen_);

    LOG_DEBUG("Creating changelog");

    PrevRecordCount_ = params.PrevRecordCount;
    RecordCount_ = 0;
    IsOpen_ = true;

    {
        TGuard<TMutex> guard(Mutex_);

        {
            TChangelogHeader header(Id_, PrevRecordCount_, TChangelogHeader::NotSealedRecordCount);
            AtomicWriteHeader(FileName_, header, &LogFile_);
        }

        {
            TChangelogIndexHeader header(Id_, 0);
            AtomicWriteHeader(IndexFileName_, header, &IndexFile_);
        }

        CurrentFilePosition_ = sizeof(TChangelogHeader);
        CurrentBlockSize_ = 0;
    }

    LOG_DEBUG("Changelog created");
}

void TSyncFileChangelog::TImpl::Open()
{
    YCHECK(!IsOpen_);

    LOG_DEBUG("Opening changelog");

    {
        TGuard<TMutex> guard(Mutex_);

        LogFile_.reset(new TBufferedFile(FileName_, RdWr|Seq));

        // Read and check header of changelog.
        TChangelogHeader header;
        ReadPod(*LogFile_, header);
        ValidateSignature(header);
        YCHECK(header.ChangelogId == Id_);

        PrevRecordCount_ = header.PrevRecordCount;
        IsOpen_ = true;
        IsSealed_ = header.SealedRecordCount != TChangelogHeader::NotSealedRecordCount;

        ReadIndex();

        // TODO(babenko): truncate if sealed
        ReadChangelogUntilEnd();
    }


    LOG_DEBUG("Changelog opened (RecordCount: %d, Sealed: %s)",
        RecordCount_,
        ~FormatBool(IsSealed_));
}

void TSyncFileChangelog::TImpl::Close()
{
    if (!IsOpen_)
        return;

    {
        TGuard<TMutex> guard(Mutex_);
        LogFile_->Close();
        IndexFile_->Close();
    }

    LOG_DEBUG("Changelog closed");

    IsOpen_ = false;
}

void TSyncFileChangelog::TImpl::Append(
    int firstRecordId,
    const std::vector<TSharedRef>& records)
{
    YCHECK(IsOpen_);
    YCHECK(!IsSealed_);
    YCHECK(firstRecordId == RecordCount_);

    LOG_DEBUG("Appending %" PRISZT " records to changelog",
        records.size());

    {
        TGuard<TMutex> guard(Mutex_);
        FOREACH (const auto& record, records) {
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
    readSize += AppendPodPadded(*LogFile_, header);
    readSize += AppendPadded(*LogFile_, record);

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
    YCHECK(IsOpen_);

    LOG_DEBUG("Reading up to %d records and up to %" PRId64 " bytes from record %d",
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
    // TODO(babenko): use maxBytes limit
    auto envelope = ReadEnvelope(firstRecordId, lastRecordId);

    // Read records from envelope data and save them to the records.
    // TODO(babenko): this is suboptimal since for small maxSize values we actually read way more than needed.
    i64 readSize = 0;
    TMemoryInput inputStream(envelope.Blob.Begin(), envelope.GetLength());
    for (int recordId = envelope.GetStartRecordId();
         recordId < envelope.GetEndRecordId() && readSize <= maxBytes;
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

    return std::move(records);
}

int TSyncFileChangelog::TImpl::GetId() const
{
    return Id_;
}

int TSyncFileChangelog::TImpl::GetPrevRecordCount() const
{
    return PrevRecordCount_;
}

int TSyncFileChangelog::TImpl::GetRecordCount() const
{
    return RecordCount_;
}

bool TSyncFileChangelog::TImpl::IsSealed() const
{
    return IsSealed_;
}

void TSyncFileChangelog::TImpl::Seal(int recordCount)
{
    YCHECK(IsOpen_);
    YCHECK(!IsSealed_);
    YCHECK(recordCount >= 0);

    LOG_DEBUG("Sealing changelog with %d records", recordCount);

    IsSealed_ = true;
    RecordCount_ = recordCount;

    {
        TGuard<TMutex> guard(Mutex_);
        UpdateLogHeader();
    }

    // TODO(babenko): ignat@ should definitely fix this :)
    if (recordCount == 0)
        return;

    auto envelope = ReadEnvelope(recordCount, recordCount);
    if (recordCount == 0) {
        Index_.clear();
    } else {
        auto cutBound =
            envelope.LowerBound.RecordId == recordCount
            ? envelope.LowerBound
            : envelope.UpperBound;
        auto indexPosition =
            std::lower_bound(Index_.begin(), Index_.end(), cutBound) -
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

        LogFile_->Resize(CurrentFilePosition_);
        LogFile_->Flush();
        LogFile_->Seek(0, sEnd);
    }

    LOG_DEBUG("Changelog sealed");
}

void TSyncFileChangelog::TImpl::Unseal()
{
    YCHECK(IsOpen_);
    YCHECK(IsSealed_);

    LOG_DEBUG("Unsealing changelog");

    IsSealed_ = false;

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
        LogFile_->Flush();
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
        LOG_DEBUG("Changelog index record added (RecordId: %d, Offset: %" PRId64 ")",
            recordId,
            CurrentFilePosition_);
    }
    // Record appended successfully.
    CurrentBlockSize_ += readSize;
    CurrentFilePosition_ += readSize;
    RecordCount_ += 1;
}

void TSyncFileChangelog::TImpl::ReadIndex()
{
    // Read an existing index.
    {
        TMappedFileInput indexStream(IndexFileName_);

        // Read and check index header.
        TChangelogIndexHeader indexHeader;
        ReadPod(indexStream, indexHeader);
        ValidateSignature(indexHeader);
        YCHECK(indexHeader.IndexSize >= 0);

        // Read index records.
        for (int i = 0; i < indexHeader.IndexSize; ++i) {
            TChangelogIndexRecord indexRecord;
            ReadPod(indexStream, indexRecord);
            Index_.push_back(indexRecord);
        }
    }
    // Compute the maximum correct prefix and truncate the index.
    {
        auto correctPrefixSize = GetMaxCorrectIndexPrefix(Index_, &(*LogFile_));
        LOG_ERROR_IF(correctPrefixSize < Index_.size(), "Changelog index contains incorrect records");
        Index_.resize(correctPrefixSize);

        IndexFile_.reset(new TFile(IndexFileName_, RdWr|Seq|CloseOnExec));
        IndexFile_->Resize(sizeof(TChangelogIndexHeader) + Index_.size() * sizeof(TChangelogIndexRecord));
        IndexFile_->Seek(0, sEnd);
    }
}

void TSyncFileChangelog::TImpl::UpdateLogHeader()
{
    i64 oldPosition = LogFile_->GetPosition();
    LogFile_->Seek(0, sSet);
    TChangelogHeader header(
        Id_,
        PrevRecordCount_,
        IsSealed_ ? RecordCount_ : TChangelogHeader::NotSealedRecordCount);
    WritePod(*LogFile_, header);
    LogFile_->Seek(oldPosition, sSet);
}

void TSyncFileChangelog::TImpl::UpdateIndexHeader()
{
    i64 oldPosition = IndexFile_->GetPosition();
    IndexFile_->Seek(0, sSet);
    TChangelogIndexHeader header(Id_, Index_.size());
    WritePod(*IndexFile_, header);
    IndexFile_->Seek(oldPosition, sSet);
}

void TSyncFileChangelog::TImpl::ReadChangelogUntilEnd()
{
    // Extract changelog properties from index.
    i64 fileLength = LogFile_->GetLength();
    CurrentBlockSize_ = 0;
    if (Index_.empty()) {
        RecordCount_ = 0;
        CurrentFilePosition_ = sizeof(TChangelogHeader);
    } else {
        // Record count would be set below.
        CurrentFilePosition_ = Index_.back().FilePosition;
    }

    // Seek to proper position in file, initialize checkable reader.
    LogFile_->Seek(CurrentFilePosition_, sSet);
    TCheckedReader<TBufferedFile> logFileReader(*LogFile_);

    TNullable<TRecordInfo> recordInfo;
    if (!Index_.empty()) {
        // Skip first record.
        recordInfo = ReadRecord(logFileReader);
        // It should be correct because we have already check index.
        YASSERT(recordInfo);
        RecordCount_ = Index_.back().RecordId + 1;
        CurrentFilePosition_ += recordInfo->TotalSize;
    }

    while (CurrentFilePosition_ < fileLength) {
        // Record size also counts size of record header.
        recordInfo = ReadRecord(logFileReader);
        if (!recordInfo || recordInfo->Id != RecordCount_) {
            // Broken changelog case.
            LOG_ERROR("Broken record found, changelog trimmed (RecordId: %d, Offset: %" PRId64 ")",
                RecordCount_,
                CurrentFilePosition_);
            LogFile_->Resize(CurrentFilePosition_);
            LogFile_->Seek(0, sEnd);
            break;
        }
        ProcessRecord(recordInfo->Id, recordInfo->TotalSize);
    }
}

TSyncFileChangelog::TImpl::TEnvelopeData TSyncFileChangelog::TImpl::ReadEnvelope(
    int firstRecordId,
    int lastRecordId)
{
    // Index can be changed during Append.
    TGuard<TMutex> guard(Mutex_);

    TEnvelopeData result;
    result.LowerBound = *LastNotGreater(Index_, TChangelogIndexRecord(firstRecordId, -1));
    
    auto it = FirstGreater(Index_, TChangelogIndexRecord(lastRecordId, -1));
    result.UpperBound =
        it != Index_.end() ?
        *it :
        TChangelogIndexRecord(RecordCount_, CurrentFilePosition_);

    struct TSyncChangelogEnvelopeTag { };
    result.Blob = TSharedRef::Allocate<TSyncChangelogEnvelopeTag>(result.GetLength(), false);

    size_t bytesRead = LogFile_->Pread(
        result.Blob.Begin(),
        result.GetLength(),
        result.GetStartPosition());
    YCHECK(bytesRead == result.GetLength());

    return result;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NHydra
} // namespace NYT
