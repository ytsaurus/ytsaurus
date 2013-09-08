#include "change_log_impl.h"

#include <ytlib/misc/fs.h>
#include <ytlib/misc/nullable.h>
#include <ytlib/misc/serialize.h>
#include <ytlib/misc/string.h>

namespace NYT {
namespace NMetaState {

////////////////////////////////////////////////////////////////////////////////

static auto& Logger = MetaStateLogger;

static const char* const IndexSuffix = ".index";

////////////////////////////////////////////////////////////////////////////////

TChangeLog::TImpl::TImpl(
    const Stroka& fileName,
    int id,
    i64 indexBlockSize)
    : Id(id)
    , IndexBlockSize(indexBlockSize)
    , FileName(fileName)
    , IndexFileName(fileName + IndexSuffix)
    , State(EState::Uninitialized)
    , RecordCount(-1)
    , CurrentBlockSize(-1)
    , CurrentFilePosition(-1)
    , PrevRecordCount(-1)
    , Logger(MetaStateLogger)
{
    Logger.AddTag(Sprintf("ChangeLogId: %d", Id));
}

////////////////////////////////////////////////////////////////////////////////

void TChangeLog::TImpl::Append(const std::vector<TSharedRef>& records)
{
    YCHECK(State == EState::Open);

    LOG_DEBUG("Appending %" PRISZT " records to changelog", records.size());

    TGuard<TMutex> guard(Mutex);
    FOREACH (const auto& record, records) {
        Append(record);
    }
}

void TChangeLog::TImpl::Append(int firstRecordIndex, const std::vector<TSharedRef>& records)
{
    YCHECK(firstRecordIndex == RecordCount);
    Append(records);
}

void TChangeLog::TImpl::Append(const TSharedRef& recordData)
{
    int recordIndex = RecordCount;
    TRecordHeader header(recordIndex, recordData.Size(), GetChecksum(recordData));

    int readSize = 0;
    readSize += AppendPodPadded(*File, header);
    readSize += AppendPadded(*File, recordData);

    ProcessRecord(recordIndex, readSize);
}

////////////////////////////////////////////////////////////////////////////////

void TChangeLog::TImpl::Read(
    int firstRecordIndex,
    int recordCount,
    i64 maxSize,
    std::vector<TSharedRef>* records)
{
    // Sanity check.
    YCHECK(firstRecordIndex >= 0);
    YCHECK(recordCount >= 0);
    YCHECK(State != EState::Uninitialized);

    LOG_DEBUG("Reading records %d-%d (MaxSize: %" PRId64 ")",
        firstRecordIndex,
        firstRecordIndex + recordCount - 1,
        maxSize);

    // Prevent search in empty index.
    if (Index.empty()) {
        records->clear();
        return;
    }

    recordCount = std::min(recordCount, RecordCount - firstRecordIndex);
    int lastRecordIndex = firstRecordIndex + recordCount;

    // Read envelope piece of changelog.
    auto envelope = ReadEnvelope(firstRecordIndex, lastRecordIndex);

    // Read records from envelope data and save them to the records.
    // TODO(babenko): this is suboptimal since for small maxSize values we actually read way more than needed.
    i64 readSize = 0;
    TMemoryInput inputStream(envelope.Blob.Begin(), envelope.GetLength());
    for (int recordIndex = envelope.GetStartRecordIndex();
         recordIndex < envelope.GetEndRecordIndex() && readSize <= maxSize;
         ++recordIndex)
    {
        // Read and check header.
        TRecordHeader header;
        ReadPodPadded(inputStream, header);
        YCHECK(header.RecordIndex == recordIndex);

        // Save and pad data.
        auto data = envelope.Blob.Slice(TRef(const_cast<char*>(inputStream.Buf()), header.DataLength));
        inputStream.Skip(AlignUp(header.DataLength));

        // Add data to the records.
        if (recordIndex >= firstRecordIndex && recordIndex < lastRecordIndex) {
            records->push_back(data);
            readSize += data.Size();
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

namespace {

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

    CheckedMoveFile(tempFileName, fileName);
    fileHolder->reset(new TFile(fileName, RdWr));
    (*fileHolder)->Seek(0, sEnd);
}

} // anonymous namespace

////////////////////////////////////////////////////////////////////////////////

void TChangeLog::TImpl::Create(int prevRecordCount, const TEpochId& epoch)
{
    YCHECK(State == EState::Uninitialized);

    LOG_DEBUG("Creating changelog");

    PrevRecordCount = prevRecordCount;
    Epoch = epoch;
    RecordCount = 0;
    State = EState::Open;

    {
        TGuard<TMutex> guard(Mutex);

        AtomicWriteHeader(FileName, TLogHeader(Id, epoch, prevRecordCount, /*finalized*/ false), &File);
        AtomicWriteHeader(IndexFileName, TLogIndexHeader(Id, 0), &IndexFile);

        CurrentFilePosition = sizeof(TLogHeader);
        CurrentBlockSize = 0;
    }

    LOG_DEBUG("Changelog created");
}

////////////////////////////////////////////////////////////////////////////////

namespace {

template <class T>
void ValidateSignature(const T& header)
{
    LOG_FATAL_UNLESS(header.Signature == T::CorrectSignature,
        "Invalid signature (expected %" PRIx64 ", got %" PRIx64 ")",
        T::CorrectSignature,
        header.Signature);
}

} // anonymous namespace

////////////////////////////////////////////////////////////////////////////////

void TChangeLog::TImpl::Open()
{
    YCHECK(State == EState::Uninitialized);
    LOG_DEBUG("Opening changelog (FileName: %s)", ~FileName);

    TGuard<TMutex> guard(Mutex);

    File.reset(new TBufferedFile(FileName, RdWr|Seq));

    // Read and check header of changelog.
    TLogHeader header;
    ReadPod(*File, header);
    ValidateSignature(header);
    YCHECK(header.ChangeLogId == Id);

    PrevRecordCount = header.PrevRecordCount;
    Epoch = header.Epoch;
    State = header.Finalized ? EState::Finalized : EState::Open;

    ReadIndex();
    ReadChangeLogUntilEnd();

    LOG_DEBUG("Changelog opened (RecordCount: %d, Finalized: %s)",
        RecordCount,
        ~FormatBool(header.Finalized));
}

////////////////////////////////////////////////////////////////////////////////

void TChangeLog::TImpl::Truncate(int truncatedRecordCount)
{
    YCHECK(State == EState::Open);
    YCHECK(truncatedRecordCount >= 0);

    if (truncatedRecordCount >= RecordCount) {
        return;
    }

    LOG_DEBUG("Truncating changelog: %d->%d",
        RecordCount,
        truncatedRecordCount);

    auto envelope = ReadEnvelope(truncatedRecordCount, truncatedRecordCount);
    if (truncatedRecordCount == 0) {
        Index.clear();
    } else {
        auto cutBound = (envelope.LowerBound.RecordIndex == truncatedRecordCount) ? envelope.LowerBound : envelope.UpperBound;
        auto indexPosition =
            std::lower_bound(Index.begin(), Index.end(), cutBound) - Index.begin();
        Index.resize(indexPosition);
    }

    i64 readSize = 0;
    TMemoryInput inputStream(envelope.Blob.Begin(), envelope.GetLength());
    for (int i = envelope.GetStartRecordIndex(); i < truncatedRecordCount; ++i) {
        TRecordHeader header;
        readSize += ReadPodPadded(inputStream, header);
        auto alignedSize = AlignUp(header.DataLength);
        inputStream.Skip(alignedSize);
        readSize += alignedSize;
    }

    RecordCount = truncatedRecordCount;
    CurrentBlockSize = readSize;
    CurrentFilePosition = envelope.GetStartPosition() + readSize;
    {
        TGuard<TMutex> guard(Mutex);
        IndexFile->Resize(sizeof(TLogIndexHeader) + Index.size() * sizeof(TLogIndexRecord));
        RefreshIndexHeader();
        File->Resize(CurrentFilePosition);
        File->Seek(0, sEnd);
    }
}

////////////////////////////////////////////////////////////////////////////////

void TChangeLog::TImpl::Flush()
{
    LOG_DEBUG("Flushing changelog");
    {
        TGuard<TMutex> guard(Mutex);
        File->Flush();
        IndexFile->Flush();
    }
    LOG_DEBUG("Changelog flushed");
}


////////////////////////////////////////////////////////////////////////////////

void TChangeLog::TImpl::WriteHeader(bool finalized)
{
    TGuard<TMutex> guard(Mutex);

    File->Seek(0, sSet);
    WritePod(*File, TLogHeader(Id, Epoch, PrevRecordCount, finalized));
    File->Flush();
}

void TChangeLog::TImpl::Finalize()
{
    YCHECK(State != EState::Uninitialized);
    if (State == EState::Finalized) {
        return;
    }

    LOG_DEBUG("Finalizing changelog");
    WriteHeader(true);
    State = EState::Finalized;
    LOG_DEBUG("Changelog finalized");
}

void TChangeLog::TImpl::Definalize()
{
    YCHECK(State == EState::Finalized);

    LOG_DEBUG("Definalizing changelog");

    WriteHeader(false);

    {
        // Additionally seek to the end of changelog
        TGuard<TMutex> guard(Mutex);
        File->Seek(0, sEnd);
    }
    State = EState::Open;

    LOG_DEBUG("Changelog definalized");
}


////////////////////////////////////////////////////////////////////////////////

int TChangeLog::TImpl::GetId() const
{
    return Id;
}

int TChangeLog::TImpl::GetPrevRecordCount() const
{
    return PrevRecordCount;
}

int TChangeLog::TImpl::GetRecordCount() const
{
    return RecordCount;
}

const TEpochId& TChangeLog::TImpl::GetEpoch() const
{
    YCHECK(State != EState::Uninitialized);
    return Epoch;
}

bool TChangeLog::TImpl::IsFinalized() const
{
    return State == EState::Finalized;
}

////////////////////////////////////////////////////////////////////////////////

namespace {

struct TRecordInfo
{
    TRecordInfo():
        Id(-1), TotalSize(-1)
    { }

    TRecordInfo(int id, int takenPlace):
        Id(id), TotalSize(takenPlace)
    { }

    int Id;
    int TotalSize;
};

// Trying to read one record from changelog file.
// Returns Null if reading is failed and record info otherwise.
template <class Stream>
TNullable<TRecordInfo> ReadRecord(TCheckableFileReader<Stream>& input)
{
    int readSize = 0;
    TRecordHeader header;
    readSize += ReadPodPadded(input, header);
    if (!input.Success() || header.DataLength <= 0) {
        return Null;
    }

    struct TChangeLogRecordTag { };
    auto data = TSharedRef::Allocate<TChangeLogRecordTag>(header.DataLength, false);
    readSize += ReadPadded(input, data);
    if (!input.Success()) {
        return Null;
    }

    auto checksum = GetChecksum(data);
    LOG_FATAL_UNLESS(header.Checksum == checksum,
        "Incorrect checksum of record %d", header.RecordIndex);
    return TRecordInfo(header.RecordIndex, readSize);
}

// Calculates maximal correct prefix of index.
size_t GetMaxCorrectIndexPrefix(const std::vector<TLogIndexRecord>& index, TBufferedFile* changelogFile)
{
    // Check adequacy of index.
    size_t correctPrefixLength = 0;
    for (int i = 0; i < index.size(); ++i) {
        bool correct;
        if (i == 0) {
            correct = index[i].FilePosition == sizeof(TLogHeader) && index[i].RecordIndex == 0;
        } else {
            correct =
                index[i].FilePosition > index[i - 1].FilePosition &&
                index[i].RecordIndex > index[i - 1].RecordIndex;
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
    auto checkableFile = CreateCheckableReader(*changelogFile);
    if (!ReadRecord(checkableFile)) {
        correctPrefixLength -= 1;
    }

    return correctPrefixLength;
}


} // anonymous namespace


void TChangeLog::TImpl::ProcessRecord(int recordIndex, int readSize)
{
    if (CurrentBlockSize >= IndexBlockSize || RecordCount == 0) {
        // Add index record in two cases:
        // 1) processing first record;
        // 2) size of records since previous index record is more than IndexBlockSize.
        YCHECK(Index.empty() || Index.back().RecordIndex != recordIndex);

        CurrentBlockSize = 0;
        Index.push_back(TLogIndexRecord(recordIndex, CurrentFilePosition));
        {
            TGuard<TMutex> guard(Mutex);
            WritePod(*IndexFile, Index.back());
            RefreshIndexHeader();
        }
        LOG_DEBUG("Changelog index record added (Index: %d, Offset: %" PRId64 ")",
            recordIndex, CurrentFilePosition);
    }
    // Record appended successfully.
    CurrentBlockSize += readSize;
    CurrentFilePosition += readSize;
    RecordCount += 1;
}

////////////////////////////////////////////////////////////////////////////////

void TChangeLog::TImpl::ReadIndex()
{
    // Read an existing index.
    {
        TMappedFileInput indexStream(IndexFileName);

        // Read and check index header.
        TLogIndexHeader indexHeader;
        ReadPod(indexStream, indexHeader);
        ValidateSignature(indexHeader);
        YCHECK(indexHeader.IndexSize >= 0);

        // Read index records.
        for (int i = 0; i < indexHeader.IndexSize; ++i) {
            TLogIndexRecord indexRecord;
            ReadPod(indexStream, indexRecord);
            Index.push_back(indexRecord);
        }
    }
    // Compute the maximum correct prefix and truncate the index.
    {
        auto correctPrefixSize = GetMaxCorrectIndexPrefix(Index, &(*File));
        LOG_ERROR_IF(correctPrefixSize < Index.size(), "Changelog index contains incorrect records");
        Index.resize(correctPrefixSize);

        IndexFile.reset(new TFile(IndexFileName, RdWr|Seq|CloseOnExec));
        IndexFile->Resize(sizeof(TLogIndexHeader) + Index.size() * sizeof(TLogIndexRecord));
        IndexFile->Seek(0, sEnd);
    }
}

void TChangeLog::TImpl::RefreshIndexHeader()
{
    i64 currentIndexPosition = IndexFile->GetPosition();
    IndexFile->Seek(0, sSet);
    WritePod(*IndexFile, TLogIndexHeader(Id, Index.size()));
    IndexFile->Seek(currentIndexPosition, sSet);
}

void TChangeLog::TImpl::ReadChangeLogUntilEnd()
{
    // Extract changelog properties from index.
    i64 fileLength = File->GetLength();
    CurrentBlockSize = 0;
    if (Index.empty()) {
        RecordCount = 0;
        CurrentFilePosition = sizeof(TLogHeader);
    } else {
        // Record count would be set below.
        CurrentFilePosition = Index.back().FilePosition;
    }

    // Seek to proper position in file, initialize checkable reader.
    File->Seek(CurrentFilePosition, sSet);
    auto checkableFile = CreateCheckableReader(*File);

    TNullable<TRecordInfo> recordInfo;
    if (!Index.empty()) {
        // Skip first record.
        recordInfo = ReadRecord(checkableFile);
        // It should be correct because we have already check index.
        YASSERT(recordInfo);
        RecordCount = Index.back().RecordIndex + 1;
        CurrentFilePosition += recordInfo->TotalSize;
    }

    while (CurrentFilePosition < fileLength) {
        // Record size also counts size of record header.
        recordInfo = ReadRecord(checkableFile);
        if (!recordInfo || recordInfo->Id != RecordCount) {
            // Broken changelog case.
            if (State == EState::Finalized) {
                LOG_ERROR("Finalized changelog contains a broken record (Index: %d, Offset: %" PRId64 ")",
                    RecordCount,
                    CurrentFilePosition);
            } else {
                LOG_ERROR("Broken record found, changelog trimmed (Index: %d, Offset: %" PRId64 ")",
                    RecordCount,
                    CurrentFilePosition);
            }
            File->Resize(CurrentFilePosition);
            File->Seek(0, sEnd);
            break;
        }
        ProcessRecord(recordInfo->Id, recordInfo->TotalSize);
    }
}

////////////////////////////////////////////////////////////////////////////////

namespace {

// This method uses forward iterator instead of reverse because they work faster.
// Asserts if last not greater element is absent.
template <class T>
typename std::vector<T>::const_iterator LastNotGreater(const std::vector<T>& vec, const T& value)
{
    auto res = std::upper_bound(vec.begin(), vec.end(), value);
    YCHECK(res != vec.begin());
    --res;
    return res;
}

template <class T>
typename std::vector<T>::const_iterator FirstGreater(const std::vector<T>& vec, const T& value)
{
    auto res = std::upper_bound(vec.begin(), vec.end(), value);
    return res;
}

} // anonymous namespace

TChangeLog::TImpl::TEnvelopeData TChangeLog::TImpl::ReadEnvelope(int firstRecordIndex, int lastRecordIndex)
{
    TEnvelopeData result;
    {
        TGuard<TMutex> guard(Mutex);

        // Index can be changes during Append, so we need search under the mutex
        result.LowerBound = *LastNotGreater(Index, TLogIndexRecord(firstRecordIndex, -1));
        auto it = FirstGreater(Index, TLogIndexRecord(lastRecordIndex, -1));
        result.UpperBound =
            it != Index.end() ?
            *it :
            TLogIndexRecord(RecordCount, CurrentFilePosition);
        struct TChangeLogEnvelopeTag { };
        result.Blob = TSharedRef::Allocate<TChangeLogEnvelopeTag>(result.GetLength(), false);

        size_t bytesRead = File->Pread(
            result.Blob.Begin(),
            result.GetLength(),
            result.GetStartPosition());
        YCHECK(bytesRead == result.GetLength());
    }
    return result;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NMetaState
} // namespace NYT
