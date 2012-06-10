#include "change_log_impl.h"

#include <ytlib/misc/fs.h>
#include <ytlib/misc/nullable.h>
#include <ytlib/misc/serialize.h>

#include <util/folder/dirut.h>

namespace NYT {
namespace NMetaState {

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger& Logger = MetaStateLogger;
static const char* const IndexSuffix = ".index";

////////////////////////////////////////////////////////////////////////////////

TChangeLog::TImpl::TImpl(
    const Stroka& fileName,
    i32 id,
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

void TChangeLog::TImpl::Append(i32 firstRecordId, const std::vector<TSharedRef>& records)
{
    YCHECK(firstRecordId == RecordCount);
    Append(records);
}

void TChangeLog::TImpl::Append(const TSharedRef& recordData)
{
    i32 recordId = RecordCount;
    TRecordHeader header(recordId, recordData.Size(), GetChecksum(recordData));

    i32 readSize = 0;
    readSize += AppendPodPadded(*File, header);
    readSize += AppendPadded(*File, recordData);

    ProcessRecord(recordId, readSize);
}

////////////////////////////////////////////////////////////////////////////////

void TChangeLog::TImpl::Read(i32 firstRecordId, i32 recordCount, std::vector<TSharedRef>* records)
{
    // Check stupid conditions.
    YCHECK(firstRecordId >= 0);
    YCHECK(recordCount >= 0);
    YCHECK(State != EState::Uninitialized);

    LOG_DEBUG("Reading records %d-%d", firstRecordId, firstRecordId + recordCount - 1);

    // Prevent search in empty index.
    if (Index.empty()) {
        records->clear();
        return;
    }

    recordCount = std::min(recordCount, RecordCount - firstRecordId);
    i32 lastRecordId = firstRecordId + recordCount;

    // Read envelope piece of changelog.
    auto envelope = ReadEnvelope(firstRecordId, lastRecordId);

    // Read records from envelope data and save them to the records.
    records->resize(recordCount);
    TMemoryInput inputStream(envelope.Blob.Begin(), envelope.Length());
    for (i32 recordId = envelope.StartRecordId(); recordId < envelope.EndRecordId(); ++recordId) {
        // Read and check header.
        TRecordHeader header;
        ReadPodPadded(inputStream, header);
        YCHECK(header.RecordId == recordId);

        // Save and pad data.
        TSharedRef data(envelope.Blob, TRef(const_cast<char*>(inputStream.Buf()), header.DataLength));
        inputStream.Skip(AlignUp(header.DataLength));

        // Add data to the records.
        if (recordId >= firstRecordId && recordId < lastRecordId) {
            (*records)[recordId - firstRecordId] = data;
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

namespace {

void Move(Stroka source, Stroka destination)
{
    if (isexist(~destination)) {
        YVERIFY(NFS::Remove(~destination));
    }
    YVERIFY(NFS::Rename(~source, ~destination));
}

template <class FileType, class HeaderType>
void AtomicWriteHeader(
    const Stroka& fileName,
    const HeaderType& header,
    THolder<FileType>* fileHolder)
{
    Stroka tempFileName(fileName + NFS::TempFileSuffix);
    FileType tempFile(tempFileName, WrOnly|CreateAlways);
    WritePod(tempFile, header);
    tempFile.Close();

    Move(tempFileName, fileName);
    fileHolder->Reset(new FileType(fileName, RdWr));
    (*fileHolder)->Seek(0, sEnd);
}

} // anonymous namespace

////////////////////////////////////////////////////////////////////////////////

void TChangeLog::TImpl::Create(i32 prevRecordCount)
{
    YCHECK(State == EState::Uninitialized);

    LOG_DEBUG("Creating changelog");

    PrevRecordCount = prevRecordCount;
    RecordCount = 0;
    State = EState::Open;

    {
        TGuard<TMutex> guard(Mutex);

        AtomicWriteHeader(FileName, TLogHeader(Id, prevRecordCount, /*finalized*/ false), &File);
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

// Calculates maximal correct prefix of index.
size_t GetMaxCorrectIndexPrefix(const std::vector<TLogIndexRecord>& index)
{
    size_t correctPrefixLength = 0;
    for (i32 i = 0; i < index.size(); ++i) {
        bool correct;
        if (i == 0) {
            correct = index[i].FilePosition == sizeof(TLogHeader) && index[i].RecordId == 0;
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
    return correctPrefixLength;
}

} // anonymous namespace

////////////////////////////////////////////////////////////////////////////////

void TChangeLog::TImpl::Open()
{
    YCHECK(State == EState::Uninitialized);
    LOG_DEBUG("Opening changelog (FileName: %s)", ~FileName);

    TGuard<TMutex> guard(Mutex);

    File.Reset(new TBufferedFile(FileName, RdWr|Seq));

    // Read and check header of changelog.
    TLogHeader header;
    ReadPod(*File, header);
    ValidateSignature(header);
    YCHECK(header.ChangeLogId == Id);

    PrevRecordCount = header.PrevRecordCount;
    State = header.Finalized ? EState::Finalized : EState::Open;

    ReadIndex();
    ReadChangeLogUntilEnd();

    LOG_DEBUG("Changelog opened (RecordCount: %d, Finalized: %d)",
        RecordCount,
        header.Finalized);
}

////////////////////////////////////////////////////////////////////////////////

void TChangeLog::TImpl::Truncate(i32 atRecordId)
{
    YCHECK(State == EState::Open);
    YCHECK(atRecordId >= 0);
    LOG_DEBUG("Truncating changelog (RecordId: %d)", atRecordId);

    if (atRecordId >= RecordCount) {
        return;
    }

    auto envelope = ReadEnvelope(atRecordId, atRecordId);
    if (atRecordId == 0) {
        Index.clear();
    } else {
        auto indexPosition =
            std::lower_bound(Index.begin(), Index.end(), envelope.UpperBound) - Index.begin();
        Index.resize(indexPosition);
    }

    i64 readSize = 0;
    TMemoryInput inputStream(envelope.Blob.Begin(), envelope.Length());
    for (i32 i = envelope.StartRecordId(); i < atRecordId; ++i) {
        TRecordHeader header;
        readSize += ReadPodPadded(inputStream, header);
        auto alignedSize = AlignUp(header.DataLength);
        inputStream.Skip(alignedSize);
        readSize += alignedSize;
    }

    RecordCount = atRecordId;
    CurrentBlockSize = readSize;
    CurrentFilePosition = envelope.StartPosition() + readSize;
    {
        TGuard<TMutex> guard(Mutex);
        IndexFile->Resize(sizeof(TLogIndexHeader) + Index.size() * sizeof(TLogIndexRecord));
        RefreshIndexHeader();
        File->Resize(CurrentFilePosition);
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

void TChangeLog::TImpl::Finalize()
{
    YCHECK(State != EState::Uninitialized);
    if (State == EState::Finalized)
    {
        return;
    }

    LOG_DEBUG("Finalizing changelog");

    TGuard<TMutex> guard(Mutex);

    // Write to the header that changelog is finalized.
    File->Seek(0, sSet);
    WritePod(*File, TLogHeader(Id, PrevRecordCount, /*Finalized*/ true));
    File->Flush();

    State = EState::Finalized;

    LOG_DEBUG("Changelog finalized");
}

////////////////////////////////////////////////////////////////////////////////

i32 TChangeLog::TImpl::GetId() const
{
    return Id;
}

i32 TChangeLog::TImpl::GetPrevRecordCount() const
{
    return PrevRecordCount;
}

i32 TChangeLog::TImpl::GetRecordCount() const
{
    return RecordCount;
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

    TRecordInfo(i32 id, i32 takenPlace):
        Id(id), TotalSize(takenPlace)
    { }

    i32 Id;
    i32 TotalSize;
};

// Trying to read one record from changelog file.
// Returns Null if reading is failed and record info otherwise.
template <class Stream>
TNullable<TRecordInfo> ReadRecord(TCheckableFileReader<Stream>& input)
{
    i32 readSize = 0;
    TRecordHeader header;
    readSize += ReadPodPadded(input, header);
    if (!input.Success()) {
        return Null;
    }

    TBlob blob(header.DataLength);
    auto data = TRef::FromBlob(blob);
    readSize += ReadPadded(input, data);
    if (!input.Success()) {
        return Null;
    }

    auto checksum = GetChecksum(data);
    LOG_FATAL_UNLESS(header.Checksum == checksum,
        "Incorrect checksum of record %d", header.RecordId);
    return TRecordInfo(header.RecordId, readSize);
}

} // anonymous namespace


void TChangeLog::TImpl::ProcessRecord(i32 recordId, i32 readSize)
{
    if (CurrentBlockSize >= IndexBlockSize || RecordCount == 0) {
        // Add index record in two cases:
        // 1) processing first record;
        // 2) size of records since previous index record is more than IndexBlockSize.
        YCHECK(Index.empty() || Index.back().RecordId != recordId);

        CurrentBlockSize = 0;
        Index.push_back(TLogIndexRecord(recordId, CurrentFilePosition));
        {
            TGuard<TMutex> guard(Mutex);
            WritePod(*IndexFile, Index.back());
            RefreshIndexHeader();
        }
        LOG_DEBUG("Changelog index record added (RecordId: %d, Offset: %" PRId64 ")",
            recordId, CurrentFilePosition);
    }
    // Successfuly append record.
    CurrentBlockSize += readSize;
    CurrentFilePosition += readSize;
    RecordCount += 1;
}

////////////////////////////////////////////////////////////////////////////////

void TChangeLog::TImpl::ReadIndex()
{
    TMappedFileInput indexStream(IndexFileName);

    // Read and check index header.
    TLogIndexHeader indexHeader;
    ReadPod(indexStream, indexHeader);
    ValidateSignature(indexHeader);
    YCHECK(indexHeader.IndexSize >= 0);

    // Read index records.
    for (i32 i = 0; i < indexHeader.IndexSize; ++i) {
        TLogIndexRecord indexRecord;
        ReadPod(indexStream, indexRecord);
        Index.push_back(indexRecord);
    }
    Index.resize(GetMaxCorrectIndexPrefix(Index));

    IndexFile.Reset(new TFile(IndexFileName, RdWr|Seq));
    IndexFile->Resize(sizeof(TLogIndexHeader) + Index.size() * sizeof(TLogIndexRecord));
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
    CurrentBlockSize = 0;
    if (Index.empty()) {
        RecordCount = 0;
        CurrentFilePosition = sizeof(TLogHeader);
    } else {
        RecordCount = Index.back().RecordId;
        CurrentFilePosition = Index.back().FilePosition;
    }
    i64 fileLength = File->GetLength();

    // Seek to proper position in file, initialize checkable reader.
    File->Seek(CurrentFilePosition, sSet);
    auto checkableFile = CreateCheckableReader(*File);

    TNullable<TRecordInfo> recordInfo;
    if (!Index.empty()) {
        // Skip first record.
        recordInfo = ReadRecord(checkableFile);
        LOG_FATAL_UNLESS(recordInfo, "Incorrect indexed changelog record");
        CurrentFilePosition += recordInfo->TotalSize;
        RecordCount += 1;
    }

    while (CurrentFilePosition < fileLength) {
        // Record size also counts size of record header.
        recordInfo = ReadRecord(checkableFile);
        if (!recordInfo) {
            // Broken changelog case.
            LOG_ERROR("Changelog contains incorrect record with id %d at position %"PRIx64,
                RecordCount, CurrentFilePosition);
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

} // anonymous namesapce

TChangeLog::TImpl::TEnvelopeData TChangeLog::TImpl::ReadEnvelope(i32 firstRecordId, i32 lastRecordId)
{
    TEnvelopeData result;
    result.LowerBound = *LastNotGreater(Index, TLogIndexRecord(firstRecordId, -1));
    auto it = FirstGreater(Index, TLogIndexRecord(lastRecordId, -1));
    result.UpperBound =
        it != Index.end() ?
        *it :
        TLogIndexRecord(RecordCount, CurrentFilePosition);

    TSharedRef sharedBlob;
    {
        TBlob blob(result.Length());
        TGuard<TMutex> guard(Mutex);
        File->Pread(blob.begin(), result.Length(), result.StartPosition());
        result.Blob = TSharedRef(MoveRV(blob));
    }
    return result;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NMetaState
} // namespace NYT
