#include "change_log.h"

#include "../misc/serialize.h"
#include "../misc/checksum.h"
#include "../logging/log.h"

#include <util/generic/algorithm.h>
#include <util/digest/murmur.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger Logger("ChangeLog");
static const char* const IndexSuffix = ".index";

////////////////////////////////////////////////////////////////////////////////

namespace {

// Alignment should be a power of two.
// Alignment is measured in bytes.
const size_t ALIGNMENT = 4;
const ui8 PADDING[ALIGNMENT] = { 0 };

STATIC_ASSERT(!(ALIGNMENT & (ALIGNMENT - 1)));

size_t GetPaddingSize(size_t size)
{
    size_t res = size % ALIGNMENT;
    return res == 0 ? 0 : ALIGNMENT - res;
}

size_t AlignUp(size_t size)
{
    return size + GetPaddingSize(size);
}

void WritePadding(TOutputStream& output, size_t recordSize)
{
    output.Write(&PADDING, GetPaddingSize(recordSize));
}

} // namespace <anonymous>

////////////////////////////////////////////////////////////////////////////////

#pragma pack(push, 4)

struct TChangeLog::TLogHeader
{
    static const ui64 CurrentSignature = 0x313030304C435459ull; // YTCL0001

    ui64 Signature;
    i32 SegmentId;
    i32 PrevRecordCount;
    i32 Finalized;

    TLogHeader()
        : Signature(0)
        , SegmentId(0)
        , Finalized(0)
    { }

    TLogHeader(i32 segmentId, i32 prevRecordCount, bool finalized)
        : Signature(CurrentSignature)
        , SegmentId(segmentId)
        , PrevRecordCount(prevRecordCount)
        , Finalized(finalized ? -1 : 0)
    { }

    void Validate() const
    {
        if (Signature != CurrentSignature) {
            ythrow yexception()
                << Sprintf("Invalid TLogHeader signature: expected %" PRIx64 ", found %" PRIx64,
                CurrentSignature, Signature);
        }
    }
};

#pragma pack(pop)

////////////////////////////////////////////////////////////////////////////////

#pragma pack(push, 4)

struct TChangeLog::TRecordHeader
{
    i32 RecordId;
    i32 DataLength;
    TChecksum Checksum;

    TRecordHeader()
        : RecordId(0)
        , DataLength(0)
        , Checksum(0)
    { }

    TRecordHeader(i32 recordId, i32 dataLength)
        : RecordId(recordId)
        , DataLength(dataLength)
        , Checksum(0)
    { }
};

#pragma pack(pop)

////////////////////////////////////////////////////////////////////////////////

#pragma pack(push, 4)

struct TChangeLog::TLogIndexHeader
{                                                       
    static const ui64 CurrentSignature = 0x31303030494C5459ull; // YTLI0001

    ui64 Signature;
    i32 SegmentId;
    i32 RecordCount;

    TLogIndexHeader()
        : Signature(0)
        , SegmentId(0)
        , RecordCount(0)
    { }

    TLogIndexHeader(i32 segmentId, i32 recordCount)
        : Signature(CurrentSignature)
        , SegmentId(segmentId)
        , RecordCount(recordCount)
    { }

    void Validate() const
    {
        if (Signature != CurrentSignature) {
            ythrow yexception()
                << Sprintf("Invalid TLogIndexHeader signature: expected %" PRIx64 ", found %" PRIx64,
                CurrentSignature, Signature);
        }
    }
};

#pragma pack(pop)

////////////////////////////////////////////////////////////////////////////////

bool TChangeLog::TLogIndexRecord::operator<(const TLogIndexRecord& right) const
{
    return RecordId < right.RecordId;
}

////////////////////////////////////////////////////////////////////////////////

TChangeLog::TChangeLog(Stroka fileName, i32 id, i32 indexBlockSize)
    : State(S_Closed)
    , FileName(fileName)
    , IndexFileName(fileName + IndexSuffix)
    , Id(id)
    , IndexBlockSize(indexBlockSize)
    , PrevRecordCount(-1)
    , CurrentBlockSize(-1)
    , CurrentFilePosition(-1)
    , RecordCount(-1)
{
    // Visibility workaround.
    STATIC_ASSERT(sizeof(TLogHeader) == 20);
    STATIC_ASSERT(sizeof(TRecordHeader) == 16);
    STATIC_ASSERT(sizeof(TLogIndexHeader) == 16);
}

void TChangeLog::Open()
{
    YASSERT(State == S_Closed);

    LOG_DEBUG("Opening changelog %s", ~FileName);
    File.Reset(new TFile(FileName, RdWr));

    TLogHeader header;
    if (!NYT::Read(*File, &header)) {
        ythrow yexception() << "Cannot read header of changelog " << FileName;
    }
    header.Validate();
    
    YASSERT(header.SegmentId == Id);
    PrevRecordCount = header.PrevRecordCount;

    i64 currentIndexFilePosition = sizeof(TLogIndexHeader);
    {
        TBufferedFileInput indexInput(IndexFileName);
        TLogIndexHeader indexHeader;
        if (!NYT::Read(indexInput, &indexHeader)) {
            ythrow yexception() << "Cannot read header of changelog index " << IndexFileName;
        }
        indexHeader.Validate();

        YASSERT(indexHeader.RecordCount >= 0);

        Index.clear();
        LOG_DEBUG("Opening index file with %d records", indexHeader.RecordCount);
        if (indexHeader.RecordCount == 0) {
            RecordCount = 0;
            CurrentFilePosition = (long) File->GetPosition();
            CurrentBlockSize = IndexBlockSize; // hack for including record 0 into index
        } else {
            for (i32 i = 0; i < indexHeader.RecordCount; ++i) {
                TLogIndexRecord record;
                NYT::Read(indexInput, &record);
                if (i == 0) {
                    YASSERT(record.RecordId == 0);
                    YASSERT(record.Offset >= 0);
                } else {
                    YASSERT(record.RecordId > Index.back().RecordId);
                    YASSERT(record.Offset > Index.back().Offset);
                }
                Index.push_back(record);
                currentIndexFilePosition += sizeof(TLogIndexRecord);
            }

            RecordCount = Index.back().RecordId;
            CurrentFilePosition = Index.back().Offset;
            File->Seek(CurrentFilePosition, sSet);
            CurrentBlockSize = 0;
        }
    }

    i64 fileLength = File->GetLength();
    TBufferedFileInput fileInput(*File);
    IndexFile.Reset(new TFile(IndexFileName, RdWr));

    TBlob buffer;
    while (CurrentFilePosition < fileLength) {
        TRecordHeader recordHeader;
        if (!NYT::Read(fileInput, &recordHeader)) {
            LOG_WARNING("Can't read header of record %d at %" PRISZT "(ChangeLogId: %d)",
                RecordCount, CurrentFilePosition, Id);
            break;
        }

        i32 recordId = recordHeader.RecordId;
        if (RecordCount != recordId) {
            LOG_ERROR("Invalid record id at %" PRISZT ": expected %d, got %d (ChangeLogId: %d)",
                CurrentFilePosition, RecordCount, recordHeader.RecordId, Id);
            break;
        }

        size_t size = AlignUp(sizeof(recordHeader) + (size_t) recordHeader.DataLength);
        if ((i64) CurrentFilePosition + (i64) size > fileLength) {
            LOG_WARNING("Can't read data of record %d at %" PRISZT " (ChangeLogId: %d)",
                recordId, CurrentFilePosition, Id);
            break;
        }

        buffer.resize(size - sizeof(recordHeader));
        fileInput.Load(buffer.begin(), buffer.size());
        void* ptr = (void *) buffer.begin();
        TChecksum checksum = GetChecksum(TRef(ptr, (size_t) recordHeader.DataLength));
        if (checksum != recordHeader.Checksum) {
            LOG_ERROR("Invalid checksum of record %d at %" PRISZT " (ChangeLogId: %d)",
                recordId, CurrentFilePosition, Id);
            break;
        }

        // TODO: refactor
        // introduce a flag parameter for HandleRecord
        if(!Index.empty() && Index.back().RecordId == recordId) {
            // Do not handle record we just seeked to.
            ++RecordCount;
            AtomicAdd(CurrentFilePosition, size);
        } else {
            HandleRecord(recordId, size);
        }
    }

    File->Seek(CurrentFilePosition, sSet);
    FileOutput.Reset(new TBufferedFileOutput(*File));
    FileOutput->SetFlushPropagateMode(true);

    IndexFile->Resize(currentIndexFilePosition);

    State = header.Finalized ? S_Finalized : S_Open;

    LOG_DEBUG("Changelog %d opened (RecordCount: %d, Finalized: %d)",
                Id, RecordCount, header.Finalized);
}

void TChangeLog::Create(i32 prevRecordCount)
{
    YASSERT(State == S_Closed);

    PrevRecordCount = prevRecordCount;
    RecordCount = 0;

    File.Reset(new TFile(FileName, RdWr | CreateAlways));
    TLogHeader header(Id, prevRecordCount, false);
    NYT::Write(*File, header);

    IndexFile.Reset(new TFile(IndexFileName, RdWr | CreateAlways));
    NYT::Write(*IndexFile, TLogIndexHeader(Id, 0));
    
    CurrentFilePosition = (i32) File->GetPosition();
    CurrentBlockSize = IndexBlockSize; // hack for including record 0 into index
    
    FileOutput.Reset(new TBufferedFileOutput(*File));
    FileOutput->SetFlushPropagateMode(true);

    State = S_Open;

    LOG_DEBUG("Changelog %d created", Id);
}

void TChangeLog::Finalize()
{
    YASSERT(State == S_Open);

    Flush();

    File->Seek(0, sSet);
    TLogHeader header(Id, PrevRecordCount, true);
    NYT::Write(*File, header);
    File->Flush();

    State = S_Finalized;

    LOG_DEBUG("Changelog %d finalized", Id);
}

void TChangeLog::Append(i32 recordId, TSharedRef recordData)
{
    // Make a coarse check first...
    YASSERT(State == S_Open || State == S_Finalized);

    // ... and handle finalized changelogs next.
    if (State == S_Finalized) {
        LOG_FATAL("Unable to append to a finalized changelog %d", Id);
    }

    if (recordId != RecordCount) {
        LOG_FATAL("Unexpected record id in changelog %d (ExpectedId: %d, ReceivedId: %d)",
                   Id, RecordCount, recordId);
    }

    i32 recordSize = 0;
    TRecordHeader header(recordId, recordData.Size());
    header.Checksum = GetChecksum(TRef(recordData.Begin(), recordData.Size()));

    Write(*FileOutput, header);
    recordSize += sizeof(header);
    FileOutput->Write(recordData.Begin(), recordData.Size());
    recordSize += recordData.Size();
    WritePadding(*FileOutput, recordSize);
    recordSize = AlignUp(recordSize);

    HandleRecord(recordId, recordSize);
}

void TChangeLog::Flush()
{
    FileOutput->Flush();
}

void TChangeLog::Read(i32 firstRecordId, i32 recordCount, yvector<TSharedRef>* result)
{
    YASSERT(firstRecordId >= 0);
    YASSERT(recordCount >= 0);
    YASSERT(result);

    YASSERT(State == S_Open || State == S_Finalized);

    // TODO(sandello): WTF? Why?
    // Check if the changelog is empty.
    // NB: Cannot call GetXXXBound for an empty changelog since its index is empty.
    if (RecordCount == 0)
        return;

    TSharedRef::TBlobPtr data(new TBlob());
    result->clear();

    i32 lastRecordId = firstRecordId + recordCount - 1;
    i64 lowerBound, upperBound;
    {
        TGuard<TSpinLock> guard(IndexSpinLock);
        lowerBound = GetLowerBound(firstRecordId)->Offset;

        TIndex::iterator it = GetUpperBound(lastRecordId);
        if (it == Index.end()) {
            upperBound = CurrentFilePosition;
        } else {
            upperBound = it->Offset;
        }
    }

    size_t length = static_cast<size_t>(upperBound - lowerBound);
    data->resize(length);
    File->Pread(data->begin(), length, lowerBound);
    
    // TODO(sandello): Read this out and refactor with util/memory/*.
    i32 currentRecordId = firstRecordId;
    size_t position = 0;
    while (position < length) {
        i64 filePosition = lowerBound + position;
        if (position + sizeof(TRecordHeader) >= data->size()) {
            LOG_DEBUG("Can't read record header at %" PRId64, filePosition);
            break;
        }

        TRecordHeader* header = reinterpret_cast<TRecordHeader*>(&data->at(position));
        if (header->RecordId > lastRecordId) {
            break;
        }

        // TODO: can we premature exit, if header->RecordId 
        if (header->RecordId >= firstRecordId) {
            if (header->RecordId != currentRecordId) {
                LOG_DEBUG("Invalid record id at %" PRId64 ": expected %d, got %d",
                    filePosition,
                    currentRecordId,
                    header->RecordId);
                break;
            }

            if (position + sizeof(TRecordHeader) + header->DataLength > data->size()) {
                LOG_DEBUG("Can't read data of record %d at %" PRId64,
                    header->RecordId,
                    filePosition);
                break;
            }

            char* ptr = reinterpret_cast<char*>(&data->at(position + sizeof(TRecordHeader)));
            TChecksum checksum = GetChecksum(TRef(ptr, header->DataLength));
            if (checksum != header->Checksum) {
                LOG_DEBUG("Invalid checksum of record %d at %" PRId64,
                    header->RecordId,
                    filePosition);
                break;
            }

            result->push_back(TSharedRef(data, TRef(ptr, (size_t) header->DataLength)));
            ++currentRecordId;
        }

        position += AlignUp(sizeof(TRecordHeader) + (size_t) header->DataLength);
    }
}

void TChangeLog::Truncate(i32 recordId)
{
    LOG_DEBUG("Truncating changelog from %d recordId", recordId);
    i64 lowerBound, upperBound;
    i32 currentRecordId;
    {
        TGuard<TSpinLock> guard(IndexSpinLock);
        TIndex::iterator it = GetUpperBound(recordId);
        if (it == Index.end()) {
            upperBound = CurrentFilePosition;
        } else {
            upperBound = it->Offset;
        }

        TIndex::iterator itPrev = GetLowerBound(recordId);
        currentRecordId = itPrev->RecordId;
        lowerBound = itPrev->Offset;

        if (currentRecordId == recordId) {
            TruncateIndex(itPrev - Index.begin());
        } else {
            TruncateIndex(it - Index.begin());
        }
    }

    size_t length = (size_t)(upperBound - lowerBound);

    TBlob dataHolder(length);
    File->Pread(dataHolder.begin(), length, lowerBound);

    size_t position = 0;
    while (currentRecordId < recordId) {
        i64 filePosition = lowerBound + position;

        // All records before recordId should be ok
        if (position + sizeof(TRecordHeader) >= dataHolder.size()) {
            LOG_FATAL("Can't read record header at %" PRId64, filePosition);
        }

        TRecordHeader* header = (TRecordHeader*) &dataHolder.at(position);
        if (currentRecordId != header->RecordId) {
            LOG_FATAL("Invalid record id at %" PRId64 ": expected %d, got %d",
                filePosition, currentRecordId, header->RecordId);
        }

        if (position + sizeof(TRecordHeader) + header->DataLength > dataHolder.size()) {
            LOG_FATAL("Can't read data of record %d at %" PRId64,
                header->RecordId,
                filePosition);
        }
        ++currentRecordId;

        position += AlignUp(sizeof(TRecordHeader) + (size_t) header->DataLength);
    }

    CurrentBlockSize = position;
    File->Resize(lowerBound + position);
    RecordCount = recordId;

    LOG_DEBUG("Changelog %d is truncated to %d record(s)",
        Id, recordId);
}

void TChangeLog::HandleRecord(i32 recordId, i32 recordSize)
{
    ++RecordCount;
    i32 filePosition = CurrentFilePosition;
    AtomicAdd(CurrentFilePosition, recordSize);
    YASSERT(CurrentFilePosition >= 0 && CurrentFilePosition <= Max<i32>());
    CurrentBlockSize += recordSize;

    if (CurrentBlockSize >= IndexBlockSize) {
        CurrentBlockSize = 0;

        bool appendToIndexFile = false;
        i32 indexRecordCount = -1;

        TLogIndexRecord record;
        record.RecordId = recordId;
        record.Offset = filePosition;
        {
            TGuard<TSpinLock> guard(IndexSpinLock);
            if (Index.empty() || Index.back().RecordId != record.RecordId) {
                Index.push_back(record);
                indexRecordCount = Index.ysize();
                appendToIndexFile = true;
            }
        }

        if (appendToIndexFile) {
            LOG_DEBUG("Record (%d, %d) is added to index of changelog %d",
                        record.RecordId, record.Offset, Id);
            IndexFile->Seek(0, sEnd);
            NYT::Write(*IndexFile, record);
            IndexFile->Seek(0, sSet);
            TLogIndexHeader header(Id, indexRecordCount);
            NYT::Write(*IndexFile, header);
            IndexFile->Flush();
        }
    }
}

// TODO: wtf? why this thing calls UpperBound instead of lower bound?
TChangeLog::TIndex::iterator TChangeLog::GetLowerBound(i32 recordId)
{
    YASSERT(Index.ysize() > 0);
    TLogIndexRecord record;
    record.RecordId = recordId;
    record.Offset = Max<i32>();
    TIndex::iterator it = UpperBound(Index.begin(), Index.end(), record);
    --it;
    return it;
}

TChangeLog::TIndex::iterator TChangeLog::GetUpperBound(i32 recordId)
{
    YASSERT(Index.ysize() > 0);
    TLogIndexRecord record;
    record.RecordId = recordId;
    record.Offset = Max<i32>();
    TIndex::iterator it = UpperBound(Index.begin(), Index.end(), record);
    return it;
}

// Should be called under SpinLock
void TChangeLog::TruncateIndex(i32 indexRecordId)
{
    // TODO: polish
    LOG_DEBUG("Truncating changelog index from %d recordId", indexRecordId);
    Index.erase(Index.begin() + indexRecordId, Index.end());

    TLogIndexHeader header(Id, Index.size());
    IndexFile->Seek(0, sSet);
    NYT::Write(*IndexFile, header);
    // TODO: polish
    LOG_DEBUG("LogIndexHeader was updated with size %d", Index.ysize());

    i64 position = sizeof(TLogIndexHeader) + indexRecordId * sizeof(TLogIndexRecord);
    IndexFile->Resize(position);
    IndexFile->Flush();
}

bool TChangeLog::IsFinalized() const
{
    return State == S_Finalized;
}

TMasterStateId TChangeLog::GetPrevStateId() const
{
    if (Id == 0) {
        YASSERT(PrevRecordCount == 0);
        return TMasterStateId(0, 0);
    } else {
        return TMasterStateId(Id - 1, PrevRecordCount);
    }
}

i32 TChangeLog::GetRecordCount() const
{
    return RecordCount;
}

i32 TChangeLog::GetId() const
{
    return Id;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
