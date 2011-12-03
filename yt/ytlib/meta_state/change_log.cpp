#include "stdafx.h"
#include "change_log.h"

#include "../misc/serialize.h"
#include "../misc/checksum.h"
#include "../logging/log.h"

#include <algorithm>
#include <util/generic/ptr.h>
#include <util/generic/noncopyable.h>
#include <util/digest/murmur.h>

namespace NYT {
namespace NMetaState {

////////////////////////////////////////////////////////////////////////////////

//! Auxiliary constants and functions.
namespace {

static NLog::TLogger& Logger = MetaStateLogger;
static const char* const IndexSuffix = ".index";

} // namespace <anonymous>

////////////////////////////////////////////////////////////////////////////////

//! Implementation of TChangeLog.
//! \cond Implementation
class TChangeLog::TImpl
    : private ::TNonCopyable
{
public:
    TImpl(
        Stroka fileName,
        i32 id,
        i32 indexBlockSize);

    void Open();
    void Create(i32 previousRecordCount);
    void Finalize();

    void Append(i32 recordId, TSharedRef recordData);
    void Read(i32 firstRecordId, i32 recordCount, yvector<TSharedRef>* result);
    void Flush();
    void Truncate(i32 recordId);

    i32 GetId() const;
    i32 GetPrevRecordCount() const;
    i32 GetRecordCount() const;
    bool IsFinalized() const;

private:
    // Binary Structures {{{
    ////////////////////////////////////////////////////////////////////////////////

    #pragma pack(push, 4)

    struct TLogHeader
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

        void ValidateAndThrow() const
        {
            if (Signature != CurrentSignature) {
                ythrow yexception() << Sprintf(
                    "Invalid log header signature (expected %" PRIx64 ", got %" PRIx64 ")",
                    CurrentSignature,
                    Signature);
            }
        }
    };

    STATIC_ASSERT(sizeof(TLogHeader) == 20);

    #pragma pack(pop)

    ////////////////////////////////////////////////////////////////////////////////

    #pragma pack(push, 4)

    struct TRecordHeader
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

    STATIC_ASSERT(sizeof(TRecordHeader) == 16);

    #pragma pack(pop)

    ////////////////////////////////////////////////////////////////////////////////

    #pragma pack(push, 4)

    struct TLogIndexHeader
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
                ythrow yexception() << Sprintf(
                    "Invalid TLogIndexHeader signature "
                    "(expected %" PRIx64 ", got %" PRIx64 ")",
                    CurrentSignature,
                    Signature);
            }
        }
    };

    STATIC_ASSERT(sizeof(TLogIndexHeader) == 16);

    #pragma pack(pop)

    ////////////////////////////////////////////////////////////////////////////////

    #pragma pack(push, 4)

    struct TLogIndexRecord
    {
        i32 RecordId;
        i32 Offset;

        bool operator<(const TLogIndexRecord& other) const
        {
            return RecordId < other.RecordId;
        }
    };

    STATIC_ASSERT(sizeof(TLogIndexRecord) == 8);

    #pragma pack(pop)

    ////////////////////////////////////////////////////////////////////////////////
    // }}}

private:
    DECLARE_ENUM(EState,
        (Closed)
        (Open)
        (Finalized)
    );

    typedef yvector<TLogIndexRecord> TIndex;

    void HandleRecord(i32 recordId, i32 recordSize);
    TIndex::iterator GetLowerBound(i32 recordId);
    TIndex::iterator GetUpperBound(i32 recordId);
    void TruncateIndex(i32 indexRecordId);

    EState State;
    Stroka FileName;
    Stroka IndexFileName;
    i32 Id;
    i32 IndexBlockSize;

    i32 PrevRecordCount;
    i32 CurrentBlockSize;
    i64 CurrentFilePosition;
    i32 RecordCount;

    TMutex Mutex;
    TIndex Index;
    THolder<TFile> File;
    THolder<TBufferedFileOutput> FileOutput;

    THolder<TFile> IndexFile;
}; // class TChangeLog::TImpl
//! \endcond

////////////////////////////////////////////////////////////////////////////////

TChangeLog::TImpl::TImpl(
    Stroka fileName,
    i32 id,
    i32 indexBlockSize)
    : State(EState::Closed)
    , FileName(fileName)
    , IndexFileName(fileName + IndexSuffix)
    , Id(id)
    , IndexBlockSize(indexBlockSize)
    , PrevRecordCount(-1)
    , CurrentBlockSize(-1)
    , CurrentFilePosition(-1)
    , RecordCount(-1)
{ }

void TChangeLog::TImpl::Open()
{
    YASSERT(State == EState::Closed);

    LOG_DEBUG("Opening changelog (Id: %d, FileName: %s)",
        Id,
        ~FileName);

    File.Reset(new TFile(FileName, RdWr|Seq));

    // TODO: Why don't use ysaveload.h?
    // TODO: Why return values are inconsistent?
    // ::Read returns bool while Validate() throws exception.
    TLogHeader header;
    if (!NYT::Read(*File, &header)) {
        ythrow yexception() << "Cannot read changelog header of " << FileName.Quote();
    }
    header.ValidateAndThrow();
    
    YASSERT(header.SegmentId == Id);
    PrevRecordCount = header.PrevRecordCount;

    i64 currentIndexFilePosition = sizeof(TLogIndexHeader);
    {
        TBufferedFileInput indexInput(IndexFileName);

        TLogIndexHeader indexHeader;
        if (!NYT::Read(indexInput, &indexHeader)) {
            ythrow yexception() << "Cannot read header of changelog index " << IndexFileName.Quote();
        }
        indexHeader.Validate();

        YASSERT(indexHeader.RecordCount >= 0);

        Index.clear();
        LOG_DEBUG("Opening changelog index (SegmentId: %d, RecordCount: %d)",
            Id,
            indexHeader.RecordCount);

        if (indexHeader.RecordCount == 0) {
            RecordCount = 0;
            CurrentFilePosition = File->GetPosition();
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
            LOG_WARNING("Cannot read changelog record header (SegmentId: %d, RecordId: %d, Offset: %" PRId64 ")",
                Id,
                RecordCount,
                CurrentFilePosition);
            break;
        }

        i32 recordId = recordHeader.RecordId;
        if (RecordCount != recordId) {
            LOG_ERROR("Invalid record id (SegmentId: %d, Offset: %" PRISZT ", ExpectedId: %d, FoundId: %d)",
                Id,
                CurrentFilePosition,
                RecordCount,
                recordHeader.RecordId);
            break;
        }

        i32 recordSisze = AlignUp(static_cast<i32>(sizeof(recordHeader)) + recordHeader.DataLength);
        if (CurrentFilePosition + recordSisze > fileLength) {
            LOG_WARNING("Cannot read changelog record data (SegmentId: %d, RecordId: %d, Offset: %" PRId64 ")",
                Id,
                recordId,
                CurrentFilePosition);
            break;
        }

        buffer.resize(static_cast<size_t>(recordSisze) - sizeof(recordHeader));
        fileInput.Load(buffer.begin(), buffer.size());
        void* ptr = (void *) buffer.begin();
        TChecksum checksum = GetChecksum(TRef(ptr, (size_t) recordHeader.DataLength));
        if (checksum != recordHeader.Checksum) {
            LOG_ERROR("Invalid changelog record checksum (SegmentId: %d, RecordId: %d, Offset: %" PRId64 ")",
                Id,
                recordId,
                CurrentFilePosition);
            break;
        }

        // TODO: refactor
        // introduce a flag parameter for HandleRecord
        if(!Index.empty() && Index.back().RecordId == recordId) {
            // Do not handle record we just seeked to.
            ++RecordCount;
            CurrentFilePosition += recordSisze;
        } else {
            HandleRecord(recordId, recordSisze);
        }
    }

    File->Seek(CurrentFilePosition, sSet);

    FileOutput.Reset(new TBufferedFileOutput(*File));

    IndexFile->Resize(currentIndexFilePosition);

    State = header.Finalized ? EState::Finalized : EState::Open;

    LOG_DEBUG("Changelog %d opened (RecordCount: %d, Finalized: %d)",
        Id,
        RecordCount,
        header.Finalized);
}

void TChangeLog::TImpl::Create(i32 prevRecordCount)
{
    YASSERT(State == EState::Closed);

    PrevRecordCount = prevRecordCount;
    RecordCount = 0;

    File.Reset(new TFile(FileName, RdWr | CreateAlways));
    TLogHeader header(Id, prevRecordCount, false);
    NYT::Write(*File, header);

    IndexFile.Reset(new TFile(IndexFileName, RdWr | CreateAlways));
    NYT::Write(*IndexFile, TLogIndexHeader(Id, 0));
    
    CurrentFilePosition = File->GetPosition();
    CurrentBlockSize = IndexBlockSize; // hack for including record 0 into index
    
    FileOutput.Reset(new TBufferedFileOutput(*File));

    State = EState::Open;

    LOG_DEBUG("Changelog %d created", Id);
}

void TChangeLog::TImpl::Finalize()
{
    TGuard<TMutex> guard(Mutex);

    YASSERT(State == EState::Open);

    Flush();

    File->Seek(0, sSet);
    TLogHeader header(Id, PrevRecordCount, true);
    NYT::Write(*File, header);
    File->Flush();

    State = EState::Finalized;

    LOG_DEBUG("Changelog %d finalized", Id);
}

void TChangeLog::TImpl::Append(i32 recordId, TSharedRef recordData)
{
    // Make a coarse check first...
    YASSERT(State == EState::Open || State == EState::Finalized);

    // ... and handle finalized changelogs next.
    if (State == EState::Finalized) {
        LOG_FATAL("Unable to append to a finalized changelog (SegmentId: %d)",
            Id);
    }

    if (recordId != RecordCount) {
        LOG_FATAL("Unexpected record id in changelog %d (expected: %d, got: %d)",
            Id,
            RecordCount,
            recordId);
    }

    i32 recordSize = 0;
    TRecordHeader header(recordId, recordData.Size());
    header.Checksum = GetChecksum(TRef(recordData.Begin(), recordData.Size()));

    {
        TGuard<TMutex> guard(Mutex);

        Write(*FileOutput, header);
        recordSize += sizeof(header);
        
        FileOutput->Write(recordData.Begin(), recordData.Size());
        recordSize += recordData.Size();

        WritePadding(*FileOutput, recordSize);
        recordSize = AlignUp(recordSize);

        HandleRecord(recordId, recordSize);
    }
}

void TChangeLog::TImpl::Flush()
{
    TGuard<TMutex> guard(Mutex);
    FileOutput->Flush();
    File->Flush();

    LOG_DEBUG("Changelog is flushed (SegmentId: %d)", Id);
}

void TChangeLog::TImpl::Read(i32 firstRecordId, i32 recordCount, yvector<TSharedRef>* result)
{
    YASSERT(firstRecordId >= 0);
    YASSERT(recordCount >= 0);
    YASSERT(result != NULL);

    YASSERT(State == EState::Open || State == EState::Finalized);

    // TODO(sandello): WTF? Why?
    // Check if the changelog is empty.
    // NB: Cannot call GetXXXBound for an empty changelog since its index is empty.
    if (RecordCount == 0)
        return;

    TBlob blob;
    result->clear();

    i32 lastRecordId;
    i64 lowerBound, upperBound;
    size_t length;

    {
        TGuard<TMutex> guard(Mutex);

        // Compute lower and upper estimates.
        lastRecordId = firstRecordId + recordCount - 1;
        lowerBound = GetLowerBound(firstRecordId)->Offset;

        auto it = GetUpperBound(lastRecordId);
        if (it == Index.end()) {
            upperBound = CurrentFilePosition;
        } else {
            upperBound = it->Offset;
        }

        // Prepare the buffer.
        length = static_cast<size_t>(upperBound - lowerBound);
        blob.resize(length);

        // Ensure that all buffers are flushed to disk so pread will succeed.
        FileOutput->Flush();
     
        // Do the actual read.
        File->Pread(blob.begin(), length, lowerBound);
    }
    
    TSharedRef sharedBlob(MoveRV(blob));

        // TODO(sandello): Read this out and refactor with util/memory/*.
    i32 currentRecordId = firstRecordId;
    size_t position = 0;
    while (position < length) {
        i64 filePosition = lowerBound + position;
        if (position + sizeof(TRecordHeader) >= length) {
            LOG_DEBUG("Can't read record header at %" PRId64, filePosition);
            break;
        }

        auto* header = reinterpret_cast<TRecordHeader*>(sharedBlob.Begin() + position);
        if (header->RecordId > lastRecordId) {
            break;
        }

        if (header->RecordId >= firstRecordId) {
            if (header->RecordId != currentRecordId) {
                LOG_DEBUG("Invalid record id at %" PRId64 ": expected %d, got %d",
                    filePosition,
                    currentRecordId,
                    header->RecordId);
                break;
            }

            if (position + sizeof(TRecordHeader) + header->DataLength > length) {
                LOG_DEBUG("Can't read data of record %d at %" PRId64,
                    header->RecordId,
                    filePosition);
                break;
            }

            char* ptr = sharedBlob.Begin() + position + sizeof(TRecordHeader);
            auto checksum = GetChecksum(TRef(ptr, header->DataLength));
            if (checksum != header->Checksum) {
                LOG_DEBUG("Invalid checksum of record %d at %" PRId64,
                    header->RecordId,
                    filePosition);
                break;
            }

            result->push_back(TSharedRef(sharedBlob, TRef(ptr, static_cast<size_t>(header->DataLength))));
            ++currentRecordId;
        }

        position += AlignUp(static_cast<i32>(sizeof(TRecordHeader)) + header->DataLength);
    }
}

void TChangeLog::TImpl::Truncate(i32 atRecordId)
{
    TGuard<TMutex> guard(Mutex);

    LOG_DEBUG("Truncating changelog (SegmentId: %d, AtRecordId: %d)",
        Id,
        atRecordId);

    i64 lowerBound, upperBound;
    i32 currentRecordId;

    auto it = GetUpperBound(atRecordId);
    if (it == Index.end()) {
        upperBound = CurrentFilePosition;
    } else {
        upperBound = it->Offset;
    }

    auto itPrev = GetLowerBound(atRecordId);
    currentRecordId = itPrev->RecordId;
    lowerBound = itPrev->Offset;

    if (currentRecordId == atRecordId) {
        TruncateIndex(itPrev - Index.begin());
    } else {
        TruncateIndex(it - Index.begin());
    }

    size_t length = static_cast<size_t>(upperBound - lowerBound);

    TBlob data(length);
    File->Pread(data.begin(), length, lowerBound);

    size_t position = 0;
    while (currentRecordId < atRecordId) {
        i64 filePosition = lowerBound + position;

        // All records before recordId should be ok
        if (position + sizeof(TRecordHeader) >= data.size()) {
            LOG_FATAL("Can't read record header at %" PRId64, filePosition);
        }

        TRecordHeader* header = (TRecordHeader*) &data.at(position);
        if (currentRecordId != header->RecordId) {
            LOG_FATAL("Invalid record id at %" PRId64 ": expected %d, got %d",
                filePosition, currentRecordId, header->RecordId);
        }

        if (position + sizeof(TRecordHeader) + header->DataLength > data.size()) {
            LOG_FATAL("Can't read data of record %d at %" PRId64,
                header->RecordId,
                filePosition);
        }
        ++currentRecordId;

        position += AlignUp(static_cast<i32>(sizeof(TRecordHeader)) + header->DataLength);
    }

    CurrentBlockSize = position;
    CurrentFilePosition = lowerBound + position;
    File->Resize(CurrentFilePosition);
    RecordCount = atRecordId;
}

void TChangeLog::TImpl::HandleRecord(i32 recordId, i32 recordSize)
{
    ++RecordCount;

    CurrentBlockSize += recordSize;
    if (CurrentBlockSize >= IndexBlockSize) {
        CurrentBlockSize = 0;

        bool appendToIndexFile = false;
        i32 indexRecordCount = -1;

        TLogIndexRecord record;
        record.RecordId = recordId;
        record.Offset = static_cast<i32>(CurrentFilePosition);
        if (Index.empty() || Index.back().RecordId != record.RecordId) {
            Index.push_back(record);
            indexRecordCount = Index.ysize();
            appendToIndexFile = true;
        }

        if (appendToIndexFile) {
            try {
                IndexFile->Seek(0, sEnd);
                NYT::Write(*IndexFile, record);
                IndexFile->Seek(0, sSet);
                TLogIndexHeader header(Id, indexRecordCount);
                NYT::Write(*IndexFile, header);
                IndexFile->Flush();
            } catch (...) {
                LOG_FATAL("Error appending to index (SegmentId: %d)\n%s",
                    Id,
                    ~CurrentExceptionMessage());
            }
            LOG_DEBUG("Added record to index (SegmentId: %d, RecordId: %d, Offset: %d)",
                Id,
                record.RecordId,
                record.Offset);
        }
    }

    CurrentFilePosition += recordSize;
    YASSERT(CurrentFilePosition <= Max<i32>());
}

// TODO: What are exact semantics of these two methods?
// Why NStl::LowerBound is unfeasible?
TChangeLog::TImpl::TIndex::iterator TChangeLog::TImpl::GetLowerBound(i32 recordId)
{
    YASSERT(Index.ysize() > 0);
    TLogIndexRecord record;
    record.RecordId = recordId;
    record.Offset = Max<i32>();
    auto it = std::upper_bound(Index.begin(), Index.end(), record);
    --it;
    return it;
}

TChangeLog::TImpl::TIndex::iterator TChangeLog::TImpl::GetUpperBound(i32 recordId)
{
    YASSERT(Index.ysize() > 0);
    TLogIndexRecord record;
    record.RecordId = recordId;
    record.Offset = Max<i32>();
    auto it = std::upper_bound(Index.begin(), Index.end(), record);
    return it;
}

// Should be called under SpinLock
void TChangeLog::TImpl::TruncateIndex(i32 indexRecordId)
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

TChangeLog::TChangeLog(Stroka fileName, i32 id, i32 indexBlockSize)
    : Impl(new TImpl(fileName, id, indexBlockSize))
{ }

i32 TChangeLog::GetId() const
{
    return Impl->GetId();
}

i32 TChangeLog::GetPrevRecordCount() const
{
    return Impl->GetPrevRecordCount();
}

i32 TChangeLog::GetRecordCount() const
{
    return Impl->GetRecordCount();
}

bool TChangeLog::IsFinalized() const
{
    return Impl->IsFinalized();
}

void TChangeLog::Open()
{
    Impl->Open();
}

void TChangeLog::Create(i32 prevRecordCount)
{
    Impl->Create(prevRecordCount);
}

void TChangeLog::Finalize()
{
    Impl->Finalize();
}

void TChangeLog::Append(i32 recordId, TSharedRef recordData)
{
    Impl->Append(recordId, recordData);
}

void TChangeLog::Flush()
{
    Impl->Flush();
}

void TChangeLog::Read(i32 firstRecordId, i32 recordCount, yvector<TSharedRef>* result)
{
    Impl->Read(firstRecordId, recordCount, result);
}

void TChangeLog::Truncate(i32 atRecordId)
{
    Impl->Truncate(atRecordId);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NMetaState
} // namespace NYT
