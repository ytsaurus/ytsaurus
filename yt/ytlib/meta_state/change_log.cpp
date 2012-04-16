#include "stdafx.h"
#include "change_log.h"
#include "common.h"

#include <ytlib/misc/serialize.h>
#include <ytlib/misc/checksum.h>
#include <ytlib/logging/log.h>
#include <ytlib/logging/tagged_logger.h>

#include <algorithm>
#include <util/generic/noncopyable.h>
#include <util/digest/murmur.h>

namespace NYT {
namespace NMetaState {

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger& Logger = MetaStateLogger;
static const char* const IndexSuffix = ".index";

////////////////////////////////////////////////////////////////////////////////

//! Implementation of TChangeLog.
//! \cond Implementation
class TChangeLog::TImpl
    : private ::TNonCopyable
{
public:
    TImpl(
        const Stroka& fileName,
        i32 id,
        bool disableFlush,
        i64 indexBlockSize);

    void Open();
    void Create(i32 previousRecordCount);
    void Finalize();

    void Append(i32 firstRecordId, const yvector<TSharedRef>& records);
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

    static_assert(sizeof(TLogHeader) == 20, "Binary size of TLogHeader has changed.");

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

    static_assert(sizeof(TRecordHeader) == 16, "Binary size of TRecordHeader has changed.");

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

    static_assert(sizeof(TLogIndexHeader) == 16, "Binary size of TLogIndexHeader has changed.");

    #pragma pack(pop)

    ////////////////////////////////////////////////////////////////////////////////

    #pragma pack(push, 4)

    struct TLogIndexRecord
    {
        i64 Offset;
        i32 RecordId;
        i32 Padding;

        TLogIndexRecord()
            : Offset(-1)
            , RecordId(-1)
            , Padding(0)
        { }

        bool operator<(const TLogIndexRecord& other) const
        {
            return RecordId < other.RecordId;
        }
    };

    static_assert(sizeof(TLogIndexRecord) == 16, "Binary size of TLogIndexRecord has changed.");

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
    bool DisableFlush;
    i64 IndexBlockSize;

    i32 RecordCount;
    i32 PrevRecordCount;
    i64 CurrentBlockSize;
    i64 CurrentFilePosition;

    TMutex Mutex;
    TIndex Index;
    THolder<TFile> File;
    THolder<TBufferedFileOutput> FileOutput;

    THolder<TFile> IndexFile;

    NLog::TTaggedLogger Logger;
}; // class TChangeLog::TImpl
//! \endcond

////////////////////////////////////////////////////////////////////////////////

TChangeLog::TImpl::TImpl(
    const Stroka& fileName,
    i32 id,
    bool disableFlush,
    i64 indexBlockSize)
    : State(EState::Closed)
    , FileName(fileName)
    , IndexFileName(fileName + IndexSuffix)
    , Id(id)
    , DisableFlush(disableFlush)
    , IndexBlockSize(indexBlockSize)
    , PrevRecordCount(-1)
    , CurrentBlockSize(-1)
    , CurrentFilePosition(-1)
    , RecordCount(-1)
    , Logger(MetaStateLogger)
{
    Logger.AddTag(Sprintf("ChangeLogId: %d", Id));
}

void TChangeLog::TImpl::Open()
{
    YASSERT(State == EState::Closed);

    LOG_DEBUG("Opening changelog (FileName: %s)",
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
        LOG_DEBUG("Opening changelog index (RecordCount: %d)",
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
            LOG_WARNING("Cannot read changelog record header (RecordId: %d, Offset: %" PRId64 ")",
                RecordCount,
                CurrentFilePosition);
            break;
        }

        i32 recordId = recordHeader.RecordId;
        if (RecordCount != recordId) {
            LOG_ERROR("Invalid record id (Offset: %" PRId64 ", ExpectedId: %d, FoundId: %d)",
                CurrentFilePosition,
                RecordCount,
                recordHeader.RecordId);
            break;
        }

        i32 recordSisze = AlignUp(static_cast<i32>(sizeof(recordHeader)) + recordHeader.DataLength);
        if (CurrentFilePosition + recordSisze > fileLength) {
            LOG_WARNING("Cannot read changelog record data (RecordId: %d, Offset: %" PRId64 ")",
                recordId,
                CurrentFilePosition);
            break;
        }

        buffer.resize(static_cast<size_t>(recordSisze) - sizeof(recordHeader));
        fileInput.Load(buffer.begin(), buffer.size());
        void* ptr = (void *) buffer.begin();
        TChecksum checksum = GetChecksum(TRef(ptr, (size_t) recordHeader.DataLength));
        if (checksum != recordHeader.Checksum) {
            LOG_ERROR("Invalid changelog record checksum (RecordId: %d, Offset: %" PRId64 ")",
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

    LOG_DEBUG("Changelog opened (RecordCount: %d, Finalized: %d)",
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

    LOG_DEBUG("Changelog created");
}

void TChangeLog::TImpl::Finalize()
{
    TGuard<TMutex> guard(Mutex);

    if (State == EState::Finalized)
        return;

    YASSERT(State == EState::Open);

    Flush();

    File->Seek(0, sSet);
    TLogHeader header(Id, PrevRecordCount, true);
    NYT::Write(*File, header);
    File->Flush();

    State = EState::Finalized;

    LOG_DEBUG("Changelog finalized");
}

void TChangeLog::TImpl::Append(i32 firstRecordId, const yvector<TSharedRef>& records)
{
    // Make a coarse check first...
    YASSERT(State == EState::Open || State == EState::Finalized);

    // ... and handle finalized changelogs next.
    if (State == EState::Finalized) {
        LOG_FATAL("Unable to append to a finalized changelog");
    }

    if (firstRecordId != RecordCount) {
        LOG_FATAL("Unexpected record id in changelog (expected: %d, got: %d)",
            RecordCount,
            firstRecordId);
    }

    i32 recordCount = records.ysize();
    yvector<TChecksum> checksums(recordCount);
    for (int i = 0; i < recordCount; ++i) {
        checksums[i] = GetChecksum(records[i]);
    }

    {
        TGuard<TMutex> guard(Mutex);

        for (int i = 0; i < recordCount; ++i) {
            i32 recordId = firstRecordId + i;
            const auto& recordData = records[i];

            i32 recordSize = 0;
            TRecordHeader header(recordId, recordData.Size());
            header.Checksum = checksums[i];

            Write(*FileOutput, header);
            recordSize += sizeof(header);
        
            FileOutput->Write(recordData.Begin(), recordData.Size());
            recordSize += recordData.Size();

            WritePadding(*FileOutput, recordSize);
            recordSize = AlignUp(recordSize);

            HandleRecord(recordId, recordSize);

            ++recordId;
        }
    }

    LOG_DEBUG("Changelog records added (FirstRecordId: %d, RecordCount: %d)",
        firstRecordId,
        records.ysize());
}

void TChangeLog::TImpl::Flush()
{
    if (DisableFlush)
        return;

    LOG_DEBUG("Changelog flush started");

    {
        TGuard<TMutex> guard(Mutex);
        FileOutput->Flush();
        File->Flush();
        IndexFile->Flush();
    }

    LOG_DEBUG("Changelog flush completed");
}

void TChangeLog::TImpl::Read(i32 firstRecordId, i32 recordCount, yvector<TSharedRef>* result)
{
    YASSERT(firstRecordId >= 0);
    YASSERT(recordCount >= 0);
    YASSERT(result);

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
            LOG_DEBUG("Can't read record header (Offset: %" PRId64 ")",
                filePosition);
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

    YASSERT(State == EState::Open);

    LOG_DEBUG("Truncating changelog (RecordId: %d)", atRecordId);

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
            } catch (const std::exception& ex) {
                LOG_FATAL("Error appending to index\n%s", ex.what());
            }
            LOG_DEBUG("Changelog record is added to index (RecordId: %d, Offset: %" PRId64 ")",
                record.RecordId,
                record.Offset);
        }
    }

    CurrentFilePosition += recordSize;
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

TChangeLog::TChangeLog(
    const Stroka& fileName,
    i32 id,
    bool disableFlush,
    i64 indexBlockSize)
    : Impl(new TImpl(
        fileName,
        id,
        disableFlush,
        indexBlockSize))
{ }

TChangeLog::~TChangeLog()
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

void TChangeLog::Append(i32 firstRecordId, const yvector<TSharedRef>& records)
{
    Impl->Append(firstRecordId, records);
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
