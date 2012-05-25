#pragma once

#include "common.h"
#include "change_log.h"

#include <ytlib/misc/buffered_file.h>
#include <ytlib/misc/serialize.h>
#include <ytlib/misc/checksum.h>
#include <ytlib/logging/tagged_logger.h>

#include <util/generic/noncopyable.h>

namespace NYT {
namespace NMetaState {

// Binary Structures {{{

//! Signature is used in TLogHeader and TIndexHeader
//! for checking their correctness.
static const ui64 CorrectSignature = 0x31303030494C5459ull; // YTLI0001

//! Align all data structures by 4 bytes for gain
//! independence from compiler and platform alignment.
#pragma pack(push, 4)

struct TLogHeader
{
    ui64 Signature;
    i32 ChangeLogId;
    i32 PrevRecordCount;
    bool Finalized;

    TLogHeader()
        : Signature(0)
        , ChangeLogId(0)
        , Finalized(false)
    { }

    TLogHeader(i32 changeLogId, i32 prevRecordCount, bool finalized)
        : Signature(CorrectSignature)
        , ChangeLogId(changeLogId)
        , PrevRecordCount(prevRecordCount)
        , Finalized(finalized)
    { }
};

static_assert(sizeof(TLogHeader) == 20, "Binary size of TLogHeader has changed.");

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

    TRecordHeader(i32 recordId, i32 dataLength, TChecksum checksum)
        : RecordId(recordId)
        , DataLength(dataLength)
        , Checksum(checksum)
    { }
};

static_assert(sizeof(TRecordHeader) == 16, "Binary size of TRecordHeader has changed.");

struct TLogIndexHeader
{
    ui64 Signature;
    i32 ChangeLogId;
    i32 IndexSize;

    TLogIndexHeader()
        : Signature(0)
        , ChangeLogId(0)
        , IndexSize(0)
    { }

    TLogIndexHeader(i32 changeLogId, i32 indexSize)
        : Signature(CorrectSignature)
        , ChangeLogId(changeLogId)
        , IndexSize(indexSize)
    { }
};

static_assert(sizeof(TLogIndexHeader) == 16, "Binary size of TLogIndexHeader has changed.");

struct TLogIndexRecord
{
    i64 FilePosition;
    i32 RecordId;

    TLogIndexRecord(i32 recordId, i64 filePosition):
        FilePosition(filePosition),
        RecordId(recordId)
    { }

    //! This initializer is necessary only for reading TLogIndexRecord.
    TLogIndexRecord():
        FilePosition(-1),
        RecordId(-1)
    { }

    bool operator < (const TLogIndexRecord& other) const
    {
        return RecordId < other.RecordId;
    }
};

static_assert(sizeof(TLogIndexRecord) == 12, "Binary size of TLogIndexRecord has changed.");

#pragma pack(pop)

////////////////////////////////////////////////////////////////////////////////

// }}}

//! Implementation of TChangeLog.
class TChangeLog::TImpl
    : private ::TNonCopyable
{
public:
    TImpl(
        const Stroka& fileName,
        i32 id,
        i64 indexBlockSize);

    void Open();
    //! Creates changelog atomically
    void Create(i32 previousRecordCount);
    void Append(const std::vector<TSharedRef>&);
    void Append(i32 firstRecordId, const std::vector<TSharedRef>&);
    void Finalize();
    void Flush();
    //! Deletes all records with id greater or equal than atRecordId.
    void Truncate(i32 atRecordId);
    void Read(i32 firstRecordId, i32 recordCount, std::vector<TSharedRef>* records);

    i32 GetId() const;
    i32 GetPrevRecordCount() const;
    i32 GetRecordCount() const;
    bool IsFinalized() const;

private:
    DECLARE_ENUM(EState,
        (Uninitialized)
        (Open)
        (Finalized)
    );

    struct TEnvelopeData
    {
        i64 Length() const
        {
            return UpperBound.FilePosition - LowerBound.FilePosition;
        }

        i64 StartPosition() const
        {
            return LowerBound.FilePosition;
        }

        i64 StartRecordId() const
        {
            return LowerBound.RecordId;
        }

        i64 EndRecordId() const
        {
            return UpperBound.RecordId;
        }

        TLogIndexRecord LowerBound;
        TLogIndexRecord UpperBound;
        TSharedRef Blob;
   };

    //! Append one record without Mutex.
    void Append(const TSharedRef& ref);

    //! Processes currently read or written record to changelog.
    /*! Checks correctness of record id, updates the index, record count,
     *  current block size and current file position.
     */
    void ProcessRecord(i32 recordId, i32 readSize);

    //! Refresh index header by current number of records.
    void RefreshIndexHeader();
    //! Reads maximal correct prefix of index, truncate bad index records.
    void ReadIndex();

    //! Reads piece of changelog that contains firstRecordId and lastRecordId.
    TEnvelopeData ReadEnvelope(i32 firstRecordId, i32 lastRecordId);

    //! Upreads changelog from the end of the current index.
    void ReadChangeLogUntilEnd();

    //! Constant data.
    const i32 Id;
    const i64 IndexBlockSize;
    const Stroka FileName;
    const Stroka IndexFileName;

    //! Mutable data.
    EState State;

    i32 RecordCount;
    i64 CurrentBlockSize;
    i64 CurrentFilePosition;

    //! This is a foreign constraint and it is used to verify integrity of a sequence of changelogs.
    //! \see IMetaState
    i32 PrevRecordCount;

    std::vector<TLogIndexRecord> Index;

    THolder<TBufferedFile> File;
    THolder<TFile> IndexFile;

    //! Auxiliary data.
    //! Protects file resources.
    TMutex Mutex;
    NLog::TTaggedLogger Logger;
}; // class TChangeLog::TImpl

////////////////////////////////////////////////////////////////////////////////

} // namespace NMetaState
} // namespace NYT
