#pragma once

#include "common.h"
#include "file_helpers.h"
#include "change_log.h"

#include <ytlib/misc/serialize.h>
#include <ytlib/misc/checksum.h>
#include <ytlib/logging/tagged_logger.h>

#include <util/generic/noncopyable.h>

namespace NYT {
namespace NMetaState {

// Binary Structures {{{

#pragma pack(push, 4)

struct TLogHeader
{
    //! Used to check correctness of this header.
    static const ui64 CorrectSignature = 0x313030304C435459ull; // YTCL0002

    ui64 Signature;
    i32 ChangeLogId;
    TEpoch Epoch;
    i32 PrevRecordCount;
    bool Finalized;

    TLogHeader()
        : Signature(0)
        , ChangeLogId(0)
        , Epoch()
        , Finalized(false)
    { }

    TLogHeader(i32 changeLogId, const TEpoch& epoch, i32 prevRecordCount, bool finalized)
        : Signature(CorrectSignature)
        , ChangeLogId(changeLogId)
        , Epoch(epoch)
        , PrevRecordCount(prevRecordCount)
        , Finalized(finalized)
    { }
};

static_assert(sizeof(TLogHeader) == 36, "Binary size of TLogHeader has changed.");

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
    //! Used to check correctness of this header.
    static const ui64 CorrectSignature = 0x31303030494C5459ull; // YTLI0001

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
    void Create(i32 previousRecordCount, const TEpoch& epoch);

    void Append(const std::vector<TSharedRef>&);
    void Append(i32 firstRecordId, const std::vector<TSharedRef>&);
    void Append(const TSharedRef& ref);

    void Flush();
    void Read(i32 firstRecordId, i32 recordCount, std::vector<TSharedRef>* records);
    void Truncate(i32 atRecordId);

    void Finalize();

    i32 GetId() const;
    i32 GetPrevRecordCount() const;
    i32 GetRecordCount() const;
    const TEpoch& GetEpoch() const;
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

    //! Processes currently read or written record to changelog.
    /*! Checks correctness of record id, updates the index, record count,
     *  current block size and current file position.
     */
    void ProcessRecord(i32 recordId, i32 readSize);

    //! Refresh index header and update current number of records.
    void RefreshIndexHeader();

    //! Reads maximal correct prefix of index, truncate bad index records.
    void ReadIndex();

    //! Reads piece of changelog that contains firstRecordId and lastRecordId.
    TEnvelopeData ReadEnvelope(i32 firstRecordId, i32 lastRecordId);

    //! Reads changelog starting from the last indexed record until the end of file.
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
    TEpoch Epoch;

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
