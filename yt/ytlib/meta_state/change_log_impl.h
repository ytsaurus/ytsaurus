#pragma once

#include "private.h"
#include "file_helpers.h"
#include "change_log.h"

#include <core/misc/serialize.h>
#include <core/misc/checksum.h>
#include <core/logging/tagged_logger.h>

#include <util/generic/noncopyable.h>

namespace NYT {
namespace NMetaState {

////////////////////////////////////////////////////////////////////////////////

// Binary Structures {{{

#pragma pack(push, 4)

struct TLogHeader
{
    //! Used to check correctness of this header.
    static const ui64 CorrectSignature = 0x313030304C435459ull; // YTCL0002

    ui64 Signature;
    int ChangeLogId;
    TEpochId Epoch;
    int PrevRecordCount;
    int Finalized;

    TLogHeader()
        : Signature(0)
        , ChangeLogId(0)
        , Epoch()
        , Finalized(false)
    { }

    TLogHeader(int changeLogId, const TEpochId& epoch, int prevRecordCount, bool finalized)
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
    int RecordIndex;
    int DataLength;
    TChecksum Checksum;

    TRecordHeader()
        : RecordIndex(0)
        , DataLength(0)
        , Checksum(0)
    { }

    TRecordHeader(int recordIndex, int dataLength, TChecksum checksum)
        : RecordIndex(recordIndex)
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
    int ChangeLogId;
    int IndexSize;

    TLogIndexHeader()
        : Signature(0)
        , ChangeLogId(0)
        , IndexSize(0)
    { }

    TLogIndexHeader(int changeLogId, int indexSize)
        : Signature(CorrectSignature)
        , ChangeLogId(changeLogId)
        , IndexSize(indexSize)
    { }
};

static_assert(sizeof(TLogIndexHeader) == 16, "Binary size of TLogIndexHeader has changed.");

struct TLogIndexRecord
{
    i64 FilePosition;
    int RecordIndex;

    TLogIndexRecord(int recordIndex, i64 filePosition):
        FilePosition(filePosition),
        RecordIndex(recordIndex)
    { }

    //! This initializer is necessary only for reading TLogIndexRecord.
    TLogIndexRecord():
        FilePosition(-1),
        RecordIndex(-1)
    { }

    bool operator < (const TLogIndexRecord& other) const
    {
        return RecordIndex < other.RecordIndex;
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
        int id,
        i64 indexBlockSize);

    void Open();
    void Create(int previousRecordCount, const TEpochId& epoch);

    void Append(const std::vector<TSharedRef>&);
    void Append(int firstRecordIndex, const std::vector<TSharedRef>&);
    void Append(const TSharedRef& ref);

    void Flush();
    void Read(
        int firstRecordIndex,
        int recordCount,
        i64 maxSize,
        std::vector<TSharedRef>* records);
    void Truncate(int recordCount);

    void Finalize();
    void Definalize();

    int GetId() const;
    int GetPrevRecordCount() const;
    int GetRecordCount() const;
    const TEpochId& GetEpoch() const;
    bool IsFinalized() const;

private:
    DECLARE_ENUM(EState,
        (Uninitialized)
        (Open)
        (Finalized)
    );

    struct TEnvelopeData
    {
        i64 GetLength() const
        {
            return UpperBound.FilePosition - LowerBound.FilePosition;
        }

        i64 GetStartPosition() const
        {
            return LowerBound.FilePosition;
        }

        i64 GetStartRecordIndex() const
        {
            return LowerBound.RecordIndex;
        }

        i64 GetEndRecordIndex() const
        {
            return UpperBound.RecordIndex;
        }

        TLogIndexRecord LowerBound;
        TLogIndexRecord UpperBound;
        TSharedRef Blob;
    };

    //! Processes currently read or written record to changelog.
    /*! Checks correctness of record id, updates the index, record count,
     *  current block size and current file position.
     */
    void ProcessRecord(int recordIndex, int readSize);

    //! Refresh index header and update current number of records.
    void RefreshIndexHeader();

    //! Reads maximal correct prefix of index, truncate bad index records.
    void ReadIndex();

    //! Reads piece of changelog containing both #firstRecordIndex and #lastRecordIndex.
    TEnvelopeData ReadEnvelope(int firstRecordIndex, int lastRecordIndex);

    //! Reads changelog starting from the last indexed record until the end of file.
    void ReadChangeLogUntilEnd();

    //! Write header
    void WriteHeader(bool finalized);

    //! Constant data.
    const int Id;
    const i64 IndexBlockSize;
    const Stroka FileName;
    const Stroka IndexFileName;

    //! Mutable data.
    EState State;

    int RecordCount;
    i64 CurrentBlockSize;
    i64 CurrentFilePosition;

    //! This is a foreign constraint and it is used to verify integrity of a sequence of changelogs.
    //! \see IMetaState
    int PrevRecordCount;
    TEpochId Epoch;

    std::vector<TLogIndexRecord> Index;

    std::unique_ptr<TBufferedFile> File;
    std::unique_ptr<TFile> IndexFile;

    //! Auxiliary data.
    //! Protects file resources.
    TMutex Mutex;
    NLog::TTaggedLogger Logger;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NMetaState
} // namespace NYT

DECLARE_PODTYPE(NYT::NMetaState::TRecordHeader);
