#pragma once

#include "private.h"
#include "sync_file_changelog.h"
#include "file_helpers.h"

#include <core/misc/serialize.h>
#include <core/misc/checksum.h>

#include <core/logging/tagged_logger.h>

namespace NYT {
namespace NHydra {

////////////////////////////////////////////////////////////////////////////////

// Binary Structures {{{

#pragma pack(push, 4)

struct TChangelogHeader
{
    //! Used for format validation.
    static const ui64 ExpectedSignature = 0x323030304C435459ull; // YTCL0003

    //! Indicates that the changelog is not yet sealed.
    static const i32 NotSealedRecordCount = -1;

    ui64 Signature;
    i32 ChangelogId;
    i32 PrevRecordCount;
    i32 SealedRecordCount;

    TChangelogHeader()
        : Signature(-1)
        , ChangelogId(-1)
    { }

    TChangelogHeader(
        int changelogId,
        int prevRecordCount,
        int sealedRecordCount)
        : Signature(ExpectedSignature)
        , ChangelogId(changelogId)
        , PrevRecordCount(prevRecordCount)
        , SealedRecordCount(sealedRecordCount)
    { }
};

static_assert(sizeof(TChangelogHeader) == 20, "Binary size of TLogHeader has changed.");

////////////////////////////////////////////////////////////////////////////////

struct TChangelogRecordHeader
{
    i32 RecordId;
    i32 DataSize;
    TChecksum Checksum;

    TChangelogRecordHeader()
        : RecordId(-1)
        , DataSize(-1)
        , Checksum(0)
    { }

    TChangelogRecordHeader(
        int recordIndex,
        int dataLength,
        TChecksum checksum)
        : RecordId(recordIndex)
        , DataSize(dataLength)
        , Checksum(checksum)
    { }
};

static_assert(sizeof(TChangelogRecordHeader) == 16, "Binary size of TRecordHeader has changed.");

////////////////////////////////////////////////////////////////////////////////

struct TChangelogIndexHeader
{
    //! Used for format validation.
    static const ui64 ExpectedSignature = 0x32303030494C5459ull; // YTLI0002

    ui64 Signature;
    int ChangeLogId;
    int IndexSize;

    TChangelogIndexHeader()
        : Signature(0)
        , ChangeLogId(-1)
        , IndexSize(-1)
    { }

    TChangelogIndexHeader(
        int changeLogId,
        int indexSize)
        : Signature(ExpectedSignature)
        , ChangeLogId(changeLogId)
        , IndexSize(indexSize)
    { }
};

static_assert(sizeof(TChangelogIndexHeader) == 16, "Binary size of TLogIndexHeader has changed.");

////////////////////////////////////////////////////////////////////////////////

struct TChangelogIndexRecord
{
    i64 FilePosition;
    i32 RecordId;

    //! This initializer is only needed to read TLogIndexRecord.
    TChangelogIndexRecord()
        : FilePosition(-1)
        , RecordId(-1)
    { }

    TChangelogIndexRecord(int recordIndex, i64 filePosition)
        : FilePosition(filePosition)
        , RecordId(recordIndex)
    { }


    bool operator < (const TChangelogIndexRecord& other) const
    {
        return RecordId < other.RecordId;
    }
};

static_assert(sizeof(TChangelogIndexRecord) == 12, "Binary size of TLogIndexRecord has changed.");

#pragma pack(pop)

////////////////////////////////////////////////////////////////////////////////

// }}}

//! Implementation of TChangeLog.
class TSyncFileChangelog::TImpl
{
public:
    TImpl(
        const Stroka& fileName,
        int id,
        TFileChangelogConfigPtr config);

    TFileChangelogConfigPtr GetConfig();

    void Open();
    void Close();
    void Create(const TChangelogCreateParams& params);

    int GetId() const;
    int GetRecordCount() const;
    int GetPrevRecordCount() const;
    bool IsSealed() const;

    void Append(
        int firstRecordId,
        const std::vector<TSharedRef>& records);

    void Flush();

    TInstant GetLastFlushed();

    std::vector<TSharedRef> Read(
        int firstRecordId,
        int maxRecords,
        i64 maxBytes);

    void Seal(int recordCount);
    void Unseal();

private:
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

        i64 GetStartRecordId() const
        {
            return LowerBound.RecordId;
        }

        i64 GetEndRecordId() const
        {
            return UpperBound.RecordId;
        }

        TChangelogIndexRecord LowerBound;
        TChangelogIndexRecord UpperBound;
        TSharedRef Blob;
    };

    void DoAppend(const TRef& record);
    void DoAppend(const std::vector<TSharedRef>& records);

    //! Processes currently read or written record to changelog.
    /*! Checks correctness of record id, updates the index, record count,
     *  current block size and current file position.
     */
    void ProcessRecord(int recordId, int readSize);

    //! Rewrites changelog header.
    void UpdateLogHeader();

    //! Rewrites index header.
    void UpdateIndexHeader();

    //! Reads maximal correct prefix of index, truncate bad index records.
    void ReadIndex();

    //! Reads piece of changelog containing both #firstRecordId and #lastRecordId.
    TEnvelopeData ReadEnvelope(int firstRecordId, int lastRecordId);

    //! Reads changelog starting from the last indexed record until the end of file.
    void ReadChangelogUntilEnd();

    const int Id_;
    const Stroka FileName_;
    const Stroka IndexFileName_;
    const TFileChangelogConfigPtr Config_;

    bool IsOpen_;
    bool IsSealed_;
    int RecordCount_;
    i64 CurrentBlockSize_;
    i64 CurrentFilePosition_;
    TInstant LastFlushed_;

    //! Used to verify integrity of a sequence of changelogs.
    int PrevRecordCount_;

    std::vector<TChangelogIndexRecord> Index_;

    std::unique_ptr<TBufferedFile> LogFile_;
    std::unique_ptr<TFile> IndexFile_;

    //! Auxiliary data.
    //! Protects file resources.
    TMutex Mutex_;
    NLog::TTaggedLogger Logger;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NHydra
} // namespace NYT

DECLARE_PODTYPE(NYT::NHydra::TChangelogRecordHeader);
