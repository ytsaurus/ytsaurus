#pragma once

#include "private.h"
#include "sync_file_changelog.h"
#include "file_helpers.h"

#include <core/misc/serialize.h>
#include <core/misc/checksum.h>

#include <core/logging/log.h>

#include <util/system/mutex.h>

namespace NYT {
namespace NHydra {

////////////////////////////////////////////////////////////////////////////////

// Binary Structures {{{

#pragma pack(push, 4)

struct TChangelogHeader
{
    //! Used for format validation.
    static const ui64 ExpectedSignature;

    //! Indicates that the changelog is not yet sealed.
    static const i32 UnsealedRecordCount = -2;

    ui64 Signature;
    i32 HeaderSize; // with padding
    i32 MetaSize;
    i32 SealedRecordCount;
    i32 Padding = 0;

    TChangelogHeader()
        : Signature(-1)
        , HeaderSize(-1)
        , MetaSize(-1)
    { }

    TChangelogHeader(
        int metaSize,
        int sealedRecordCount)
        : Signature(ExpectedSignature)
        , HeaderSize(sizeof (TChangelogHeader) + AlignUp(metaSize))
        , MetaSize(metaSize)
        , SealedRecordCount(sealedRecordCount)
    { }
};

static_assert(sizeof (TChangelogHeader) == 24, "Binary size of TChangelogHeader has changed.");

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

static_assert(sizeof (TChangelogRecordHeader) == 16, "Binary size of TChangelogRecordHeader has changed.");

////////////////////////////////////////////////////////////////////////////////

struct TChangelogIndexHeader
{
    //! Used for format validation.
    static const ui64 ExpectedSignature;
    
    ui64 Signature;
    i32 IndexRecordCount;

    TChangelogIndexHeader()
        : Signature(0)
        , IndexRecordCount(-1)
    { }

    explicit TChangelogIndexHeader(int indexRecordCount)
        : Signature(ExpectedSignature)
        , IndexRecordCount(indexRecordCount)
    { }
};

static_assert(sizeof(TChangelogIndexHeader) == 12, "Binary size of TChangelogIndexHeader has changed.");

////////////////////////////////////////////////////////////////////////////////

struct TChangelogIndexRecord
{
    i64 FilePosition;
    i32 RecordId;
    i32 Padding;

    //! This initializer is only needed to read TLogIndexRecord.
    TChangelogIndexRecord()
        : FilePosition(-1)
        , RecordId(-1)
        , Padding(0)
    { }

    TChangelogIndexRecord(int recordIndex, i64 filePosition)
        : FilePosition(filePosition)
        , RecordId(recordIndex)
        , Padding(0)
    { }
};

static_assert(sizeof(TChangelogIndexRecord) == 16, "Binary size of TLogIndexRecord has changed.");

#pragma pack(pop)

////////////////////////////////////////////////////////////////////////////////

// }}}

//! Implementation of TChangeLog.
class TSyncFileChangelog::TImpl
{
public:
    TImpl(
        const Stroka& fileName,
        TFileChangelogConfigPtr config);

    TFileChangelogConfigPtr GetConfig() const;
    const Stroka& GetFileName() const;

    void Open();
    void Close();
    void Create(const TSharedRef& meta);

    TSharedRef GetMeta() const;
    int GetRecordCount() const;
    i64 GetDataSize() const;
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

    void CreateIndexFile();

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

    //! Reads the maximal valid prefix of index, truncates bad index records.
    void ReadIndex(const TChangelogHeader& header);

    //! Reads piece of changelog containing both #firstRecordId and #lastRecordId.
    TEnvelopeData ReadEnvelope(int firstRecordId, int lastRecordId, i64 maxBytes = -1);

    //! Reads changelog starting from the last indexed record until the end of file.
    void ReadChangelogUntilEnd(const TChangelogHeader& header);

    const Stroka FileName_;
    const Stroka IndexFileName_;
    const TFileChangelogConfigPtr Config_;

    bool Open_;
    int RecordCount_;
    int SealedRecordCount_;
    i64 CurrentBlockSize_;
    i64 CurrentFilePosition_;
    TInstant LastFlushed_;

    TSharedRef Meta_;
    
    std::vector<TChangelogIndexRecord> Index_;

    std::unique_ptr<TBufferedFile> DataFile_;
    std::unique_ptr<TFile> IndexFile_;

    //! Auxiliary data.
    //! Protects file resources.
    TMutex Mutex_;
    NLog::TLogger Logger;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NHydra
} // namespace NYT

DECLARE_PODTYPE(NYT::NHydra::TChangelogHeader);
DECLARE_PODTYPE(NYT::NHydra::TChangelogRecordHeader);
DECLARE_PODTYPE(NYT::NHydra::TChangelogIndexHeader);
DECLARE_PODTYPE(NYT::NHydra::TChangelogIndexRecord);
