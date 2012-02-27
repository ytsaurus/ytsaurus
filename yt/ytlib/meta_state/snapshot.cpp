#include "stdafx.h"
#include "snapshot.h"

#include <ytlib/misc/common.h>
#include <ytlib/misc/fs.h>
#include <ytlib/misc/serialize.h>

#include <util/folder/dirut.h>
#include <util/stream/lz.h>

namespace NYT {
namespace NMetaState {

////////////////////////////////////////////////////////////////////////////////

namespace {

typedef TSnappyCompress TCompressedOutput;
typedef TSnappyDecompress TDecompressedInput;

} // namespace <anonymous>

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger& Logger = MetaStateLogger;

////////////////////////////////////////////////////////////////////////////////

#pragma pack(push, 4)

struct TSnapshotHeader
{
    static const ui64 CurrentSignature =  0x3130303053535459ull; // YTSS0001

    ui64 Signature;
    i32 SegmentId;
    i32 PrevRecordCount;
    ui64 DataLength;
    ui64 Checksum;

    TSnapshotHeader()
        : Signature(0)
        , SegmentId(0)
        , PrevRecordCount(0)
        , DataLength(0)
        , Checksum(0)
    { }

    explicit TSnapshotHeader(i32 segmentId, i32 prevRecordCount)
        : Signature(CurrentSignature)
        , SegmentId(segmentId)
        , PrevRecordCount(prevRecordCount)
        , DataLength(0)
        , Checksum(0)
    { }

    void Validate() const
    {
        if (Signature != CurrentSignature) {
            LOG_FATAL("Invalid signature: expected %" PRIx64 ", found %" PRIx64,
                CurrentSignature,
                Signature);
        }
    }
};

#pragma pack(pop)

////////////////////////////////////////////////////////////////////////////////

TSnapshotReader::TSnapshotReader(const Stroka& fileName, i32 segmentId)
    : FileName(fileName)
    , SegmentId(segmentId)
{ }

void TSnapshotReader::Open()
{
    LOG_DEBUG("Opening snapshot reader %s", ~FileName);
    File.Reset(new TFile(FileName, OpenExisting));
    
    TSnapshotHeader header;
    Read(*File, &header);
    header.Validate();
    if (header.SegmentId != SegmentId) {
        LOG_FATAL("Invalid snapshot id in header: expected %d, got %d",
            SegmentId,
            header.SegmentId);
    }
    PrevRecordCount = header.PrevRecordCount;
    Checksum = header.Checksum;
    YASSERT(header.DataLength + sizeof(header) == static_cast<ui64>(File->GetLength()));

    FileInput.Reset(new TBufferedFileInput(*File));
    DecompressedInput.Reset(new TDecompressedInput(~FileInput));
    ChecksummableInput.Reset(new TChecksummableInput(*DecompressedInput));
}

TInputStream& TSnapshotReader::GetStream() const
{
    YASSERT(~ChecksummableInput);
    return *ChecksummableInput;
}

i64 TSnapshotReader::GetLength() const
{
    return File->GetLength();
}

TChecksum TSnapshotReader::GetChecksum() const
{
    // TODO: check that checksum is available
    return Checksum;
}

i32 TSnapshotReader::GetPrevRecordCount() const
{
    return PrevRecordCount;
}

////////////////////////////////////////////////////////////////////////////////

TSnapshotWriter::TSnapshotWriter(Stroka fileName, i32 segmentId)
    : FileName(fileName)
    , TempFileName(fileName + NFS::TempFileSuffix)
    , SegmentId(segmentId)
    , PrevRecordCount(0)
    , Checksum(0)
{ }

void TSnapshotWriter::Open(i32 prevRecordCount)
{
    // Do not use logging here. This method is used in forked process.

    PrevRecordCount = prevRecordCount;
    Close();

    //LOG_DEBUG("Opening snapshot %s for writing", ~TempFileName.Quote());
    File.Reset(new TFile(TempFileName, RdWr | CreateAlways));
    FileOutput.Reset(new TBufferedFileOutput(*File));

    TSnapshotHeader header(SegmentId, PrevRecordCount);
    Write(*FileOutput, header);

    CompressedOutput.Reset(new TCompressedOutput(~FileOutput));
    ChecksummableOutput.Reset(new TChecksummableOutput(*CompressedOutput));
    
    Checksum = 0;
}

TOutputStream& TSnapshotWriter::GetStream() const
{
    YASSERT(~ChecksummableOutput);©≠ош
    return *ChecksummableOutput;
}

void TSnapshotWriter::Close()
{
    // Do not use logging here. This method is used in forked process.

    if (!FileOutput)
        return;

    //LOG_DEBUG("Closing snapshot %s", ~TempFileName.Quote());

    if (~ChecksummableOutput) {
        Checksum = ChecksummableOutput->GetChecksum();
        ChecksummableOutput->Flush();
        ChecksummableOutput.Reset(NULL);
        CompressedOutput.Reset(NULL);
    }

    FileOutput->Flush();
    FileOutput.Reset(NULL);

    TSnapshotHeader header(SegmentId, PrevRecordCount);
    header.DataLength = File->GetLength() - sizeof(TSnapshotHeader);
    header.Checksum = Checksum;

    File->Seek(0, sSet);
    File->Write(&header, sizeof(header));
    File->Flush();
    File->Close();
    File.Reset(NULL);

    if (isexist(~FileName)) {
        if (!NFS::Remove(~FileName)) {
            ythrow yexception() << Sprintf("Error removing %s", ~FileName.Quote());
        }
        //LOG_WARNING("File %s already existed and was deleted", ~FileName.Quote());
    }

    if (!NFS::Rename(~TempFileName, ~FileName)) {
        ythrow yexception() << Sprintf("Error renaming %s to %s",
            ~TempFileName.Quote(),
            ~FileName.Quote());
    }
}

TChecksum TSnapshotWriter::GetChecksum() const
{
    // TODO: check that checksum is available.
    return Checksum;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NMetaState
} // namespace NYT
