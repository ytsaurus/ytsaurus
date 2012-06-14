#include "stdafx.h"
#include "snapshot.h"
#include "common.h"

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

} // namespace

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger& Logger = MetaStateLogger;

////////////////////////////////////////////////////////////////////////////////

#pragma pack(push, 4)

struct TSnapshotHeader
{
    static const ui64 CorrectSignature =  0x3130303053535459ull; // YTSS0002

    ui64 Signature;
    i32 SegmentId;
    TEpoch Epoch;
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

    TSnapshotHeader(i32 segmentId, i32 prevRecordCount)
        : Signature(CorrectSignature)
        , SegmentId(segmentId)
        , PrevRecordCount(prevRecordCount)
        , DataLength(0)
        , Checksum(0)
    { }

    void Validate() const
    {
        if (Signature != CorrectSignature) {
            LOG_FATAL("Invalid signature: expected %" PRIx64 ", found %" PRIx64,
                CorrectSignature,
                Signature);
        }
    }
};

static_assert(sizeof(TSnapshotHeader) == 48, "Binary size of TSnapshotHeader has changed.");

#pragma pack(pop)

////////////////////////////////////////////////////////////////////////////////

TSnapshotReader::TSnapshotReader(
    const Stroka& fileName,
    i32 segmentId,
    bool enableCompression)
    : FileName(fileName)
    , SnapshotId(segmentId)
    , EnableCompression(enableCompression)
{ }

void TSnapshotReader::Open()
{
    LOG_DEBUG("Opening snapshot reader %s", ~FileName);
    File.Reset(new TFile(FileName, OpenExisting));
    
    TSnapshotHeader header;
    ReadPod(*File, header);
    header.Validate();
    LOG_FATAL_UNLESS(header.SegmentId == SnapshotId,
        "Invalid snapshot id in header: expected %d, got %d",
        SnapshotId,
        header.SegmentId);
    PrevRecordCount = header.PrevRecordCount;
    Checksum = header.Checksum;
    YASSERT(header.DataLength + sizeof(header) == static_cast<ui64>(File->GetLength()));

    FileInput.Reset(new TBufferedFileInput(*File));

    if (EnableCompression) {
        DecompressedInput.Reset(new TDecompressedInput(~FileInput));
        ChecksummableInput.Reset(new TChecksummableInput(~DecompressedInput));
    } else {
        ChecksummableInput.Reset(new TChecksummableInput(~FileInput));
    }
}

TInputStream* TSnapshotReader::GetStream() const
{
    YASSERT(~ChecksummableInput);
    return ~ChecksummableInput;
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

TSnapshotWriter::TSnapshotWriter(
    const Stroka& fileName,
    i32 segmentId,
    bool enableCompression)
    : FileName(fileName)
    , TempFileName(fileName + NFS::TempFileSuffix)
    , SnapshotId(segmentId)
    , EnableCompression(enableCompression)
    , PrevRecordCount(0)
    , Checksum(0)
{ }

void TSnapshotWriter::Open(i32 prevRecordCount)
{
    PrevRecordCount = prevRecordCount;
    Close();

    File.Reset(new TFile(TempFileName, RdWr | CreateAlways));
    FileOutput.Reset(new TBufferedFileOutput(*File));

    TSnapshotHeader header(SnapshotId, PrevRecordCount);
    WritePod(*FileOutput, header);

    if (EnableCompression) {
        CompressedOutput.Reset(new TCompressedOutput(~FileOutput));
        ChecksummableOutput.Reset(new TChecksummableOutput(~CompressedOutput));
    } else {
        ChecksummableOutput.Reset(new TChecksummableOutput(~FileOutput));
    }
    
    Checksum = 0;
}

TOutputStream* TSnapshotWriter::GetStream() const
{
    YASSERT(~ChecksummableOutput);
    return ~ChecksummableOutput;
}

void TSnapshotWriter::Close()
{
    if (!FileOutput)
        return;

    if (~ChecksummableOutput) {
        Checksum = ChecksummableOutput->GetChecksum();
        ChecksummableOutput->Flush();
        ChecksummableOutput.Reset(NULL);
        CompressedOutput.Reset(NULL);
    }

    FileOutput->Flush();
    FileOutput.Reset(NULL);

    TSnapshotHeader header(SnapshotId, PrevRecordCount);
    header.DataLength = File->GetLength() - sizeof(TSnapshotHeader);
    header.Checksum = Checksum;

    File->Seek(0, sSet);
    File->Write(&header, sizeof(header));
    File->Flush();
    File->Close();
    File.Reset(NULL);

    // TODO(ignat): change exception to YCHECK
    if (isexist(~FileName)) {
        if (!NFS::Remove(~FileName)) {
            ythrow yexception() << Sprintf("Error removing %s", ~FileName.Quote());
        }
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
