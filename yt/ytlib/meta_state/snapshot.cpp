#include "stdafx.h"
#include "snapshot.h"
#include "private.h"

#include <ytlib/misc/fs.h>
#include <ytlib/misc/common.h>
#include <ytlib/misc/serialize.h>

#include <util/stream/lz.h>

namespace NYT {
namespace NMetaState {

////////////////////////////////////////////////////////////////////////////////

namespace {

typedef TSnappyCompress TCompressedOutput;
typedef TSnappyDecompress TDecompressedInput;

} // anonymous namespace

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger& Logger = MetaStateLogger;

////////////////////////////////////////////////////////////////////////////////

#pragma pack(push, 4)

struct TSnapshotHeader
{
    static const ui64 CorrectSignature =  0x3130303053535459ull; // YTSS0001    

    ui64 Signature;
    i32 SegmentId;
    TEpochId Epoch;
    i32 PrevRecordCount;
    ui64 DataLength;
    ui64 Checksum;

    TSnapshotHeader()
        : Signature(CorrectSignature)
        , SegmentId(0)
        , Epoch()
        , PrevRecordCount(0)
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

TSnapshotReader::~TSnapshotReader()
{ }

void TSnapshotReader::Open()
{
    File.Reset(new TFile(FileName, OpenExisting));

    Header.Reset(new TSnapshotHeader());
    ReadPod(*File, *Header);

    Header->Validate();
    LOG_FATAL_UNLESS(
        Header->SegmentId == SnapshotId,
        "Invalid snapshot id in header: expected %d, got %d", SnapshotId, Header->SegmentId);
    YCHECK(Header->DataLength + sizeof(*Header) == static_cast<ui64>(File->GetLength()));

    FileInput.Reset(new TBufferedFileInput(*File));
    TInputStream* inputStream = ~FileInput;
    if (EnableCompression) {
        DecompressedInput.Reset(new TDecompressedInput(inputStream));
        inputStream = ~DecompressedInput;
    }
    ChecksummableInput.Reset(new TChecksummableInput(inputStream));
}

TInputStream* TSnapshotReader::GetStream() const
{
    YCHECK(~ChecksummableInput);
    return ~ChecksummableInput;
}

i64 TSnapshotReader::GetLength() const
{
    YCHECK(~File);
    return File->GetLength();
}

TChecksum TSnapshotReader::GetChecksum() const
{
    YCHECK(~Header);
    return Header->Checksum;
}

i32 TSnapshotReader::GetPrevRecordCount() const
{
    YCHECK(~Header);
    return Header->PrevRecordCount;
}

const TEpochId& TSnapshotReader::GetEpoch() const
{
    YCHECK(~Header);
    return Header->Epoch;
}

////////////////////////////////////////////////////////////////////////////////

TSnapshotWriter::TSnapshotWriter(
    const Stroka& fileName,
    i32 segmentId,
    bool enableCompression)
    : State(EState::Uninitialized)
    , FileName(fileName)
    , TempFileName(fileName + NFS::TempFileSuffix)
    , EnableCompression(enableCompression)
    , Header(new TSnapshotHeader())
{
    Header->SegmentId = segmentId;
}

TSnapshotWriter::~TSnapshotWriter()
{ }

void TSnapshotWriter::Open(i32 prevRecordCount, const TEpochId& epoch)
{
    YCHECK(State == EState::Uninitialized);

    Header->PrevRecordCount = prevRecordCount;
    Header->Epoch = epoch;

    File.Reset(new TBufferedFile(TempFileName, RdWr | CreateAlways));
    File->Resize(sizeof(TSnapshotHeader));
    File->Seek(0, sEnd);

    TOutputStream* output = File->GetOutputStream();
    if (EnableCompression) {
        CompressedOutput.Reset(new TCompressedOutput(output));
        output = ~CompressedOutput;
    }
    BufferedOutput.Reset(new TBufferedOutput(output, 64 * 1024));
    ChecksummableOutput.Reset(new TChecksummableOutput(~BufferedOutput));

    State = EState::Opened;
}

TOutputStream* TSnapshotWriter::GetStream() const
{
    YCHECK(State == EState::Opened);
    return ~ChecksummableOutput;
}

void TSnapshotWriter::Close()
{
    if (State != EState::Opened) {
        return;
    }

    ChecksummableOutput->Finish();

    Header->Checksum = ChecksummableOutput->GetChecksum();
    Header->DataLength = File->GetLength() - sizeof(TSnapshotHeader);

    File->Seek(0, sSet);
    WritePod(*File, *Header);
    File->Close();

    CheckedMoveFile(TempFileName, FileName);
}

TChecksum TSnapshotWriter::GetChecksum() const
{
    YCHECK(State == EState::Closed);
    return Header->Checksum;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NMetaState
} // namespace NYT
