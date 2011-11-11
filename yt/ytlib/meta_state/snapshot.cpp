#include "stdafx.h"
#include "snapshot.h"

#include "../misc/common.h"
#include "../misc/fs.h"
#include "../misc/serialize.h"

#include <util/folder/dirut.h>

namespace NYT {
namespace NMetaState {

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

TSnapshotReader::TSnapshotReader(
    Stroka fileName,
    i32 segmentId)
    : FileName(fileName)
    , SegmentId(segmentId)
{ }

void TSnapshotReader::Open(i64 offset)
{
    Close();

    LOG_DEBUG("Opening snapshot reader %s", ~FileName);
    File.Reset(new TFile(FileName, OpenExisting));
    
    TSnapshotHeader header;
    Read(*File, &header);
    header.Validate();
    if (header.SegmentId != SegmentId) {
        LOG_FATAL("Invalid snapshot id: expected %d, got %d",
            SegmentId,
            header.SegmentId);
    }
    PrevRecordCount = header.PrevRecordCount;
    Checksum = header.Checksum;

    File->Seek(offset + sizeof(header), sSet);
    FileInput.Reset(new TBufferedFileInput(*File));
    ChecksummableInput.Reset(new TChecksummableInput(*FileInput));
}

TInputStream& TSnapshotReader::GetStream() const
{
    YASSERT(~ChecksummableInput != NULL);
    return *ChecksummableInput;
}

i64 TSnapshotReader::GetLength() const
{
    return File->GetLength() - sizeof(TSnapshotHeader);
}

void TSnapshotReader::Close()
{
    if (~FileInput == NULL)
        return;

    LOG_DEBUG("Closing snapshot reader %s", ~FileName);
    FileInput.Reset(NULL);
    ChecksummableInput.Reset(NULL);
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
    PrevRecordCount = prevRecordCount;
    Close();

    LOG_DEBUG("Opening snapshot writer %s", ~TempFileName);
    File.Reset(new TFile(TempFileName, RdWr | CreateAlways));
    FileOutput.Reset(new TBufferedFileOutput(*File));
    ChecksummableOutput.Reset(new TChecksummableOutput(*FileOutput));

    TSnapshotHeader header(SegmentId, PrevRecordCount);
    Write(*FileOutput, header);

    Checksum = 0;
}

TOutputStream& TSnapshotWriter::GetStream() const
{
    YASSERT(~ChecksummableOutput != NULL);
    return *ChecksummableOutput;
}

void TSnapshotWriter::Close()
{
    if (~FileOutput == NULL)
        return;

    Checksum = ChecksummableOutput->GetChecksum();

    LOG_DEBUG("Closing snapshot writer %s", ~TempFileName);
    ChecksummableOutput->Flush();
    ChecksummableOutput.Reset(NULL);
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
            ythrow yexception() << "Error removing " << FileName;
        }
        LOG_WARNING("File %s already existed and is deleted", ~FileName);
    }

    if (!NFS::Rename(~TempFileName, ~FileName)) {
        ythrow yexception() << "Error renaming " << TempFileName << " to " << FileName;
    }
}

TChecksum TSnapshotWriter::GetChecksum() const
{
    // TODO: check that checksum is available
    return Checksum;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NMetaState
} // namespace NYT
