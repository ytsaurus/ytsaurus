#include "stdafx.h"
#include "file_writer.h"

#include <core/misc/fs.h>
#include <core/misc/serialize.h>
#include <core/misc/protobuf_helpers.h>

#include <core/logging/log.h>

#include <ytlib/chunk_client/chunk_meta_extensions.h>

#include <util/stream/null.h>

namespace NYT {
namespace NChunkClient {

using namespace NChunkClient::NProto;

///////////////////////////////////////////////////////////////////////////////

static auto& Logger = ChunkWriterLogger;

static TNullOutput NullOutput;

///////////////////////////////////////////////////////////////////////////////

TFileWriter::TFileWriter(
    const Stroka& fileName,
    bool syncOnClose)
    : FileName(fileName)
    , SyncOnClose(syncOnClose)
    , IsOpen(false)
    , IsClosed(false)
    , DataSize(0)
    , ChecksumOutput(&NullOutput)
    , Result(OKFuture)
{ }

void TFileWriter::Open()
{
    YCHECK(!IsOpen);
    YCHECK(!IsClosed);

    ui32 oMode = CreateAlways | WrOnly | Seq | CloseOnExec |
        AR | AWUser | AWGroup;

    // NB! Races are possible between file creation and call to flock.
    // Unfortunately in Linux we cannot make it atomically.
    DataFile.reset(new TFile(FileName + NFS::TempFileSuffix, oMode));
    DataFile->Flock(LOCK_EX);

    IsOpen = true;
}

bool TFileWriter::WriteBlock(const TSharedRef& block)
{
    YCHECK(IsOpen);
    YCHECK(!IsClosed);

    try {
        auto* blockInfo = BlocksExt.add_blocks();
        blockInfo->set_offset(DataFile->GetPosition());
        blockInfo->set_size(static_cast<int>(block.Size()));

        auto checksum = GetChecksum(block);
        blockInfo->set_checksum(checksum);
        ChecksumOutput.Write(&checksum, sizeof(checksum));
        DataFile->Write(block.Begin(), block.Size());

        DataSize += block.Size();
        return true;
    } catch (const std::exception& ex) {
        Result = MakeFuture(
            TError("Failed to write block to file")
            << ex);
        return false;
    }
}

TAsyncError TFileWriter::GetReadyEvent()
{
    return Result;
}

TAsyncError TFileWriter::Close(const NChunkClient::NProto::TChunkMeta& chunkMeta)
{
    if (!IsOpen || !Result.Get().IsOK()) {
        return Result;
    }

    IsOpen = false;
    IsClosed = true;

    try {
        if (SyncOnClose) {
#ifdef _linux_
            if (fsync(DataFile->GetHandle()) != 0) {
                THROW_ERROR_EXCEPTION("fsync failed for chunk data file")
                    << TError::FromSystem();
            }
#endif
        }
        DataFile->Close();
        DataFile.reset();
    } catch (const std::exception& ex) {
        return MakeFuture(
            TError("Failed to close chunk data file %s", ~FileName)
            << ex);
    }

    // Write meta.
    ChunkMeta.CopyFrom(chunkMeta);
    SetProtoExtension(ChunkMeta.mutable_extensions(), BlocksExt);

    TSharedRef metaData;
    YCHECK(SerializeToProtoWithEnvelope(ChunkMeta, &metaData));

    TChunkMetaHeader header;
    header.Signature = header.ExpectedSignature;
    header.Checksum = GetChecksum(metaData);

    Stroka chunkMetaFileName = FileName + ChunkMetaSuffix;

    try {
        TFile chunkMetaFile(
            chunkMetaFileName + NFS::TempFileSuffix,
            CreateAlways | WrOnly | Seq | CloseOnExec | ARUser | ARGroup | AWUser | AWGroup);

        WritePod(chunkMetaFile, header);
        chunkMetaFile.Write(metaData.Begin(), metaData.Size());

        if (SyncOnClose) {
#ifdef _linux_
            if (fsync(chunkMetaFile.GetHandle()) != 0) {
                THROW_ERROR_EXCEPTION("Error closing chunk: fsync failed")
                    << TError::FromSystem();
            }
#endif
        }

        chunkMetaFile.Close();
    } catch (const std::exception& ex) {
        return MakeFuture(
            TError("Failed to write chunk meta to %s", ~chunkMetaFileName.Quote())
            << ex);
    }

    if (!NFS::Rename(chunkMetaFileName + NFS::TempFileSuffix, chunkMetaFileName)) {
        return MakeFuture(TError(
            "Error renaming temp chunk meta file %s",
            ~chunkMetaFileName.Quote()));
    }

    if (!NFS::Rename(FileName + NFS::TempFileSuffix, FileName)) {
        return MakeFuture(TError(
            "Error renaming temp chunk file %s",
            ~FileName.Quote()));
    }

    ChunkInfo.set_meta_checksum(ChecksumOutput.GetChecksum());
    ChunkInfo.set_disk_space(DataSize + metaData.Size() + sizeof (TChunkMetaHeader));

    return OKFuture;
}


void TFileWriter::Abort()
{
    if (!IsOpen) {
        return;
    }
    IsClosed = true;
    IsOpen = false;

    DataFile.reset();
    NFS::Remove(FileName + NFS::TempFileSuffix);
}

const TChunkInfo& TFileWriter::GetChunkInfo() const
{
    YCHECK(IsClosed);
    return ChunkInfo;
}

const TChunkMeta& TFileWriter::GetChunkMeta() const
{
    YCHECK(IsClosed);
    return ChunkMeta;
}

const std::vector<int> TFileWriter::GetWrittenIndexes() const
{
    YUNIMPLEMENTED();
}

i64 TFileWriter::GetDataSize() const
{
    return DataSize;
}

///////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT

