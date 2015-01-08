#include "stdafx.h"
#include "file_writer.h"
#include "format.h"
#include "chunk_replica.h"
#include "chunk_meta_extensions.h"

#include <core/misc/fs.h>

namespace NYT {
namespace NChunkClient {

using namespace NChunkClient::NProto;

///////////////////////////////////////////////////////////////////////////////

static const auto FileMode =
	CreateAlways | WrOnly | Seq | CloseOnExec |  AR | AWUser | AWGroup;

///////////////////////////////////////////////////////////////////////////////

TFileWriter::TFileWriter(
    const Stroka& fileName,
    bool syncOnClose)
    : FileName_(fileName)
    , SyncOnClose_(syncOnClose)
{ }

TFuture<void> TFileWriter::Open()
{
    YCHECK(!IsOpen_);
    YCHECK(!IsClosed_);

    try {
        // NB: Races are possible between file creation and a call to flock.
        // Unfortunately in Linux we can't create'n'flock a file atomically.
        DataFile_.reset(new TFile(FileName_ + NFS::TempFileSuffix, FileMode));
        DataFile_->Flock(LOCK_EX);
    } catch (const std::exception& ex) {
        return MakeFuture(TError(
            "Error opening chunk data file %v",
            FileName_)
             << ex);
    }

    IsOpen_ = true;

    return VoidFuture;
}

bool TFileWriter::WriteBlock(const TSharedRef& block)
{
    YCHECK(IsOpen_);
    YCHECK(!IsClosed_);

    try {
        auto* blockInfo = BlocksExt_.add_blocks();
        blockInfo->set_offset(DataFile_->GetPosition());
        blockInfo->set_size(static_cast<int>(block.Size()));

        auto checksum = GetChecksum(block);
        blockInfo->set_checksum(checksum);
        DataFile_->Write(block.Begin(), block.Size());

        DataSize_ += block.Size();
        return true;
    } catch (const std::exception& ex) {
        Error_ = TError(
            "Failed to write chunk data file %v",
            FileName_)
            << ex;
        return false;
    }
}

bool TFileWriter::WriteBlocks(const std::vector<TSharedRef>& blocks)
{
    YCHECK(IsOpen_);
    YCHECK(!IsClosed_);

    for (const auto& block : blocks) {
        if (!WriteBlock(block)) {
            return false;
        }
    }
    return true;
}

TFuture<void> TFileWriter::GetReadyEvent()
{
    YCHECK(IsOpen_);
    YCHECK(!IsClosed_);

    return MakeFuture(Error_);
}

TFuture<void> TFileWriter::Close(const NChunkClient::NProto::TChunkMeta& chunkMeta)
{
    if (!IsOpen_ || !Error_.IsOK()) {
        return MakeFuture(Error_);
    }

    IsOpen_ = false;
    IsClosed_ = true;

    try {
        if (SyncOnClose_) {
#ifdef _linux_
            if (fsync(DataFile_->GetHandle()) != 0) {
                THROW_ERROR_EXCEPTION("Error closing chunk: fsync failed for data file %v",
                    FileName_)
                    << TError::FromSystem();
            }
#endif
        }
        DataFile_->Close();
        DataFile_.reset();
    } catch (const std::exception& ex) {
        return MakeFuture(TError(
            "Error closing chunk data file %v",
            FileName_)
            << ex);
    }

    // Write meta.
    ChunkMeta_.CopyFrom(chunkMeta);
    SetProtoExtension(ChunkMeta_.mutable_extensions(), BlocksExt_);

    TSharedRef metaData;
    YCHECK(SerializeToProtoWithEnvelope(ChunkMeta_, &metaData));

    TChunkMetaHeader header;
    header.Signature = header.ExpectedSignature;
    header.Checksum = GetChecksum(metaData);

    auto metaFileName = FileName_ + ChunkMetaSuffix;

    try {
        TFile chunkMetaFile(metaFileName + NFS::TempFileSuffix, FileMode);

        WritePod(chunkMetaFile, header);
        chunkMetaFile.Write(metaData.Begin(), metaData.Size());

        if (SyncOnClose_) {
#ifdef _linux_
            if (fsync(chunkMetaFile.GetHandle()) != 0) {
                THROW_ERROR_EXCEPTION("Error closing chunk: fsync failed for meta file %v",
                    metaFileName)
                    << TError::FromSystem();
            }
#endif
        }

        chunkMetaFile.Close();

        NFS::Rename(metaFileName + NFS::TempFileSuffix, metaFileName);
        NFS::Rename(FileName_ + NFS::TempFileSuffix, FileName_);
    } catch (const std::exception& ex) {
        return MakeFuture(TError(
            "Error writing chunk meta file %v",
            metaFileName)
            << ex);
    }

    ChunkInfo_.set_disk_space(DataSize_ + metaData.Size() + sizeof (TChunkMetaHeader));

    return VoidFuture;
}

void TFileWriter::Abort()
{
    if (!IsOpen_)
        return;

    IsClosed_ = true;
    IsOpen_ = false;

    DataFile_.reset();

    NFS::Remove(FileName_ + NFS::TempFileSuffix);
}

const TChunkInfo& TFileWriter::GetChunkInfo() const
{
    YCHECK(IsClosed_);

    return ChunkInfo_;
}

const TChunkMeta& TFileWriter::GetChunkMeta() const
{
    YCHECK(IsClosed_);

    return ChunkMeta_;
}

TChunkReplicaList TFileWriter::GetWrittenChunkReplicas() const
{
    YUNIMPLEMENTED();
}

i64 TFileWriter::GetDataSize() const
{
    return DataSize_;
}

///////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT

