#include "stdafx.h"
#include "file_writer.h"

#include "chunk_replica.h"

#include <core/misc/fs.h>
#include <core/misc/serialize.h>
#include <core/misc/protobuf_helpers.h>

#include <core/logging/log.h>

#include <ytlib/chunk_client/chunk_meta_extensions.h>

namespace NYT {
namespace NChunkClient {

using namespace NChunkClient::NProto;

///////////////////////////////////////////////////////////////////////////////

static TNullOutput NullOutput;

///////////////////////////////////////////////////////////////////////////////

TFileWriter::TFileWriter(
    const Stroka& fileName,
    bool syncOnClose)
    : FileName_(fileName)
    , SyncOnClose_(syncOnClose)
    , IsOpen_(false)
    , IsClosed_(false)
    , DataSize_(0)
    , Result_(OKFuture)
{ }

TAsyncError TFileWriter::Open()
{
    YCHECK(!IsOpen_);
    YCHECK(!IsClosed_);

    ui32 oMode = CreateAlways | WrOnly | Seq | CloseOnExec |
        AR | AWUser | AWGroup;

    // NB! Races are possible between file creation and call to flock.
    // Unfortunately in Linux we cannot make it atomically.
    DataFile_.reset(new TFile(FileName_ + NFS::TempFileSuffix, oMode));
    DataFile_->Flock(LOCK_EX);

    IsOpen_ = true;
    return OKFuture;
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
        Result_ = MakeFuture(
            TError("Failed to write block to file")
            << ex);
        return false;
    }
}

bool TFileWriter::WriteBlocks(const std::vector<TSharedRef>& blocks)
{
    bool result = true;
    for (const auto& block : blocks) {
        result = WriteBlock(block);
    }
    return result;
}

TAsyncError TFileWriter::GetReadyEvent()
{
    return Result_;
}

TAsyncError TFileWriter::Close(const NChunkClient::NProto::TChunkMeta& chunkMeta)
{
    if (!IsOpen_ || !Result_.Get().IsOK()) {
        return Result_;
    }

    IsOpen_ = false;
    IsClosed_ = true;

    try {
        if (SyncOnClose_) {
#ifdef _linux_
            if (fsync(DataFile_->GetHandle()) != 0) {
                THROW_ERROR_EXCEPTION("fsync failed for chunk data file")
                    << TError::FromSystem();
            }
#endif
        }
        DataFile_->Close();
        DataFile_.reset();
    } catch (const std::exception& ex) {
        return MakeFuture(
            TError("Failed to close chunk data file %s", ~FileName_)
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

    Stroka chunkMetaFileName = FileName_ + ChunkMetaSuffix;

    try {
        TFile chunkMetaFile(
            chunkMetaFileName + NFS::TempFileSuffix,
            CreateAlways | WrOnly | Seq | CloseOnExec | ARUser | ARGroup | AWUser | AWGroup);

        WritePod(chunkMetaFile, header);
        chunkMetaFile.Write(metaData.Begin(), metaData.Size());

        if (SyncOnClose_) {
#ifdef _linux_
            if (fsync(chunkMetaFile.GetHandle()) != 0) {
                THROW_ERROR_EXCEPTION("Error closing chunk: fsync failed")
                    << TError::FromSystem();
            }
#endif
        }

        chunkMetaFile.Close();

        NFS::Rename(chunkMetaFileName + NFS::TempFileSuffix, chunkMetaFileName);
        NFS::Rename(FileName_ + NFS::TempFileSuffix, FileName_);
    } catch (const std::exception& ex) {
        return MakeFuture(TError(
            "Failed to write chunk meta to %s",
            ~chunkMetaFileName.Quote())
            << ex);
    }

    ChunkInfo_.set_disk_space(DataSize_ + metaData.Size() + sizeof (TChunkMetaHeader));

    return OKFuture;
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

void RemoveChunkFiles(const Stroka& dataFileName)
{
    NFS::Remove(dataFileName);

    auto metaFileName = dataFileName + ChunkMetaSuffix;
    if (NFS::Exists(metaFileName)) {
        NFS::Remove(metaFileName);
    }
}

///////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT

