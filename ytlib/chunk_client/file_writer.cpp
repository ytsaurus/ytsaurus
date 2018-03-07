#include "file_writer.h"
#include "chunk_meta_extensions.h"
#include "chunk_replica.h"
#include "format.h"
#include "block.h"

#include <yt/core/misc/fs.h>
#include <yt/core/misc/checksum.h>

namespace NYT {
namespace NChunkClient {

using namespace NChunkClient::NProto;

////////////////////////////////////////////////////////////////////////////////

static const auto FileMode =
    CreateAlways |
    WrOnly |
    Seq |
    CloseOnExec |
    AR |
    AWUser |
    AWGroup;

////////////////////////////////////////////////////////////////////////////////

TFileWriter::TFileWriter(
    const TChunkId& chunkId,
    const TString& fileName,
    bool syncOnClose,
    bool enableWriteDirectIO)
    : ChunkId_(chunkId)
    , FileName_(fileName)
    , SyncOnClose_(syncOnClose)
    , EnableWriteDirectIO_(enableWriteDirectIO)
{ }

TFuture<void> TFileWriter::Open()
{
    YCHECK(!IsOpen_);
    YCHECK(!IsClosed_);

    try {
        NFS::ExpectIOErrors([&] () {
            if (!EnableWriteDirectIO_) {
                // NB: Races are possible between file creation and a call to flock.
                // Unfortunately in Linux we can't create'n'flock a file atomically.
                DataFile_.reset(new TFile(FileName_ + NFS::TempFileSuffix, FileMode));
                DataFile_->Flock(LOCK_EX);
            } else {
                DirectIOFile_.reset(new TDirectIOBufferedFile(
                    FileName_ + NFS::TempFileSuffix,
                    FileMode | Direct));
                DirectIOFile_->GetHandle().Flock(LOCK_EX);
            }
        });
    } catch (const std::exception& ex) {
        return MakeFuture(TError(
            "Error opening chunk data file %v",
            FileName_)
             << ex);
    }

    IsOpen_ = true;

    return VoidFuture;
}

bool TFileWriter::WriteBlock(const TBlock& block)
{
    YCHECK(IsOpen_);
    YCHECK(!IsClosed_);

    block.ValidateChecksum();

    try {
        auto* blockInfo = BlocksExt_.add_blocks();
        if (!EnableWriteDirectIO_) {
            blockInfo->set_offset(DataFile_->GetPosition());
        } else {
            blockInfo->set_offset(DirectIOFile_->GetWritePosition());
        }

        blockInfo->set_size(static_cast<int>(block.Size()));

        blockInfo->set_checksum(block.GetOrComputeChecksum());

        NFS::ExpectIOErrors([&] () {
            if (!EnableWriteDirectIO_) {
                DataFile_->Write(block.Data.Begin(), block.Size());
            } else {
                DirectIOFile_->Write(block.Data.Begin(), block.Size());
            }
        });

        DataSize_ += block.Size();
    } catch (const std::exception& ex) {
        Error_ = TError(
            "Failed to write chunk data file %v",
            FileName_)
            << ex;
        return false;
    }

    return true;
}

bool TFileWriter::WriteBlocks(const std::vector<TBlock>& blocks)
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
        NFS::ExpectIOErrors([&] () {
            if (!DirectIOFile_) {
                if (SyncOnClose_) {
                    DataFile_->Flush();
                }
                DataFile_->Close();
                DataFile_.reset();
            } else {
                DirectIOFile_->Finish();
                DirectIOFile_.reset();
            }
        });
    } catch (const std::exception& ex) {
        return MakeFuture(TError(
            "Error closing chunk data file %v",
            FileName_)
            << ex);
    }

    // Write meta.
    ChunkMeta_.CopyFrom(chunkMeta);
    SetProtoExtension(ChunkMeta_.mutable_extensions(), BlocksExt_);

    auto metaData = SerializeProtoToRefWithEnvelope(ChunkMeta_);

    TChunkMetaHeader_2 header;
    header.Signature = header.ExpectedSignature;
    header.Checksum = GetChecksum(metaData);
    header.ChunkId = ChunkId_;

    auto metaFileName = FileName_ + ChunkMetaSuffix;

    try {
        NFS::ExpectIOErrors([&] () {
            TFile chunkMetaFile(metaFileName + NFS::TempFileSuffix, FileMode);

            WritePod(chunkMetaFile, header);

            chunkMetaFile.Write(metaData.Begin(), metaData.Size());

            if (SyncOnClose_) {
                chunkMetaFile.Flush();
            }

            chunkMetaFile.Close();

            NFS::Rename(metaFileName + NFS::TempFileSuffix, metaFileName);
            NFS::Rename(FileName_ + NFS::TempFileSuffix, FileName_);

            if (SyncOnClose_) {
                NFS::FlushDirectory(NFS::GetDirectoryName(FileName_));
            }
        });
    } catch (const std::exception& ex) {
        return MakeFuture(TError(
            "Error writing chunk meta file %v",
            metaFileName)
            << ex);
    }

    ChunkInfo_.set_disk_space(DataSize_ + metaData.Size() + sizeof (TChunkMetaHeader_2));

    return VoidFuture;
}

void TFileWriter::Abort()
{
    if (!IsOpen_)
        return;

    IsClosed_ = true;
    IsOpen_ = false;

    if (!EnableWriteDirectIO_) {
        DataFile_.reset();
    } else {
        DirectIOFile_.reset();
    }

    NFS::Remove(FileName_ + NFS::TempFileSuffix);
}

const TChunkInfo& TFileWriter::GetChunkInfo() const
{
    YCHECK(IsClosed_);

    return ChunkInfo_;
}

const TDataStatistics& TFileWriter::GetDataStatistics() const
{
    YCHECK(IsClosed_);

    Y_UNREACHABLE();
}

const TChunkMeta& TFileWriter::GetChunkMeta() const
{
    YCHECK(IsClosed_);

    return ChunkMeta_;
}

TChunkReplicaList TFileWriter::GetWrittenChunkReplicas() const
{
    Y_UNIMPLEMENTED();
}

TChunkId TFileWriter::GetChunkId() const
{
    return ChunkId_;
}

NErasure::ECodec TFileWriter::GetErasureCodecId() const
{
    return NErasure::ECodec::None;
}

i64 TFileWriter::GetDataSize() const
{
    return DataSize_;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT

