#include "file_writer.h"
#include "chunk_meta_extensions.h"
#include "chunk_replica.h"
#include "format.h"
#include "block.h"

#include <yt/core/misc/fs.h>
#include <yt/core/misc/checksum.h>

#include <yt/ytlib/chunk_client/io_engine.h>

#include <util/system/align.h>

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
    const IIOEnginePtr& ioEngine,
    const TChunkId& chunkId,
    const TString& fileName,
    bool syncOnClose,
    bool enableWriteDirectIO)
    : IOEngine_(ioEngine)
    , ChunkId_(chunkId)
    , FileName_(fileName)
    , SyncOnClose_(syncOnClose)
    , EnableWriteDirectIO_(enableWriteDirectIO)
{
    size_t size = 1_MB;
    auto data = TSharedMutableRef::Allocate<TNull>(size + Alignment_, false);
    data = data.Slice(AlignUp(data.Begin(), Alignment_), data.End());
    data = data.Slice(data.Begin(), data.Begin() + size);
    Buffer_ = data;
}

TFuture<void> TFileWriter::Open()
{
    YCHECK(!IsOpen_);
    YCHECK(!IsClosed_);

    try {
        NFS::ExpectIOErrors([&] () {
            auto mode = FileMode;
            if (EnableWriteDirectIO_) {
                mode |= DirectAligned;
            }
            // NB: Races are possible between file creation and a call to flock.
            // Unfortunately in Linux we can't create'n'flock a file atomically.
            DataFile_ = IOEngine_->Open(FileName_ + NFS::TempFileSuffix, mode);
            DataFile_->Flock(LOCK_EX);
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
        blockInfo->set_offset(DataSize_);

        blockInfo->set_size(static_cast<int>(block.Size()));

        blockInfo->set_checksum(block.GetOrComputeChecksum());

        const char* p = block.Data.Begin();
        const char* pe = p + block.Size();

        auto filePosition = DataSize_;
        while (p != pe) {
            auto size = Min<size_t>(pe - p, Buffer_.Size() - BufferPosition_);
            ::memcpy(Buffer_.Begin() + BufferPosition_, p, size);

            auto offset = ::AlignDown(filePosition, Alignment_);
            auto start = ::AlignDown(Buffer_.Begin() + BufferPosition_, Alignment_);
            auto end = start + ::AlignUp<size_t>(size, Alignment_);
            auto data = Buffer_.Slice(start, end);

            YCHECK(offset >= 0 && offset <= filePosition);
            YCHECK(start >= Buffer_.Begin() && end <= Buffer_.End());
            YCHECK(filePosition - offset == Buffer_.Begin() + BufferPosition_ - start);

            IOEngine_->Pwrite(DataFile_, data, offset).Get().ThrowOnError();

            filePosition += size;

            BufferPosition_ += size;
            p += size;

            YCHECK(BufferPosition_ <= Buffer_.Size());

            if (BufferPosition_ == Buffer_.Size()) {
                BufferPosition_ = 0;
            }
        }

        DataSize_ += block.Size();

        YCHECK(filePosition == DataSize_);
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
            DataFile_->Resize(DataSize_);
            if (SyncOnClose_) {
                DataFile_->Flush();
            }
            DataFile_->Close();
            DataFile_.reset();
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

    DataFile_.reset();

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

