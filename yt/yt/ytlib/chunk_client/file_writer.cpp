#include "file_writer.h"
#include "chunk_meta_extensions.h"
#include "format.h"
#include "block.h"

#include <yt/ytlib/chunk_client/io_engine.h>

#include <yt/client/chunk_client/chunk_replica.h>

#include <yt/core/misc/fs.h>
#include <yt/core/misc/checksum.h>

#include <util/system/align.h>
#include <util/system/compiler.h>

namespace NYT::NChunkClient {

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

static constexpr i64 Alignment = 4096;

////////////////////////////////////////////////////////////////////////////////

struct TFileWriterBufferTag
{ };

TFileWriter::TFileWriter(
    const IIOEnginePtr& ioEngine,
    TChunkId chunkId,
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
#ifdef _msan_enabled_
    constexpr bool initializeMemory = true;
#else
    constexpr bool initializeMemory = false;
#endif
    auto data = TSharedMutableRef::Allocate<TFileWriterBufferTag>(size + Alignment, initializeMemory);
    data = data.Slice(AlignUp(data.Begin(), Alignment), data.End());
    data = data.Slice(data.Begin(), data.Begin() + size);
    Buffer_ = data;
}

void TFileWriter::TryLockDataFile(TPromise<void> promise)
{
    if (DataFile_->Flock(LOCK_EX | LOCK_NB) < 0 && errno == EWOULDBLOCK) {
        NConcurrency::TDelayedExecutor::Submit(
            BIND(&TFileWriter::TryLockDataFile, MakeStrong(this), promise),
            TDuration::MilliSeconds(10));
    } else {
        Open_ = true;
        promise.Set();
    }
}

TFuture<void> TFileWriter::LockDataFile(const std::shared_ptr<TFileHandle>& file)
{
    DataFile_ = file;

    TPromise<void> promise = NewPromise<void>();
    TryLockDataFile(promise);
    return promise;
}

TFuture<void> TFileWriter::Open()
{
    YT_VERIFY(!Open_);
    YT_VERIFY(!Closed_);
    YT_VERIFY(!Opening_);

    Opening_ = true;

    auto mode = FileMode;
    if (EnableWriteDirectIO_) {
        mode |= DirectAligned;
    }
    // NB: Races are possible between file creation and a call to flock.
    // Unfortunately in Linux we can't create'n'flock a file atomically.
    return IOEngine_->Open(FileName_ + NFS::TempFileSuffix, mode)
        .Apply(BIND(&TFileWriter::LockDataFile, MakeStrong(this)))
        .Apply(BIND([this, this_ = MakeStrong(this)] (const TErrorOr<void>& error) {
            Opening_ = false;
            if (!error.IsOK()) {
                THROW_ERROR error;
            }
        }));
}

bool TFileWriter::WriteBlock(const TBlock& block)
{
    YT_VERIFY(Open_);
    YT_VERIFY(!Closed_);

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

            auto offset = ::AlignDown(filePosition, Alignment);
            auto start = ::AlignDown(Buffer_.Begin() + BufferPosition_, Alignment);
            auto end = ::AlignUp(Buffer_.Begin() + BufferPosition_ + size, Alignment);
            auto data = Buffer_.Slice(start, end);

            YT_VERIFY(offset >= 0 && offset <= filePosition);
            YT_VERIFY(start >= Buffer_.Begin() && end <= Buffer_.End());
            YT_VERIFY(filePosition - offset == Buffer_.Begin() + BufferPosition_ - start);

            NConcurrency::WaitFor(IOEngine_->Pwrite(DataFile_, data, offset)).ThrowOnError();

            filePosition += size;

            BufferPosition_ += size;
            p += size;

            YT_VERIFY(BufferPosition_ <= Buffer_.Size());

            if (BufferPosition_ == Buffer_.Size()) {
                BufferPosition_ = 0;
            }
        }

        DataSize_ += block.Size();

        YT_VERIFY(filePosition == DataSize_);
    } catch (const TSystemError& error) {
        if (error.Status() == ENOSPC) {
            Error_ = TError(
                NChunkClient::EErrorCode::NoSpaceLeftOnDevice,
                "Not enough space to write chunk data file %v",
                FileName_);
        } else {
            Error_ = TError(
                "Failed to write chunk data file %v",
                FileName_)
                << static_cast<const std::exception&>(error);
        }
        return false;
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
    YT_VERIFY(Open_);
    YT_VERIFY(!Closed_);

    BlocksExt_.set_sync_on_close(SyncOnClose_);

    for (const auto& block : blocks) {
        if (!WriteBlock(block)) {
            return false;
        }
    }
    return true;
}

TFuture<void> TFileWriter::GetReadyEvent()
{
    YT_VERIFY(Open_);
    YT_VERIFY(!Closed_);

    return MakeFuture(Error_);
}

TFuture<void> TFileWriter::WriteMeta(const TRefCountedChunkMetaPtr& chunkMeta)
{
    // Write meta.
    ChunkMeta_->CopyFrom(*chunkMeta);
    SetProtoExtension(ChunkMeta_->mutable_extensions(), BlocksExt_);

    auto metaFileName = FileName_ + ChunkMetaSuffix;

    return IOEngine_->Open(metaFileName + NFS::TempFileSuffix, FileMode)
        .Apply(BIND([this, _this = MakeStrong(this)] (const std::shared_ptr<TFileHandle>& chunkMetaFile) {
            auto metaData = SerializeProtoToRefWithEnvelope(*ChunkMeta_);

            TChunkMetaHeader_2 header;
            header.Signature = header.ExpectedSignature;
            header.Checksum = GetChecksum(metaData);
            header.ChunkId = ChunkId_;

            MetaDataSize_ = metaData.Size() + sizeof(header);

            TSharedMutableRef buffer = Buffer_;
            if (buffer.Size() < MetaDataSize_) {
                auto data = TSharedMutableRef::Allocate<TFileWriterBufferTag>(MetaDataSize_ + Alignment, true);
                data = data.Slice(AlignUp(data.Begin(), Alignment), data.End());
                data = data.Slice(data.Begin(), data.Begin() + MetaDataSize_);
                buffer = data;
            }

            ::memcpy(buffer.Begin(), &header, sizeof(header));
            ::memcpy(buffer.Begin() + sizeof(header), metaData.Begin(), metaData.Size());

            return IOEngine_->Pwrite(chunkMetaFile, buffer, 0)
                .Apply(BIND(&IIOEngine::Close, IOEngine_, chunkMetaFile, MetaDataSize_, SyncOnClose_));
        }))
        .Apply(BIND([metaFileName, this, _this = MakeStrong(this)] () {
            NFS::Rename(metaFileName + NFS::TempFileSuffix, metaFileName);
            NFS::Rename(FileName_ + NFS::TempFileSuffix, FileName_);

            if (SyncOnClose_) {
                return IOEngine_->FlushDirectory(NFS::GetDirectoryName(FileName_));
            } else {
                return VoidFuture;
            }
        }))
        .Apply(BIND([this, _this = MakeStrong(this)] () {
            ChunkInfo_.set_disk_space(DataSize_ + MetaDataSize_);
        }));
}

TFuture<void> TFileWriter::Close(const TRefCountedChunkMetaPtr& chunkMeta)
{
    if (!Open_ || !Error_.IsOK()) {
        return MakeFuture(Error_);
    }

    Open_ = false;
    Closed_ = true;

    return IOEngine_->Close(DataFile_, DataSize_, SyncOnClose_)
        .Apply(BIND(&TFileWriter::WriteMeta, MakeStrong(this), chunkMeta));
}

void TFileWriter::Abort()
{
    if (!Open_)
        return;

    Closed_ = true;
    Open_ = false;

    DataFile_.reset();

    NFS::Remove(FileName_ + NFS::TempFileSuffix);
}

const TChunkInfo& TFileWriter::GetChunkInfo() const
{
    YT_VERIFY(Closed_);

    return ChunkInfo_;
}

const TDataStatistics& TFileWriter::GetDataStatistics() const
{
    YT_VERIFY(Closed_);

    YT_ABORT();
}

const TRefCountedChunkMetaPtr& TFileWriter::GetChunkMeta() const
{
    YT_VERIFY(Closed_);

    return ChunkMeta_;
}

TChunkReplicaWithMediumList TFileWriter::GetWrittenChunkReplicas() const
{
    YT_UNIMPLEMENTED();
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

bool TFileWriter::IsCloseDemanded() const
{
    YT_UNIMPLEMENTED();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient

