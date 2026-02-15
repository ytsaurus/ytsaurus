#include "chunk_file_writer.h"
#include "io_engine.h"
#include "private.h"

#include <yt/yt/ytlib/chunk_client/chunk_meta_extensions.h>
#include <yt/yt/ytlib/chunk_client/deferred_chunk_meta.h>
#include <yt/yt/ytlib/chunk_client/format.h>
#include <yt/yt/ytlib/chunk_client/block.h>

#include <yt/yt/client/chunk_client/chunk_replica.h>

#include <yt/yt/core/misc/fs.h>
#include <yt/yt/core/misc/checksum.h>

#include <util/system/align.h>
#include <util/system/compiler.h>

namespace NYT::NIO {

using namespace NConcurrency;
using namespace NChunkClient;
using namespace NChunkClient::NProto;

////////////////////////////////////////////////////////////////////////////////

static constexpr auto FileMode =
    CreateAlways |
    WrOnly |
    Seq |
    CloseOnExec |
    AR |
    AWUser |
    AWGroup;

constinit const auto Logger = IOLogger;

////////////////////////////////////////////////////////////////////////////////

i64 TruncateBlocks(NChunkClient::NProto::TBlocksExt& blocksExt, int truncateBlockCount, i64 oldDataSize)
{
    YT_LOG_FATAL_IF(
        truncateBlockCount > blocksExt.blocks_size() || truncateBlockCount < 0,
        "Invalid truncate block count (TruncateBlockCount: %v, BlockCount: %v)",
        truncateBlockCount,
        blocksExt.blocks_size());

    i64 truncateDataSize = 0;
    for (int index = truncateBlockCount; index < blocksExt.blocks_size(); ++index) {
        truncateDataSize += blocksExt.blocks(index).size();
    }
    blocksExt.mutable_blocks()->Truncate(truncateBlockCount);
    YT_VERIFY(truncateDataSize <= oldDataSize);
    return oldDataSize - truncateDataSize;
}

TSerializedBlocksRequest SerializeBlocks(i64 startOffset, const std::vector<TBlock>& blocks, NChunkClient::NProto::TBlocksExt& blocksExt)
{
    TSerializedBlocksRequest request;

    request.StartOffset = startOffset;
    request.EndOffset = request.StartOffset;

    request.Buffers.reserve(blocks.size());

    for (const auto& block : blocks) {
        auto error = block.CheckChecksum();
        YT_LOG_FATAL_UNLESS(
            error.IsOK(),
            error,
            "Block checksum mismatch during file writing");

        auto* blockInfo = blocksExt.add_blocks();
        blockInfo->set_offset(request.EndOffset);
        blockInfo->set_size(ToProto<i64>(block.Size()));
        blockInfo->set_checksum(block.GetOrComputeChecksum());

        request.EndOffset += block.Size();
        request.Buffers.push_back(block.Data);
    }

    return request;
}

TRefCountedChunkMetaPtr FinalizeChunkMeta(TDeferredChunkMetaPtr chunkMeta, const NChunkClient::NProto::TBlocksExt& blocksExt)
{
    if (!chunkMeta->IsFinalized()) {
        auto& mapping = chunkMeta->BlockIndexMapping();
        mapping = std::vector<int>(blocksExt.blocks().size());
        std::iota(mapping->begin(), mapping->end(), 0);
        chunkMeta->Finalize();
    }

    SetProtoExtension(chunkMeta->mutable_extensions(), blocksExt);
    return chunkMeta;
}

TSharedMutableRef SerializeChunkMeta(TChunkId chunkId, const TRefCountedChunkMetaPtr& chunkMeta)
{
    auto metaData = SerializeProtoToRefWithEnvelope(*chunkMeta);

    TChunkMetaHeader_2 header;
    header.Signature = header.ExpectedSignature;
    header.Checksum = GetChecksum(metaData);
    header.ChunkId = chunkId;

    auto metaDataSize = metaData.Size() + sizeof(header);

    struct TMetaBufferTag
    { };

    auto buffer = TSharedMutableRef::Allocate<TMetaBufferTag>(metaDataSize, {.InitializeStorage = false});
    ::memcpy(buffer.Begin(), &header, sizeof(header));
    ::memcpy(buffer.Begin() + sizeof(header), metaData.Begin(), metaData.Size());

    return buffer;
}

//////////////////////////////////////////////////////////////////////////////

TChunkFileWriter::TChunkFileWriter(
    IIOEnginePtr ioEngine,
    TChunkId chunkId,
    TString fileName,
    bool syncOnClose,
    bool useDirectIO)
    : IOEngine_(std::move(ioEngine))
    , ChunkId_(chunkId)
    , FileName_(std::move(fileName))
    , SyncOnClose_(syncOnClose)
    , UseDirectIO_(useDirectIO)
{
    BlocksExt_.set_sync_on_close(SyncOnClose_);
}

TFlags<EOpenModeFlag> TChunkFileWriter::GetFileMode() const
{
    auto flags = FileMode;
    if (UseDirectIO_) {
        flags |= DirectAligned;
    }
    return flags;
}

void TChunkFileWriter::TryLockDataFile(TPromise<void> promise)
{
    YT_VERIFY(State_.load() == EState::Opening);

    if (DataFile_->Flock(LOCK_EX | LOCK_NB) >= 0) {
        promise.Set();
        return;
    }

    if (errno != EWOULDBLOCK) {
        promise.Set(TError::FromSystem(errno));
        return;
    }

    YT_LOG_WARNING("Error locking chunk data file, retrying (Path: %v)",
        FileName_);

    TDelayedExecutor::Submit(
        BIND(&TChunkFileWriter::TryLockDataFile, MakeStrong(this), promise),
        TDuration::MilliSeconds(10),
        IOEngine_->GetAuxPoolInvoker());
}

void TChunkFileWriter::SetFailed(const TError& error)
{
    auto expected = TError();
    Error_.CompareExchange(expected, error);

    State_.store(EState::Failed);
}

TError TChunkFileWriter::TryChangeState(EState oldState, EState newState)
{
    if (State_.compare_exchange_strong(oldState, newState)) {
        return {};
    }

    auto error = TError(
        "Invalid chunk writer state: expected %Qlv, actual %Qlv",
        oldState,
        newState);
    if (oldState == EState::Failed) {
        error.MutableInnerErrors()->push_back(Error_.Load());
    }
    return error;
}

TFuture<void> TChunkFileWriter::Open()
{
    if (auto error = TryChangeState(EState::Created, EState::Opening); !error.IsOK()) {
        return MakeFuture<void>(std::move(error));
    }

    // NB: Races are possible between file creation and a call to flock.
    // Unfortunately in Linux we can't create'n'flock a file atomically.
    return IOEngine_->Open({FileName_ + NFS::TempFileSuffix, GetFileMode()})
        .Apply(BIND([
            this,
            this_ = MakeStrong(this)
        ] (const TIOEngineHandlePtr& file) {
            YT_VERIFY(State_.load() == EState::Opening);

            DataFile_ = file;

            auto promise = NewPromise<void>();
            TryLockDataFile(promise);
            return promise.ToFuture();
        }).AsyncVia(IOEngine_->GetAuxPoolInvoker()))
        .Apply(BIND([
            this,
            this_ = MakeStrong(this)
        ] (const TError& error) {
            YT_VERIFY(State_.load() == EState::Opening);

            if (!error.IsOK()) {
                SetFailed(error);
                THROW_ERROR_EXCEPTION("Failed to open chunk data file %v",
                    FileName_)
                    << error;
            }

            State_.store(EState::Ready);
        }));
}

TFuture<void> TChunkFileWriter::PreallocateDiskSpace(
    const TWorkloadDescriptor& workloadDescriptor,
    i64 spaceSize)
{
    if (DiskSpace_ < spaceSize) {
        DiskSpace_ = spaceSize;
        return IOEngine_->Allocate(TAllocateRequest{.Handle = DataFile_, .Size = DiskSpace_}, workloadDescriptor.Category);
    } else {
        return OKFuture;
    }
}

bool TChunkFileWriter::WriteBlock(
    const IChunkWriter::TWriteBlocksOptions& options,
    const TWorkloadDescriptor& workloadDescriptor,
    const TBlock& block)
{
    return WriteBlock(options, workloadDescriptor, {block}, {});
}

bool TChunkFileWriter::WriteBlock(
    const IChunkWriter::TWriteBlocksOptions& options,
    const TWorkloadDescriptor& workloadDescriptor,
    const TBlock& block,
    TFairShareSlotId fairShareSlotId)
{
    return WriteBlocks(options, workloadDescriptor, {block}, fairShareSlotId);
}

bool TChunkFileWriter::WriteBlocks(
    const IChunkWriter::TWriteBlocksOptions& options,
    const TWorkloadDescriptor& workloadDescriptor,
    const std::vector<TBlock>& blocks)
{
    return WriteBlocks(options, workloadDescriptor, blocks, {});
}

bool TChunkFileWriter::WriteBlocks(
    const IChunkWriter::TWriteBlocksOptions& options,
    const TWorkloadDescriptor& workloadDescriptor,
    const std::vector<TBlock>& blocks,
    TFairShareSlotId fairShareSlotId)
{
    if (auto error = TryChangeState(EState::Ready, EState::WritingBlocks); !error.IsOK()) {
        ReadyEvent_ = MakeFuture<void>(std::move(error));
        return false;
    }

    auto writeRequest = SerializeBlocks(DataSize_, blocks, BlocksExt_);

    ReadyEvent_ =
        IOEngine_->Write({
            DataFile_,
            writeRequest.StartOffset,
            std::move(writeRequest.Buffers),
            SyncOnClose_,
            fairShareSlotId,
        },
        workloadDescriptor.Category)
        .Apply(BIND([
            this,
            this_ = MakeStrong(this),
            newDataSize = writeRequest.EndOffset,
            blockCount = blocks.size(),
            chunkWriterStatistics = options.ClientOptions.ChunkWriterStatistics
        ] (const TErrorOr<TWriteResponse>& rspOrError) {
            YT_VERIFY(State_.load() == EState::WritingBlocks);

            if (!rspOrError.IsOK()) {
                SetFailed(rspOrError);
                THROW_ERROR_EXCEPTION("Failed to write chunk data file %v",
                    FileName_)
                    << rspOrError;
            }

            const auto& rsp = rspOrError.Value();
            YT_VERIFY(newDataSize - DataSize_ == rsp.WrittenBytes);

            chunkWriterStatistics->DataBytesWrittenToDisk.fetch_add(rsp.WrittenBytes, std::memory_order::relaxed);
            chunkWriterStatistics->DataBlocksWrittenToDisk.fetch_add(blockCount, std::memory_order::relaxed);
            chunkWriterStatistics->DataIOWriteRequests.fetch_add(rsp.IOWriteRequests, std::memory_order::relaxed);
            chunkWriterStatistics->DataIOSyncRequests.fetch_add(rsp.IOSyncRequests, std::memory_order::relaxed);

            DataSize_ = newDataSize;
            State_.store(EState::Ready);
        }).AsyncVia(IOEngine_->GetAuxPoolInvoker()));

    return false;
}

TFuture<void> TChunkFileWriter::GetReadyEvent()
{
    YT_VERIFY(ReadyEvent_);

    return ReadyEvent_;
}

TFuture<void> TChunkFileWriter::Close(
    const IChunkWriter::TWriteBlocksOptions& options,
    const TWorkloadDescriptor& workloadDescriptor,
    const TDeferredChunkMetaPtr& chunkMeta,
    std::optional<int> truncateBlockCount)
{
    return Close(options, workloadDescriptor, chunkMeta, {}, truncateBlockCount);
}

TFuture<void> TChunkFileWriter::Close(
    const IChunkWriter::TWriteBlocksOptions& options,
    const TWorkloadDescriptor& workloadDescriptor,
    const TDeferredChunkMetaPtr& chunkMeta,
    TFairShareSlotId fairShareSlotId,
    std::optional<int> truncateBlockCount)
{
    if (auto error = TryChangeState(EState::Ready, EState::Closing); !error.IsOK()) {
        return MakeFuture<void>(std::move(error));
    }

    if (truncateBlockCount.has_value()) {
        DataSize_ = TruncateBlocks(BlocksExt_, *truncateBlockCount, DataSize_);
    }

    auto metaFileName = FileName_ + ChunkMetaSuffix;
    return IOEngine_->Close({std::move(DataFile_), DataSize_, SyncOnClose_})
        .Apply(BIND([
            this,
            this_ = MakeStrong(this),
            chunkMeta,
            metaFileName,
            chunkWriterStatistics = options.ClientOptions.ChunkWriterStatistics
        ] (const TCloseResponse& rsp) {
            YT_VERIFY(State_.load() == EState::Closing);

            ChunkMeta_->CopyFrom(*FinalizeChunkMeta(std::move(chunkMeta), BlocksExt_));

            chunkWriterStatistics->DataIOSyncRequests.fetch_add(rsp.IOSyncRequests, std::memory_order::relaxed);

            return IOEngine_->Open({metaFileName + NFS::TempFileSuffix, GetFileMode()});
        }).AsyncVia(IOEngine_->GetAuxPoolInvoker()))
        .Apply(BIND([
            this,
            this_ = MakeStrong(this),
            workloadDescriptor,
            fairShareSlotId = fairShareSlotId,
            chunkWriterStatistics = options.ClientOptions.ChunkWriterStatistics
        ] (const TIOEngineHandlePtr& chunkMetaFile) {
            YT_VERIFY(State_.load() == EState::Closing);

            auto buffer = SerializeChunkMeta(ChunkId_, ChunkMeta_);
            MetaDataSize_ = buffer.size();

            return
                IOEngine_->Write({
                    chunkMetaFile,
                    0,
                    {std::move(buffer)},
                    SyncOnClose_,
                    fairShareSlotId,
                },
                workloadDescriptor.Category)
                .Apply(BIND([
                    this,
                    this_ = MakeStrong(this),
                    chunkWriterStatistics
                ] (const TWriteResponse& rsp) {
                    YT_VERIFY(MetaDataSize_ == rsp.WrittenBytes);

                    chunkWriterStatistics->MetaBytesWrittenToDisk.fetch_add(rsp.WrittenBytes, std::memory_order::relaxed);
                    chunkWriterStatistics->MetaIOWriteRequests.fetch_add(rsp.IOWriteRequests, std::memory_order::relaxed);
                    chunkWriterStatistics->MetaIOSyncRequests.fetch_add(rsp.IOSyncRequests, std::memory_order::relaxed);
                }).AsyncVia(IOEngine_->GetAuxPoolInvoker()))
                .Apply(BIND(&IIOEngine::Close,
                    IOEngine_,
                    TCloseRequest{
                        std::move(chunkMetaFile),
                        MetaDataSize_,
                        SyncOnClose_
                    },
                    workloadDescriptor.Category).AsyncVia(IOEngine_->GetAuxPoolInvoker()))
                .Apply(BIND([chunkWriterStatistics] (const TCloseResponse& rsp) {
                    chunkWriterStatistics->MetaIOSyncRequests.fetch_add(rsp.IOSyncRequests, std::memory_order::relaxed);
                }).AsyncVia(IOEngine_->GetAuxPoolInvoker()));
        }).AsyncVia(IOEngine_->GetAuxPoolInvoker()))
        .Apply(BIND([
            this,
            this_ = MakeStrong(this),
            metaFileName,
            chunkWriterStatistics = options.ClientOptions.ChunkWriterStatistics
        ] () mutable {
            YT_VERIFY(State_.load() == EState::Closing);

            NFS::Rename(metaFileName + NFS::TempFileSuffix, metaFileName);
            NFS::Rename(FileName_ + NFS::TempFileSuffix, FileName_);

            if (!SyncOnClose_) {
                return OKFuture;
            }

            return IOEngine_->FlushDirectory({NFS::GetDirectoryName(FileName_)})
                .Apply(BIND([
                    chunkWriterStatistics = std::move(chunkWriterStatistics)
                ] (const TFlushDirectoryResponse& rsp) {
                    chunkWriterStatistics->MetaIOSyncRequests.fetch_add(rsp.IOSyncRequests, std::memory_order::relaxed);
                }).AsyncVia(IOEngine_->GetAuxPoolInvoker()));
        }).AsyncVia(IOEngine_->GetAuxPoolInvoker()))
        .Apply(BIND([
            this,
            this_ = MakeStrong(this)
        ] (const TError& error) {
            YT_VERIFY(State_.load() == EState::Closing);

            if (!error.IsOK()) {
                SetFailed(error);
                THROW_ERROR_EXCEPTION("Failed to close chunk data file %v",
                    FileName_)
                    << error;
            }

            ChunkInfo_.set_disk_space(DataSize_ + MetaDataSize_);
            State_.store(EState::Closed);
        }).AsyncVia(IOEngine_->GetAuxPoolInvoker()));
}

i64 TChunkFileWriter::GetDataSize() const
{
    return DataSize_;
}

const TString& TChunkFileWriter::GetFileName() const
{
    return FileName_;
}

TFuture<void> TChunkFileWriter::Cancel()
{
    auto state = State_.exchange(EState::Aborting);
    YT_VERIFY(
        state != EState::Opening &&
        state != EState::WritingBlocks &&
        state != EState::Closing);

    return
        BIND([this, this_ = MakeStrong(this)] {
            YT_VERIFY(State_.load() == EState::Aborting);

            DataFile_.Reset();

            auto removeIfExists = [] (const TString& path) {
                if (NFS::Exists(path)) {
                    NFS::Remove(path);
                }
            };
            removeIfExists(FileName_ + NFS::TempFileSuffix);
            removeIfExists(FileName_ + ChunkMetaSuffix + NFS::TempFileSuffix);

            State_.store(EState::Aborted);
        })
        .AsyncVia(IOEngine_->GetAuxPoolInvoker())
        .Run();
}

const TChunkInfo& TChunkFileWriter::GetChunkInfo() const
{
    YT_VERIFY(State_.load() == EState::Closed);

    return ChunkInfo_;
}

const TDataStatistics& TChunkFileWriter::GetDataStatistics() const
{
    YT_VERIFY(State_.load() == EState::Closed);

    YT_ABORT();
}

const TRefCountedChunkMetaPtr& TChunkFileWriter::GetChunkMeta() const
{
    YT_VERIFY(State_.load() == EState::Closed);

    return ChunkMeta_;
}

TWrittenChunkReplicasInfo TChunkFileWriter::GetWrittenChunkReplicasInfo() const
{
    YT_UNIMPLEMENTED();
}

TChunkId TChunkFileWriter::GetChunkId() const
{
    return ChunkId_;
}

NErasure::ECodec TChunkFileWriter::GetErasureCodecId() const
{
    return NErasure::ECodec::None;
}

bool TChunkFileWriter::IsCloseDemanded() const
{
    YT_UNIMPLEMENTED();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NIO

