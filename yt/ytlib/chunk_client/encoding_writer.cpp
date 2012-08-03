#include "stdafx.h"
#include "encoding_writer.h"

#include "config.h"
#include "private.h"
#include "async_writer.h"

namespace NYT {
namespace NChunkClient {

///////////////////////////////////////////////////////////////////////////////

static NLog::TLogger& Logger = ChunkWriterLogger;

///////////////////////////////////////////////////////////////////////////////

TEncodingWriter::TEncodingWriter(TEncodingWriterConfigPtr config, IAsyncWriterPtr asyncWriter)
    : Config(config)
    , AsyncWriter(asyncWriter)
    , Semaphore(Config->EncodeWindowSize)
    , Codec(GetCodec(Config->CodecId))
    , UncompressedSize_(0)
    , CompressedSize_(0)
    , CompressionRatio_(config->DefaultCompressionRatio)
    , WritePending(
        BIND(
            &TEncodingWriter::WritePendingBlocks, 
            MakeWeak(this))
        .Via(WriterThread->GetInvoker()))
{}

void TEncodingWriter::WriteBlock(const TSharedRef& block)
{
    Semaphore.Acquire(block.Size());
    CompressionThread->GetInvoker()->Invoke(BIND(
        &TEncodingWriter::DoCompressBlock, 
        MakeStrong(this),
        block));
}

void TEncodingWriter::WriteBlock(std::vector<TSharedRef>&& vectorizedBlock)
{
    FOREACH (const auto& part, vectorizedBlock) {
        Semaphore.Acquire(part.Size());
    }

    CompressionThread->GetInvoker()->Invoke(BIND(
        &TEncodingWriter::DoCompressVector, 
        MakeWeak(this),
        MoveRV(vectorizedBlock)));
}

void TEncodingWriter::DoCompressBlock(const TSharedRef& block)
{
    auto compressedBlock = Codec->Compress(block);

    UncompressedSize_ += block.Size();
    CompressedSize_ += compressedBlock.Size();

    LOG_DEBUG("Compressing block");

    int delta = block.Size();
    delta -= compressedBlock.Size();

    ProcessCompressedBlock(compressedBlock, delta);
}

void TEncodingWriter::DoCompressVector(const std::vector<TSharedRef>& vectorizedBlock)
{
    auto compressedBlock = Codec->Compress(vectorizedBlock);

    auto oldSize = GetUncompressedSize();
    FOREACH (const auto& part, vectorizedBlock) {
        UncompressedSize_ += part.Size();
    }

    LOG_DEBUG("Compressing block");

    CompressedSize_ += compressedBlock.Size();

    int delta = UncompressedSize_ - oldSize;
    delta -= compressedBlock.Size();

    ProcessCompressedBlock(compressedBlock, delta);
}

void TEncodingWriter::ProcessCompressedBlock(const TSharedRef& block, int delta)
{
    CompressionRatio_ = double(CompressedSize_) / UncompressedSize_;

    if (delta > 0) {
        Semaphore.Release(delta);
    } else {
        Semaphore.Acquire(-delta);
    }

    PendingBlocks.push_back(block);
    LOG_DEBUG("Pending block added");

    if (PendingBlocks.size() == 1) {
        AsyncWriter->GetReadyEvent().Subscribe(WritePending);
    }
}

void TEncodingWriter::WritePendingBlocks(TError error)
{
    if (!error.IsOK()) {
        State.Fail(error);
        return;
    }

    while (!PendingBlocks.empty()) {
        LOG_DEBUG("Writing pending block");
        auto& front = PendingBlocks.front();
        if (AsyncWriter->TryWriteBlock(front)) {
            Semaphore.Release(front.Size());
            PendingBlocks.pop_front();
        } else {
            AsyncWriter->GetReadyEvent().Subscribe(WritePending);
            return;
        };
    }
}

bool TEncodingWriter::IsReady() const
{
    return Semaphore.IsReady() && State.IsActive();
}

TAsyncError TEncodingWriter::GetReadyEvent()
{
    if (!Semaphore.IsReady()) {
        State.StartOperation();

        auto this_ = MakeStrong(this);
        Semaphore.GetReadyEvent().Subscribe(BIND([=] () {
            this_->State.FinishOperation();
        }));
    }

    return State.GetOperationError();
}

TAsyncError TEncodingWriter::AsyncFlush()
{
    State.StartOperation();

    auto this_ = MakeStrong(this);
    Semaphore.GetFreeEvent().Subscribe(BIND([=] () {
        this_->State.FinishOperation();
    }));

    return State.GetOperationError();
}

TEncodingWriter::~TEncodingWriter()
{ }

///////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT
