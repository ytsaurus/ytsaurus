#include "stdafx.h"
#include "encoding_writer.h"

#include "config.h"
#include "private.h"
#include "dispatcher.h"
#include "async_writer.h"

namespace NYT {
namespace NChunkClient {

///////////////////////////////////////////////////////////////////////////////

static NLog::TLogger& Logger = ChunkWriterLogger;

///////////////////////////////////////////////////////////////////////////////

TEncodingWriter::TEncodingWriter(
    TEncodingWriterConfigPtr config,
    TEncodingWriterOptionsPtr options,
    IAsyncWriterPtr asyncWriter)
    : UncompressedSize_(0)
    , CompressedSize_(0)
    , CompressionRatio_(config->ExpectedCompressionRatio)
    , Config(config)
    , AsyncWriter(asyncWriter)
    , CompressionInvoker(CreateSerializedInvoker(TDispatcher::Get()->GetCompressionInvoker()))
    , Semaphore(Config->EncodeWindowSize)
    , Codec(NCompression::GetCodec(options->Codec))
    , WritePending(
        BIND(&TEncodingWriter::WritePendingBlocks, MakeWeak(this))
            .Via(CompressionInvoker))
{}

void TEncodingWriter::WriteBlock(const TSharedRef& block)
{
    Semaphore.Acquire(block.Size());
    CompressionInvoker->Invoke(BIND(
        &TEncodingWriter::DoCompressBlock,
        MakeStrong(this),
        block));
}

void TEncodingWriter::WriteBlock(std::vector<TSharedRef>&& vectorizedBlock)
{
    FOREACH (const auto& part, vectorizedBlock) {
        Semaphore.Acquire(part.Size());
    }
    CompressionInvoker->Invoke(BIND(
        &TEncodingWriter::DoCompressVector,
        MakeWeak(this),
        std::move(vectorizedBlock)));
}

void TEncodingWriter::DoCompressBlock(const TSharedRef& block)
{
    auto compressedBlock = Codec->Compress(block);

    UncompressedSize_ += block.Size();
    CompressedSize_ += compressedBlock.Size();

    LOG_DEBUG("Compressing block");

    int sizeToRelease = -compressedBlock.Size();

    if (!Config->VerifyCompression) {
        // We immediately release original data.
        sizeToRelease += block.Size();
    }

    ProcessCompressedBlock(compressedBlock, sizeToRelease);

    if (Config->VerifyCompression) {
        TDispatcher::Get()->GetCompressionInvoker()->Invoke(BIND(
            &TEncodingWriter::VerifyBlock,
            MakeWeak(this),
            block,
            compressedBlock));
    }
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

    i64 sizeToRelease = -compressedBlock.Size();

    if (!Config->VerifyCompression) {
        // We immediately release original data.
        sizeToRelease += UncompressedSize_ - oldSize;
    }

    ProcessCompressedBlock(compressedBlock, sizeToRelease);

    if (Config->VerifyCompression) {
        TDispatcher::Get()->GetCompressionInvoker()->Invoke(BIND(
            &TEncodingWriter::VerifyVector,
            MakeWeak(this),
            vectorizedBlock,
            compressedBlock));
    }
}

void TEncodingWriter::VerifyVector(
    const std::vector<TSharedRef>& origin,
    const TSharedRef& compressedBlock)
{
    auto decompressedBlock = Codec->Decompress(compressedBlock);

    char* begin = decompressedBlock.Begin();
    FOREACH (const auto& block, origin) {
        LOG_FATAL_IF(
            !TRef::AreBitwiseEqual(TRef(begin, block.Size()), block),
            "Compression verification failed");
        begin += block.Size();
        Semaphore.Release(block.Size());
    }
}

void TEncodingWriter::VerifyBlock(
    const TSharedRef& origin,
    const TSharedRef& compressedBlock)
{
    auto decompressedBlock = Codec->Decompress(compressedBlock);
    LOG_FATAL_IF(
        !TRef::AreBitwiseEqual(decompressedBlock, origin),
        "Compression verification failed");
    Semaphore.Release(origin.Size());
}

void TEncodingWriter::ProcessCompressedBlock(const TSharedRef& block, i64 sizeToRelease)
{
    CompressionRatio_ = double(CompressedSize_) / UncompressedSize_;

    if (sizeToRelease > 0) {
        Semaphore.Release(sizeToRelease);
    } else {
        Semaphore.Acquire(-sizeToRelease);
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
