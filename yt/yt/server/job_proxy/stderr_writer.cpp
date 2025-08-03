#include "stderr_writer.h"

#include "private.h"

#include <yt/yt/ytlib/chunk_client/data_sink.h>

namespace NYT::NJobProxy {

using namespace NChunkClient;
using namespace NFileClient;

////////////////////////////////////////////////////////////////////////////////

constinit const auto Logger = JobProxyLogger;

////////////////////////////////////////////////////////////////////////////////

namespace {

class TSizeCountingStream
    : public IOutputStream
{
public:
    ui64 GetSize() const
    {
        return Size_;
    }

private:
    void DoWrite(const void* /*buf*/, size_t size) override
    {
        Size_ += size;
    }

private:
    ui64 Size_ = 0;
};

} // namespace

////////////////////////////////////////////////////////////////////////////////

TTailBuffer::TTailBuffer(i64 sizeLimit)
    : RingBuffer_(GetRefCountedTypeCookie<TDefaultBlobTag>(), sizeLimit)
{ }

bool TTailBuffer::IsOverflowed() const
{
    return BufferOverflowed_;
}

void TTailBuffer::SaveTo(IOutputStream* out) const
{
    if (BufferOverflowed_) {
        out->Write(RingBuffer_.Begin() + Position_, RingBuffer_.Size() - Position_);
        out->Write(RingBuffer_.Begin(), Position_);
    } else {
        out->Write(RingBuffer_.Begin(), Position_);
    }
}

void TTailBuffer::DoWrite(const void* buf_, size_t len)
{
    const char* buf = static_cast<const char*>(buf_);
    if (Position_ + len <= RingBuffer_.Size()) {
        std::copy(buf, buf + len, RingBuffer_.Begin() + Position_);
        Position_ += len;
    } else {
        BufferOverflowed_ = true;
        if (len >= RingBuffer_.Size()) {
            Position_ = 0;
            std::copy(buf + len - RingBuffer_.Size(), buf + len, RingBuffer_.Begin());
        } else {
            std::copy(buf, buf + RingBuffer_.Size() - Position_, RingBuffer_.Begin() + Position_);
            std::copy(buf + RingBuffer_.Size() - Position_, buf + len, RingBuffer_.Begin());
            Position_ += len - RingBuffer_.Size();
        }
    }
}

////////////////////////////////////////////////////////////////////////////////


TStderrWriter::TStderrWriter(
    size_t sizeLimit)
    // Limit for tail and head are the half of total limit.
    : PartLimit_(sizeLimit / 2)
    , Head_(sizeLimit / 2)
{ }

NChunkClient::TChunkId TStderrWriter::GetChunkId() const
{
    return ChunkId_;
}

void TStderrWriter::Upload(
    NApi::TFileWriterConfigPtr config,
    NChunkClient::TMultiChunkWriterOptionsPtr options,
    NApi::NNative::IClientPtr client,
    NObjectClient::TTransactionId transactionId,
    NChunkClient::TTrafficMeterPtr trafficMeter,
    NConcurrency::IThroughputThrottlerPtr throttler)
{
    try {
        // NB. We use empty data sink here, so the details like object path and account are not present in IO tags.
        // That's because this code is legacy anyway and not worth covering with IO tags.
        TFileChunkOutput fileChunkOutput(
            config,
            options,
            client,
            transactionId,
            TDataSink(),
            trafficMeter,
            throttler,
            /*writeBlocksOptions*/ {});
        SaveCurrentDataTo(&fileChunkOutput);
        fileChunkOutput.Finish();
        ChunkId_ = fileChunkOutput.GetChunkId();
    } catch (const std::exception& ex) {
        YT_LOG_WARNING(ex, "Writing stderr data to chunk failed");
    }
}

void TStderrWriter::DoWrite(const void* buf_, size_t len)
{
    TotalSize_ += len;

    const char* buf = static_cast<const char*>(buf_);

    if (Head_.Size() < PartLimit_) {
        const size_t toWrite = std::min(PartLimit_ - Head_.Size(), len);
        Head_.Write(buf, toWrite);
        YT_ASSERT(Head_.Size() <= PartLimit_);
        if (toWrite == len) {
            return;
        } else {
            buf += toWrite;
            len -= toWrite;
        }
    }

    if (!Tail_) {
        Tail_.emplace(PartLimit_);
    }
    Tail_->Write(buf, len);
}

NApi::TGetJobStderrResponse TStderrWriter::GetCurrentData(const NApi::TGetJobStderrOptions& options) const
{
    TStringStream stringStream;
    stringStream.Reserve(GetCurrentSize());
    auto limit = options.Limit.value_or(0);
    auto offset = options.Offset.value_or(0);
    SaveCurrentDataTo(&stringStream, offset || limit);
    auto data = stringStream.Str();
    auto endOffset = TotalSize_;
    const i64 currentFirstAbsolutePos = TotalSize_ > std::ssize(data) ? TotalSize_ - data.size() : 0;
    i64 dataBeginOffset = 0;
    if (offset >= 0) {
        if (offset >= currentFirstAbsolutePos) {
            dataBeginOffset = offset - currentFirstAbsolutePos;
        } else {
            dataBeginOffset = 0;
            if (limit > 0) {
                limit = limit - (currentFirstAbsolutePos - offset);
                if (limit < 0) {
                    return {
                        .Data = TSharedRef{},
                        .TotalSize = TotalSize_,
                        .EndOffset = offset + options.Limit.value_or(0),
                    };
                }
            }
        }
    }
    if (offset || limit) {
        if (dataBeginOffset >= std::ssize(data)) {
            data = "";
            endOffset = 0;
        } else {
            data = data.substr(dataBeginOffset, limit ? limit : data.npos);
            endOffset = currentFirstAbsolutePos + dataBeginOffset + data.size();
        }
    }
    return {
        .Data = TSharedRef::FromString(data),
        .TotalSize = TotalSize_,
        .EndOffset = endOffset,
    };
}

size_t TStderrWriter::GetCurrentSize() const
{
    // Logic of filling stream with data is little bit complicated,
    // so it's easier to use special stream that will count bytes for us.
    TSizeCountingStream sizeCounter;
    SaveCurrentDataTo(&sizeCounter);
    return sizeCounter.GetSize();
}

void TStderrWriter::SaveCurrentDataTo(IOutputStream* output, bool noPrefix) const
{
    if (!Tail_ || !noPrefix) {
        output->Write(Head_.Begin(), Head_.Size());
    }

    if (Tail_) {
        if (Tail_->IsOverflowed() && !noPrefix) {
            static const TStringBuf skipped = "\n...skipped...\n";
            output->Write(skipped);
        }
        Tail_->SaveTo(output);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NJobProxy
