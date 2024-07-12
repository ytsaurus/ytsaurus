#include "stderr_writer.h"

#include "private.h"

#include <yt/yt/ytlib/chunk_client/data_sink.h>

namespace NYT::NJobProxy {

using namespace NChunkClient;
using namespace NFileClient;

////////////////////////////////////////////////////////////////////////////////

static constexpr auto& Logger = JobProxyLogger;

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
            throttler);
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

NApi::TPagedLog TStderrWriter::GetCurrentData(const NApi::TPagedLogReq& request) const
{
    TStringStream stringStream;
    stringStream.Reserve(GetCurrentSize());
    SaveCurrentDataTo(&stringStream, request.Offset || request.Limit);
    auto str = stringStream.Str();
    i64 endOffset = TotalSize_;
    const i64 currentFirstAbsolutePos = TotalSize_ > static_cast<i64>(str.size()) ? TotalSize_ - str.size() : 0;
    size_t first;
    i64 limit = request.Limit;
    if (request.Offset >= 0) {
        if (request.Offset >= currentFirstAbsolutePos) {
            first = request.Offset - currentFirstAbsolutePos;
        } else {
            first = 0;
            if (request.Limit > 0) {
                limit = request.Limit - (currentFirstAbsolutePos - request.Offset);
                if (limit < 0) {
                    return {.Data = "", .TotalSize = TotalSize_, .EndOffset = request.Offset + request.Limit};
                }
            }
        }
    } else {
        if (-request.Offset >= static_cast<i64>(str.size())) {
            first = 0;
        } else {
            first = str.size() + request.Offset;
        }
    }
    if (request.Offset || request.Limit) {
        if (first >= str.size()) {
            str = "";
            endOffset = 0;
        } else {
            str = str.substr(first, limit ? limit : str.npos);
            endOffset = currentFirstAbsolutePos + first + str.size();
        }
    }
    return {.Data = str, .TotalSize = TotalSize_, .EndOffset = endOffset};
}

size_t TStderrWriter::GetCurrentSize() const
{
    // Logic of filling stream with data is little bit complicated,
    // so it's easier to use special stream that will count bytes for us.
    TSizeCountingStream sizeCounter;
    SaveCurrentDataTo(&sizeCounter);
    return sizeCounter.GetSize();
}

void TStderrWriter::SaveCurrentDataTo(IOutputStream* output, bool continuous) const
{
    if (!Tail_ || !continuous) {
        output->Write(Head_.Begin(), Head_.Size());
    }

    if (Tail_) {
        if (Tail_->IsOverflowed() && !continuous) {
            static const TStringBuf skipped = "\n...skipped...\n";
            output->Write(skipped);
        }
        Tail_->SaveTo(output);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NJobProxy
