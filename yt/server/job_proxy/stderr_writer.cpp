#include "stderr_writer.h"

#include "private.h"

namespace NYT {
namespace NJobProxy {

using namespace NFileClient;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = JobProxyLogger;

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
    virtual void DoWrite(const void* /*buf*/, size_t size) override
    {
        Size_ += size;
    }

private:
    ui64 Size_ = 0;
};

} // namespace

////////////////////////////////////////////////////////////////////////////////

TTailBuffer::TTailBuffer(i64 sizeLimit)
    : RingBuffer_(TDefaultBlobTag(), sizeLimit)
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

void TStderrWriter::DoWrite(const void* buf_, size_t len)
{
    const char* buf = static_cast<const char*>(buf_);

    if (Head_.Size() < PartLimit_) {
        const size_t toWrite = std::min(PartLimit_ - Head_.Size(), len);
        Head_.Write(buf, toWrite);
        Y_ASSERT(Head_.Size() <= PartLimit_);
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

TString TStderrWriter::GetCurrentData() const
{
    TStringStream stringStream;
    stringStream.Reserve(GetCurrentSize());
    SaveCurrentDataTo(&stringStream);
    return stringStream.Str();
}

size_t TStderrWriter::GetCurrentSize() const
{
    // Logic of filling stream with data is little bit complicated,
    // so it's easier to use special stream that will count bytes for us.
    TSizeCountingStream sizeCounter;
    SaveCurrentDataTo(&sizeCounter);
    return sizeCounter.GetSize();
}

void TStderrWriter::SaveCurrentDataTo(IOutputStream* output) const
{
    output->Write(Head_.Begin(), Head_.Size());

    if (Tail_) {
        if (Tail_->IsOverflowed()) {
            static const auto skipped = AsStringBuf("\n...skipped...\n");
            output->Write(skipped);
        }
        Tail_->SaveTo(output);
    }
}

void TStderrWriter::Upload(
    NApi::TFileWriterConfigPtr config,
    NChunkClient::TMultiChunkWriterOptionsPtr options,
    NApi::NNative::IClientPtr client,
    const NObjectClient::TTransactionId& transactionId,
    NChunkClient::TTrafficMeterPtr trafficMeter,
    NConcurrency::IThroughputThrottlerPtr throttler)
{
    try {
        TFileChunkOutput fileChunkOutput(
            config,
            options,
            client,
            transactionId,
            trafficMeter,
            throttler);
        SaveCurrentDataTo(&fileChunkOutput);
        fileChunkOutput.Finish();
        ChunkId_ = fileChunkOutput.GetChunkId();
    } catch (const std::exception& ex) {
        LOG_WARNING(ex, "Writing stderr data to chunk failed");
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NJobProxy
} // namespace NYT
