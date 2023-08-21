#pragma once

#include "public.h"

#include <yt/yt/core/misc/blob_output.h>

#include <yt/yt/ytlib/file_client/file_chunk_output.h>

namespace NYT::NJobProxy {

////////////////////////////////////////////////////////////////////////////////

class TTailBuffer
    : public IOutputStream
{
public:
    explicit TTailBuffer(i64 sizeLimit);

    bool IsOverflowed() const;
    void SaveTo(IOutputStream* out) const;

private:
    void DoWrite(const void* buf, size_t len) override;

private:
    TBlob RingBuffer_;
    i64 Position_ = 0;
    bool BufferOverflowed_ = false;
};

////////////////////////////////////////////////////////////////////////////////

class TStderrWriter
    : public IOutputStream
{
public:
    explicit TStderrWriter(
        size_t sizeLimit = std::numeric_limits<size_t>::max());

    // COMPAT(ignat)
    NChunkClient::TChunkId GetChunkId() const;
    void Upload(
        NApi::TFileWriterConfigPtr config,
        NChunkClient::TMultiChunkWriterOptionsPtr options,
        NApi::NNative::IClientPtr client,
        NObjectClient::TTransactionId transactionId,
        NChunkClient::TTrafficMeterPtr trafficMeter,
        NConcurrency::IThroughputThrottlerPtr throttler);

    size_t GetCurrentSize() const;
    TString GetCurrentData() const;

private:
    void DoWrite(const void* buf, size_t len) override;

    void SaveCurrentDataTo(IOutputStream* output) const;

private:
    // Limit for the head or for the tail part.
    const size_t PartLimit_;

    TBlobOutput Head_;
    std::optional<TTailBuffer> Tail_;

    NChunkClient::TChunkId ChunkId_ = NChunkClient::NullChunkId;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NJobProxy
