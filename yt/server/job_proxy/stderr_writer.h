#pragma once

#include "public.h"

#include <yt/core/misc/blob_output.h>

#include <yt/ytlib/file_client/file_chunk_output.h>

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
    virtual void DoWrite(const void* buf, size_t len) override;

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
    virtual void DoWrite(const void* buf, size_t len) override;

    void SaveCurrentDataTo(IOutputStream* output) const;

private:
    // Limit for the head or for the tail part.
    const size_t PartLimit_;

    TBlobOutput Head_;
    std::optional<TTailBuffer> Tail_;

    NChunkClient::TChunkId ChunkId_ = NChunkClient::NullChunkId;
};

////////////////////////////////////////////////////////////////////////////////

class TProfileWriter
    : public IOutputStream
{
public:
    explicit TProfileWriter(size_t sizeLimit);

    bool IsTruncated() const;
    std::pair<TString, TString> GetProfile() const;

private:
    virtual void DoWrite(const void* buf, size_t len) override;

    const size_t Limit_;
    TString Buffer_;

    bool Truncated_ = false;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NJobProxy
