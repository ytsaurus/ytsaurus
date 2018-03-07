#pragma once

#include <yt/core/misc/blob_output.h>

#include <yt/ytlib/file_client/file_chunk_output.h>

namespace NYT {
namespace NJobProxy {

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
        NApi::INativeClientPtr client,
        const NObjectClient::TTransactionId& transactionId,
        NChunkClient::TTrafficMeterPtr trafficMeter);

    size_t GetCurrentSize() const;
    TString GetCurrentData() const;

private:
    virtual void DoWrite(const void* buf, size_t len) override;

    void SaveCurrentDataTo(IOutputStream* output) const;

private:
    // Limit for the head or for the tail part.
    const size_t PartLimit_;

    TBlobOutput Head_;
    TNullable<TTailBuffer> Tail_;

    NChunkClient::TChunkId ChunkId_ = NChunkClient::NullChunkId;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NJobProxy
} // namespace NYT
