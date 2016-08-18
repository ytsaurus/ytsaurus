#pragma once

#include <yt/ytlib/file_client/file_chunk_output.h>

namespace NYT {
namespace NJobProxy {

////////////////////////////////////////////////////////////////////////////////

class TStderrWriter
    : public NFileClient::TFileChunkOutput
{
public:
    TStderrWriter(
        NApi::TFileWriterConfigPtr config,
        NChunkClient::TMultiChunkWriterOptionsPtr options,
        NApi::IClientPtr client,
        const NObjectClient::TTransactionId& transactionId,
        i64 sizeLimit = std::numeric_limits<i64>::max())
        : TFileChunkOutput(
            config,
            options,
            client,
            transactionId)
        , SizeLimit_(sizeLimit)
    { }

private:
    virtual void DoWrite(const void* buf, size_t len) override;
    
    virtual void DoFinish() override;

    const i64 SizeLimit_;

    std::deque<TBlob> Blobs_;
    i64 AccumulatedSize_ = 0;
    i64 WrittenSize_ = 0;
    bool HasSkippedData_ = false;
    bool Failed_ = false;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NJobProxy
} // namespace NYT
