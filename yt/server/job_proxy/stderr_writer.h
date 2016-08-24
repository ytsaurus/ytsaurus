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

    // Written size before head overflowed.
    i64 WrittenSize_ = 0;
    bool Failed_ = false;

    // Cyclic buffer for keeping tail.
    TBlob CyclicBuffer_;
    i64 Position_ = 0;
    bool BufferOverflowed_ = false;
    bool BufferInitialized_ = false;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NJobProxy
} // namespace NYT
