#pragma once

#include "public.h"
#include "chunk_meta_extensions.h"

#include <yt/ytlib/api/native/public.h>

#include <yt/client/chunk_client/chunk_replica.h>
#include <yt/ytlib/chunk_client/public.h>

#include <yt/core/compression/codec.h>

#include <yt/core/logging/log.h>

#include <yt/core/rpc/public.h>

namespace NYT {
namespace NFileClient {

////////////////////////////////////////////////////////////////////////////////

//! A client-side facade for writing files.
/*!
 *  The client must call #Open and then feed the data in by calling #Write.
 *  Finally it must call #Finish.
 */
class TFileChunkOutput
    : public IOutputStream
{
public:
    //! Initializes an instance.
    TFileChunkOutput(
        NApi::TFileWriterConfigPtr config,
        NChunkClient::TMultiChunkWriterOptionsPtr options,
        NApi::NNative::IClientPtr client,
        const NObjectClient::TTransactionId& transactionId,
        NChunkClient::TTrafficMeterPtr trafficMeter = nullptr,
        i64 sizeLimit = std::numeric_limits<i64>::max());

    NChunkClient::TChunkId GetChunkId() const;

protected:
    //! Adds another portion of data.
    /*!
     *  This portion does not necessary makes up a block. The writer maintains an internal buffer
     *  and splits the input data into parts of equal size (see #TConfig::BlockSize).
     */
    virtual void DoWrite(const void* buf, size_t len) override;

    //! Closes the writer.
    virtual void DoFinish() override;

    NLogging::TLogger Logger;

private:
    void FlushBlock();

    const NApi::TFileWriterConfigPtr Config_;
    const NChunkClient::TMultiChunkWriterOptionsPtr Options_;
    const NApi::NNative::IClientPtr Client_;
    const NObjectClient::TTransactionId TransactionId_;
    const i64 SizeLimit_;

    NChunkClient::IChunkWriterPtr ConfirmingChunkWriter_;
    IFileChunkWriterPtr FileChunkWriter_;

    i64 GetSize() const;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NFileClient
} // namespace NYT
