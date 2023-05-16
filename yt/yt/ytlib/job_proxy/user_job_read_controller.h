#pragma once

#include "public.h"

#include <yt/yt/ytlib/api/native/public.h>

#include <yt/yt/ytlib/chunk_client/public.h>

#include <yt/yt/ytlib/table_client/timing_statistics.h>

#include <yt/yt/client/formats/public.h>

#include <yt/yt/client/node_tracker_client/node_directory.h>

#include <yt/yt/client/table_client/schemaful_reader_adapter.h>

#include <yt/yt/core/actions/future.h>

#include <yt/yt/core/concurrency/public.h>

#include <yt/yt/core/misc/blob.h>

namespace NYT::NJobProxy {

////////////////////////////////////////////////////////////////////////////////

struct IUserJobReadController
    : public TRefCounted
{
    //! Returns closure that launches data transfer to given async output.
    virtual TCallback<TFuture<void>()> PrepareJobInputTransfer(const NConcurrency::IAsyncOutputStreamPtr& asyncOutput) = 0;

    virtual double GetProgress() const = 0;
    virtual TFuture<std::vector<TBlob>> GetInputContext() const = 0;
    virtual std::vector<NChunkClient::TChunkId> GetFailedChunkIds() const = 0;
    virtual std::optional<NChunkClient::NProto::TDataStatistics> GetDataStatistics() const = 0;
    virtual std::optional<NChunkClient::TCodecStatistics> GetDecompressionStatistics() const = 0;
    virtual std::optional<NTableClient::TTimingStatistics> GetTimingStatistics() const = 0;
    virtual void InterruptReader() = 0;
    virtual NChunkClient::TInterruptDescriptor GetInterruptDescriptor() const = 0;
};

DEFINE_REFCOUNTED_TYPE(IUserJobReadController)

////////////////////////////////////////////////////////////////////////////////

IUserJobReadControllerPtr CreateUserJobReadController(
    IJobSpecHelperPtr jobSpecHelper,
    NChunkClient::TChunkReaderHostPtr chunkReaderHost,
    IInvokerPtr invoker,
    TClosure onNetworkRelease,
    std::optional<TString> udfDirectory,
    const NChunkClient::TClientChunkReadOptions& chunkReadOptions,
    TString localHostName);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NJobProxy
