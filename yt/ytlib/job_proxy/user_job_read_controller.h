#pragma once

#include "public.h"

#include <yt/ytlib/api/native/public.h>

#include <yt/ytlib/chunk_client/public.h>

#include <yt/client/formats/public.h>

#include <yt/client/node_tracker_client/node_directory.h>

#include <yt/client/table_client/schemaful_reader_adapter.h>

#include <yt/core/actions/public.h>

#include <yt/core/concurrency/public.h>

#include <yt/core/logging/log.h>

#include <yt/core/misc/blob.h>
#include <yt/core/misc/common.h>

#include <vector>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

namespace NScheduler {
namespace NProto {

class TQuerySpec;

} // namespace NProto
} // namespace NScheduler

namespace NJobProxy {

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
    virtual void InterruptReader() = 0;
    virtual NChunkClient::TInterruptDescriptor GetInterruptDescriptor() const = 0;
};

DEFINE_REFCOUNTED_TYPE(IUserJobReadController);

////////////////////////////////////////////////////////////////////////////////

IUserJobReadControllerPtr CreateUserJobReadController(
        IJobSpecHelperPtr jobSpecHelper,
        NApi::NNative::IClientPtr client,
        IInvokerPtr invoker,
        NNodeTrackerClient::TNodeDescriptor nodeDescriptor,
        TClosure onNetworkRelease,
        std::optional<TString> udfDirectory,
        NChunkClient::TClientBlockReadOptions& blockReadOptions,
        NChunkClient::TTrafficMeterPtr trafficMeter,
        NConcurrency::IThroughputThrottlerPtr bandwidthThrottler,
        NConcurrency::IThroughputThrottlerPtr rpsThrottler);

////////////////////////////////////////////////////////////////////////////////

} // namespace NJobProxy
} // namespace NYT
