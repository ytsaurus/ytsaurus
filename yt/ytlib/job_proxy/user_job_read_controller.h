#pragma once

#include "public.h"

#include <yt/ytlib/api/native/public.h>

#include <yt/ytlib/chunk_client/public.h>

#include <yt/ytlib/formats/public.h>

#include <yt/client/node_tracker_client/node_directory.h>

#include <yt/ytlib/table_client/schemaful_reader_adapter.h>

#include <yt/core/actions/public.h>

#include <yt/core/concurrency/public.h>

#include <yt/core/logging/log.h>

#include <yt/core/misc/blob.h>
#include <yt/core/misc/common.h>

#include <vector>
#include <yt/ytlib/chunk_client/data_statistics.h>

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
    virtual TNullable<NChunkClient::NProto::TDataStatistics> GetDataStatistics() const = 0;
    virtual TNullable<NChunkClient::TCodecStatistics> GetDecompressionStatistics() const = 0;
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
        TNullable<TString> udfDirectory,
        NChunkClient::TClientBlockReadOptions& blockReadOptions,
        NChunkClient::TTrafficMeterPtr trafficMeter);

////////////////////////////////////////////////////////////////////////////////

} // namespace NJobProxy
} // namespace NYT
