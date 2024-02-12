#pragma once

#include "block_device.h"
#include "config.h"

#include <yt/yt/ytlib/api/native/public.h>

namespace NYT::NNbd {

////////////////////////////////////////////////////////////////////////////////

IBlockDevicePtr CreateCypressFileBlockDevice(
    TString exportId,
    TCypressFileBlockDeviceConfigPtr config,
    NApi::NNative::IClientPtr client,
    IInvokerPtr invoker,
    NLogging::TLogger logger);

////////////////////////////////////////////////////////////////////////////////

IBlockDevicePtr CreateCypressFileBlockDevice(
    TString exportId,
    const ::google::protobuf::RepeatedPtrField<::NYT::NChunkClient::NProto::TChunkSpec>& chunkSpecs,
    TCypressFileBlockDeviceConfigPtr config,
    NConcurrency::IThroughputThrottlerPtr inThrottler,
    NConcurrency::IThroughputThrottlerPtr outRpsThrottler,
    NApi::NNative::IClientPtr client,
    IInvokerPtr invoker,
    NLogging::TLogger logger);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NNbd
