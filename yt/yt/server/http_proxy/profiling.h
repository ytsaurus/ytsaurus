#pragma once

#include "api.h"
#include "public.h"

#include <yt/yt/core/http/public.h>

namespace NYT::NHttpProxy {

////////////////////////////////////////////////////////////////////////////////

NConcurrency::IFlushableAsyncOutputStreamPtr CreateProfilingOutpuStream(
    NHttp::IResponseWriterPtr underlying,
    TApiPtr api,
    const std::string& user,
    const NNet::TNetworkAddress& clientAddress,
    const std::optional<NFormats::TFormat>& outputFormat,
    const std::optional<NHttp::TContentEncoding>& outputCompression);

NConcurrency::IAsyncZeroCopyInputStreamPtr CreateProfilingInputStream(
    NHttp::IRequestPtr underlying,
    TApiPtr api,
    const std::string& user,
    const NNet::TNetworkAddress& clientAddress,
    const std::optional<NFormats::TFormat>& inputFormat,
    const std::optional<NHttp::TContentEncoding>& inputCompression);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHttpProxy
