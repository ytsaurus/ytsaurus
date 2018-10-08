#pragma once

#include <yt/core/misc/public.h>

namespace NYT {
namespace NHttpProxy {

////////////////////////////////////////////////////////////////////////////////

class TBootstrap;

DECLARE_REFCOUNTED_STRUCT(TLiveness)
DECLARE_REFCOUNTED_STRUCT(TProxyEntry)

DECLARE_REFCOUNTED_CLASS(TProxyConfig)
DECLARE_REFCOUNTED_CLASS(TCoordinatorConfig)
DECLARE_REFCOUNTED_CLASS(TApiConfig)

DECLARE_REFCOUNTED_CLASS(TApi)
DECLARE_REFCOUNTED_CLASS(TCoordinator)
DECLARE_REFCOUNTED_CLASS(THostsHandler)
DECLARE_REFCOUNTED_CLASS(TPingHandler)
DECLARE_REFCOUNTED_CLASS(TDiscoverVersionsHandler);
DECLARE_REFCOUNTED_CLASS(THttpAuthenticator)

DECLARE_REFCOUNTED_CLASS(TSharedRefOutputStream)

DECLARE_REFCOUNTED_CLASS(TContext)

DEFINE_ENUM(EContentEncoding,
    (None)
    (Gzip)
    (Deflate)
    (Lzop)
    (Lzo)
    (Lzf)
    (Snappy)
    (Brotli)
);

////////////////////////////////////////////////////////////////////////////////

static constexpr size_t DefaultStreamBufferSize = 32_KB;

////////////////////////////////////////////////////////////////////////////////

} // namespace NHttpProxy
} // namespace NYT
