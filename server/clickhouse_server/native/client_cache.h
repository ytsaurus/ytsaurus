#pragma once

#include "public.h"

#include <yt/ytlib/api/native/public.h>

namespace NYT {
namespace NClickHouseServer {
namespace NNative {

////////////////////////////////////////////////////////////////////////////////

struct INativeClientCache
    : public virtual TRefCounted
{
    virtual NApi::NNative::IClientPtr CreateNativeClient(
        const NApi::TClientOptions& options) = 0;
};

DEFINE_REFCOUNTED_TYPE(INativeClientCache);

////////////////////////////////////////////////////////////////////////////////

INativeClientCachePtr CreateNativeClientCache(
    TNativeClientCacheConfigPtr config,
    NApi::NNative::IConnectionPtr connection);

////////////////////////////////////////////////////////////////////////////////

} // namespace NNative
} // namespace NClickHouseServer
} // namespace NYT
