#pragma once

#include "public.h"

#include <yt/server/clickhouse/interop/api.h>

#include <yt/ytlib/api/public.h>

namespace NYT {
namespace NClickHouse {

////////////////////////////////////////////////////////////////////////////////

const NApi::TClientOptions& UnwrapAuthToken(
    const NInterop::IAuthorizationToken& token);

////////////////////////////////////////////////////////////////////////////////

NInterop::IAuthorizationTokenService* GetAuthTokenService();

}   // namespace NClickHouse
}   // namespace NYT
