#pragma once

#include <yt/server/clickhouse_server/interop/auth_clique.h>

#include <yt/ytlib/api/public.h>

namespace NYT {
namespace NClickHouse {

////////////////////////////////////////////////////////////////////////////////

NInterop::ICliqueAuthorizationManagerPtr CreateCliqueAuthorizationManager(NApi::IClientPtr client, TString cliqueId);

////////////////////////////////////////////////////////////////////////////////

}   // namespace NClickHouse
}   // namespace NYT
