#pragma once

#include <yt/server/clickhouse/interop/api.h>

#include <yt/ytlib/api/native/public.h>

namespace NYT {
namespace NClickHouse {

////////////////////////////////////////////////////////////////////////////////

NInterop::ICoordinationServicePtr CreateCoordinationService(
    NApi::NNative::IConnectionPtr connection,
    TString cliqueId);

}   // namespace NClickHouse
}   // namespace NYT
