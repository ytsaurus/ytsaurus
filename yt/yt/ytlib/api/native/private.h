#pragma once

#include <yt/yt/client/api/private.h>

namespace NYT::NApi::NNative {

////////////////////////////////////////////////////////////////////////////////

class TListOperationsCountingFilter;
class TListOperationsFilter;

struct TReplicaSynchronicity;
using TReplicaSynchronicityList = std::vector<TReplicaSynchronicity>;

DECLARE_REFCOUNTED_STRUCT(ITypeHandler)

DECLARE_REFCOUNTED_CLASS(TClient)

////////////////////////////////////////////////////////////////////////////////

YT_DEFINE_GLOBAL(const NLogging::TLogger, TvmSynchronizerLogger, "TvmSynchronizer");

YT_DEFINE_GLOBAL(const NLogging::TLogger, NativeConnectionLogger, "NativeConnection");

inline static constexpr std::string_view UpstreamReplicaIdAttributeName = "upstream_replica_id"sv;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi::NNative

