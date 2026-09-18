#pragma once

#include <yt/yql/providers/dq/actors/yt/resource_manager.h>

#include <contrib/ydb/library/yql/providers/dq/api/grpc/api.grpc.pb.h>

#include <functional>

#include <util/generic/ptr.h>
#include <util/generic/vector.h>

namespace NYql {

using TDqWarmupIsReadyFn = std::function<bool(
    const TString& host,
    int port,
    const TVector<Yql::DqsProto::TFile>& files,
    bool udfOnly)>;

class IDqCliqueWarmupSession: public TThrRefBase {
public:
    // Drives clique artifact warmup via IsReady on clique nodes; returns true once all cliques are ready.
    virtual bool WarmupCliques(const TVector<Yql::DqsProto::TFile>& files) = 0;
    virtual void Stop() = 0;
};

using IDqCliqueWarmupSessionPtr = TIntrusivePtr<IDqCliqueWarmupSession>;

IDqCliqueWarmupSessionPtr CreateDqCliqueWarmupSession(
    TDqWarmupIsReadyFn isReadyFn,
    TVector<TResourceManagerOptions> ytBackends);

} // namespace NYql
