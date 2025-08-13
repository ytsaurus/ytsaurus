#pragma once

#include <contrib/ydb/public/api/grpc/ydb_coordination_v1.grpc.pb.h>

#include <contrib/ydb/library/grpc/server/grpc_server.h>

#include <contrib/ydb/library/actors/core/actorsystem_fwd.h>

#include <util/generic/hash_set.h>

#include <contrib/ydb/core/grpc_services/base/base_service.h>
#include <contrib/ydb/core/grpc_services/grpc_helper.h>


namespace NKikimr {
namespace NKesus {

class TKesusGRpcService
    : public ::NKikimr::NGRpcService::TGrpcServiceBase<Ydb::Coordination::V1::CoordinationService>
{
    using TBase = ::NKikimr::NGRpcService::TGrpcServiceBase<Ydb::Coordination::V1::CoordinationService>;

    class TContextBase;
    class TSessionContext;

public:
    TKesusGRpcService(
        NActors::TActorSystem* system,
        TIntrusivePtr<::NMonitoring::TDynamicCounters> counters,
        TIntrusivePtr<NGRpcService::TInFlightLimiterRegistry> limiterRegistry,
        const NActors::TActorId& proxyId,
        bool rlAllowed);

private:
    void SetupIncomingRequests(NYdbGrpc::TLoggerPtr logger);

private:
    TIntrusivePtr<NGRpcService::TInFlightLimiterRegistry> LimiterRegistry_;
};

}
}
