#pragma once

#include <contrib/ydb/library/actors/core/actorsystem_fwd.h>
#include <contrib/ydb/library/grpc/server/grpc_server.h>
#include <contrib/ydb/core/grpc_services/base/base_service.h>

#include <contrib/ydb/public/api/grpc/draft/ydb_tablet_v1.grpc.pb.h>

namespace NKikimr::NGRpcService {

class TGRpcYdbTabletService
    : public TGrpcServiceBase<Ydb::Tablet::V1::TabletService>
{
    using TBase = TGrpcServiceBase<Ydb::Tablet::V1::TabletService>;

public:
    TGRpcYdbTabletService(
        NActors::TActorSystem *system,
        TIntrusivePtr<::NMonitoring::TDynamicCounters> counters,
        const TVector<NActors::TActorId>& proxies,
        bool rlAllowed,
        size_t handlersPerCompletionQueue = 1);

private:
    void SetupIncomingRequests(NYdbGrpc::TLoggerPtr logger);

private:
    const size_t HandlersPerCompletionQueue;
};

} // namespace NKikimr::NGRpcService
