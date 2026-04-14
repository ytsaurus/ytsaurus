#pragma once

#include <contrib/ydb/library/grpc/server/grpc_server.h>
#include <contrib/ydb/public/api/grpc/draft/ydb_datastreams_v1.grpc.pb.h>
#include <contrib/ydb/core/grpc_services/base/base_service.h>

namespace NKikimr::NGRpcService {

    class TGRpcDataStreamsService : public TGrpcServiceBase<Ydb::DataStreams::V1::DataStreamsService>
    {
    public:
        using TGrpcServiceBase<Ydb::DataStreams::V1::DataStreamsService>::TGrpcServiceBase;
    private:
        void SetupIncomingRequests(NYdbGrpc::TLoggerPtr logger);
    };

}
