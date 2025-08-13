#pragma once

#include <contrib/ydb/public/api/grpc/ydb_operation_v1.grpc.pb.h>
#include <contrib/ydb/library/grpc/server/grpc_server.h>
#include <contrib/ydb/core/grpc_services/base/base_service.h>

namespace NKikimr {
namespace NGRpcService {

class TGRpcOperationService
    : public TGrpcServiceBase<Ydb::Operation::V1::OperationService>
{
public:
    using TGrpcServiceBase<Ydb::Operation::V1::OperationService>::TGrpcServiceBase;
private:
    void SetupIncomingRequests(NYdbGrpc::TLoggerPtr logger);
};

} // namespace NGRpcService
} // namespace NKikimr
