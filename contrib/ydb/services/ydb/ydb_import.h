#pragma once

#include <contrib/ydb/library/grpc/server/grpc_server.h>
#include <contrib/ydb/public/api/grpc/ydb_import_v1.grpc.pb.h>
#include <contrib/ydb/core/grpc_services/base/base_service.h>


namespace NKikimr {
namespace NGRpcService {

class TGRpcYdbImportService
    : public TGrpcServiceBase<Ydb::Import::V1::ImportService>
{
public:
    using TGrpcServiceBase<Ydb::Import::V1::ImportService>::TGrpcServiceBase;

private:
    void SetupIncomingRequests(NYdbGrpc::TLoggerPtr logger);
};

} // namespace NGRpcService
} // namespace NKikimr
