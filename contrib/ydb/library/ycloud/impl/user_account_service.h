#pragma once
#include <contrib/ydb/library/ycloud/api/user_account_service.h>
#include <contrib/ydb/library/grpc/actor_client/grpc_service_client.h>

namespace NCloud {

using namespace NKikimr;

struct TUserAccountServiceSettings : NGrpcActorClient::TGrpcClientSettings {};

IActor* CreateUserAccountService(const TUserAccountServiceSettings& settings);

inline IActor* CreateUserAccountService(const TString& endpoint) {
    TUserAccountServiceSettings settings;
    settings.Endpoint = endpoint;
    return CreateUserAccountService(settings);
}

}
