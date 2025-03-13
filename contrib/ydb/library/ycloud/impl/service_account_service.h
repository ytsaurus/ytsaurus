#pragma once
#include <contrib/ydb/library/ycloud/api/service_account_service.h>
#include <contrib/ydb/library/grpc/actor_client/grpc_service_client.h>

namespace NCloud {

using namespace NKikimr;

struct TServiceAccountServiceSettings : NGrpcActorClient::TGrpcClientSettings {};

IActor* CreateServiceAccountService(const TServiceAccountServiceSettings& settings);

inline IActor* CreateServiceAccountService(const TString& endpoint) {
    TServiceAccountServiceSettings settings;
    settings.Endpoint = endpoint;
    return CreateServiceAccountService(settings);
}

}
