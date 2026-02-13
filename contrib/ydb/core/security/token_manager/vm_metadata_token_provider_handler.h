#pragma once

#include <contrib/ydb/library/actors/core/actor_bootstrapped.h>
#include <contrib/ydb/library/actors/http/http_proxy.h>
#include <contrib/ydb/core/protos/auth.pb.h>


namespace NKikimr::NTokenManager {

struct TTokenProviderSettings;

class TVmMetadataTokenProviderHandler : public NActors::TActorBootstrapped<TVmMetadataTokenProviderHandler> {
    using TBase = NActors::TActorBootstrapped<TVmMetadataTokenProviderHandler>;

private:
    const NActors::TActorId Sender;
    const NActors::TActorId HttpProxyId;
    const NKikimrProto::TTokenManager::TVmMetadataProvider::TVmMetadataInfo& ProviderInfo;
    const TTokenProviderSettings& Settings;

public:
    TVmMetadataTokenProviderHandler(const NActors::TActorId& sender,
                                    const NActors::TActorId& httpProxyId,
                                    const NKikimrProto::TTokenManager::TVmMetadataProvider::TVmMetadataInfo& providerInfo,
                                    const TTokenProviderSettings& settings);
    void Bootstrap();
    void StateWork(TAutoPtr<NActors::IEventHandle>& ev);

private:
    void Handle(NHttp::TEvHttpProxy::TEvHttpIncomingResponse::TPtr& ev);
};

} // NKikimr::NTokenManager
