#include "config.h"

#include <yt/core/ytree/fluent.h>

namespace NYT {
namespace NHttpProxy {

using namespace NYTree;
using namespace NAuth;

////////////////////////////////////////////////////////////////////////////////

INodePtr ConvertAuthFromLegacyConfig(const INodePtr& legacyConfig)
{
    if (!legacyConfig->AsMap()->FindChild("authentication")) {
        return BuildYsonNodeFluently().BeginMap().EndMap();
    }

    auto legacyAuthentication = legacyConfig->AsMap()->GetChild("authentication")->AsMap();
    auto grant = legacyAuthentication->FindChild("grant");
    if (!grant) {
        grant = ConvertToNode("");
    }

    auto config = BuildYsonNodeFluently().BeginMap()
        .Item("auth").BeginMap()
            .Item("enable_authentication").Value(legacyAuthentication->GetChild("enable"))
            .Item("blackbox_service").BeginMap().EndMap()
            .Item("cypress_token_authenticator").BeginMap().EndMap()
            .Item("blackbox_token_authenticator").BeginMap()
                .Item("scope").Value(grant)
            .EndMap()
            .Item("blackbox_cookie_authenticator").BeginMap().EndMap()
        .EndMap().EndMap();

    auto csrfSecret = legacyAuthentication->FindChild("csrf_secret");
    if (csrfSecret) {
        config->AsMap()->AddChild("csrf_secret", CloneNode(csrfSecret));
    }

    auto csrfTokenTtl = legacyAuthentication->FindChild("csrf_token_ttl");
    if (csrfTokenTtl) {
        config->AsMap()->AddChild("csrf_token_ttl", CloneNode(csrfTokenTtl));
    }
    return config;
}

INodePtr ConvertHttpsFromLegacyConfig(const INodePtr& legacyConfig)
{
    auto sslPort = legacyConfig->AsMap()->FindChild("ssl_port");
    if (!sslPort) {
        return BuildYsonNodeFluently().BeginMap().EndMap();
    }

    return BuildYsonNodeFluently().BeginMap()
        .Item("https_server").BeginMap()
            .Item("port").Value(sslPort)
            .Item("credentials")
                .BeginMap()
                    .Item("private_key").BeginMap()
                        .Item("file_name").Value(legacyConfig->AsMap()->GetChild("ssl_key"))
                    .EndMap()
                    .Item("cert_chain").BeginMap()
                        .Item("file_name").Value(legacyConfig->AsMap()->GetChild("ssl_certificate"))
                    .EndMap()
                .EndMap()
            .EndMap()
        .EndMap();
}

INodePtr ConvertFromLegacyConfig(const INodePtr& legacyConfig)
{
    auto redirect = legacyConfig->AsMap()->FindChild("redirect");
    if (redirect) {
        redirect = redirect->AsList()->GetChild(0)->AsList()->GetChild(1);
    }

    auto proxy = legacyConfig->AsMap()->GetChild("proxy")->AsMap();

    auto config =  BuildYsonNodeFluently()
        .BeginMap()
            .Item("port").Value(legacyConfig->AsMap()->GetChild("port"))
            .Item("coordinator").Value(legacyConfig->AsMap()->GetChild("coordination"))
            .Item("logging").Value(proxy->GetChild("logging"))
            .Item("driver").Value(proxy->GetChild("driver"))
            .DoIf(static_cast<bool>(redirect), [&] (auto fluent) {
                fluent.Item("ui_redirect_url").Value(redirect);
            })
        .EndMap();

    if (auto monitoringPort = legacyConfig->AsMap()->FindChild("monitoring_port")) {
        config->AsMap()->AddChild("monitoring_port", CloneNode(monitoringPort));
    }
        
    if (auto node = legacyConfig->AsMap()->FindChild("cypress_annotations")) {
        config->AsMap()->AddChild("cypress_annotations", CloneNode(node));
    }

    if (auto node = proxy->FindChild("address_resolver")) {
        config->AsMap()->AddChild("address_resolver", CloneNode(node));
    }

    if (auto node = proxy->FindChild("chunk_client_dispatcher")) {
        config->AsMap()->AddChild("chunk_client_dispatcher", CloneNode(node));
    }

    if (auto node = legacyConfig->AsMap()->FindChild("show_ports")) {
        config->AsMap()->GetChild("coordinator")->AsMap()->AddChild("show_ports", CloneNode(node));
    }

    config = PatchNode(config, ConvertAuthFromLegacyConfig(legacyConfig));
    config = PatchNode(config, ConvertHttpsFromLegacyConfig(legacyConfig));
        
    return config;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NHttpProxy
} // namespace NYT
