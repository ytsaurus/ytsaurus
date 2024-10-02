// AUTOMATICALLY GENERATED. DO NOT EDIT!

#include "client.h"
#include "connection.h"
#include "object_service_proxy.h"
#include "private.h"

#include <yt/yt/orm/example/client/objects/autogen/acl.h>
#include <yt/yt/orm/example/client/objects/autogen/type.h>
#include <yt/yt/orm/example/client/objects/autogen/tags.h>

#include <yt/yt/orm/client/native/client_impl.h>

#include <util/folder/dirut.h>
#include <util/folder/path.h>
#include <util/stream/file.h>
#include <util/string/strip.h>
#include <util/system/env.h>

namespace NYT::NOrm::NExample::NClient::NNative {

////////////////////////////////////////////////////////////////////////////////

TString FindToken()
{
    auto token = Strip(GetEnv("EXAMPLE_TOKEN"));
    if (token.empty()) {
        token = Strip(GetEnv("YT_SECURE_VAULT_EXAMPLE_TOKEN"));
    }
    if (token.empty()) {
        auto tokenPath = Strip(GetEnv("EXAMPLE_TOKEN_PATH"));
        if (tokenPath.empty()) {
            tokenPath = GetHomeDir() + "/.example/token";
        }
        TFsPath path(tokenPath);
        if (path.IsFile()) {
            token = Strip(TIFStream(path).ReadAll());
        }
    }
    return token;
}

////////////////////////////////////////////////////////////////////////////////

NYT::NOrm::NClient::NNative::IClientPtr CreateClient(
    TConnectionConfigPtr config,
    NYT::NOrm::NClient::NNative::TClientOptions clientOptions)
{
    // Connection and client use type registry, make sure it is initialized.
    auto connection = CreateConnection(std::move(config));

    if (!clientOptions.Token) {
        clientOptions.Token = FindToken();
    }

    return NYT::NOrm::NClient::NNative::CreateClient<TObjectServiceProxy>(
        std::move(connection),
        NYT::NOrm::NExample::NClient::NObjects::CreateObjectTypeRegistry(),
        NYT::NOrm::NExample::NClient::NObjects::CreateTagsRegistry(),
        NYT::NOrm::NExample::NClient::NObjects::CreateAccessControlRegistry(),
        std::move(clientOptions),
        Logger);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NExample::NClient::NNative
