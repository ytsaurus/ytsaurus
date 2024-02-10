#include "cluster_connection.h"

#include <yt/yt/core/concurrency/thread_pool_poller.h>

#include <yt/yt/core/http/client.h>
#include <yt/yt/core/http/config.h>
#include <yt/yt/core/http/http.h>
#include <yt/yt/core/http/helpers.h>

#include <yt/yt/core/ytree/fluent.h>

#include <yt/yt/library/auth/auth.h>

#include <util/system/env.h>

namespace NYT {

using namespace NHttp;
using namespace NConcurrency;
using namespace NLogging;
using namespace NAuth;
using namespace NYTree;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

//! Download cluster connection via HTTP from `RemoteClusterProxyAddress`.
//! If address does not contain :, '.yt.yandex.net:80' will be appended automatically,
//! i.e. "hahn" -> "hahn.yt.yandex.net:80".
INodePtr DownloadClusterConnection(
    TString remoteClusterProxyAddress,
    TLogger logger)
{
    const auto& Logger = logger;

    auto poller = CreateThreadPoolPoller(1 /*threadCount*/, "CCHttpPoller");
    auto clientConfig = New<NHttp::TClientConfig>();
    auto client = CreateClient(clientConfig, poller);
    if (remoteClusterProxyAddress.find(':') == TString::npos) {
        remoteClusterProxyAddress.append(".yt.yandex.net:80");
    }
    if (!remoteClusterProxyAddress.StartsWith("http://")) {
        remoteClusterProxyAddress.prepend("http://");
    }

    YT_LOG_INFO("Downloading cluster connection (RemoteClusterProxyAddress: %v)", remoteClusterProxyAddress);

    auto token = LoadToken();
    if (!token) {
        THROW_ERROR_EXCEPTION("Token not found; supply it either via YT_TOKEN env variable or .yt/token file");
    }

    auto headers = New<THeaders>();
    headers->Add("Authorization", "OAuth " + *token);
    headers->Add("X-YT-Header-Format", "yson");
    headers->Add(
        "X-YT-Parameters", BuildYsonStringFluently(NYson::EYsonFormat::Text)
            .BeginMap()
                .Item("output_format")
                    .BeginAttributes()
                        .Item("format").Value("text")
                    .EndAttributes()
                    .Value("yson")
            .EndMap()
            .ToString());

    auto path = remoteClusterProxyAddress + "/api/v4/get?path=//sys/@cluster_connection";
    auto rsp = WaitFor(client->Get(path, headers))
        .ValueOrThrow();
    if (rsp->GetStatusCode() != EStatusCode::OK) {
        THROW_ERROR_EXCEPTION("Error downloading cluster connection")
            << TErrorAttribute("status_code", rsp->GetStatusCode())
            << NHttp::ParseYTError(rsp);
    }

    auto body = rsp->ReadAll();
    auto rspNode = ConvertToNode(TYsonString(ToString(body)));
    auto clusterConnectionNode = rspNode->AsMap()->FindChild("value");
    YT_VERIFY(clusterConnectionNode);
    return clusterConnectionNode;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
