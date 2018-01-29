#include "bootstrap.h"

#include "skynet_manager.h"
#include "skynet_api.h"

#include <yt/core/net/listener.h>

#include <yt/core/http/server.h>
#include <yt/core/http/client.h>

#include <yt/core/concurrency/thread_pool_poller.h>
#include <yt/core/concurrency/poller.h>
#include <yt/core/concurrency/action_queue.h>

#include <yt/core/logging/log.h>

namespace NYT {
namespace NSkynetManager {

using namespace NConcurrency;
using namespace NNet;
using namespace NHttp;
using namespace NLogging;

////////////////////////////////////////////////////////////////////////////////

static const NLogging::TLogger Logger("Bootstrap");

////////////////////////////////////////////////////////////////////////////////

TBootstrap::TBootstrap(TSkynetManagerConfigPtr config)
    : Config(std::move(config))
{
    WarnForUnrecognizedOptions(Logger, Config);

    Poller = CreateThreadPoolPoller(Config->IOPoolSize, "Poller");

    SkynetApiActionQueue = New<TActionQueue>("SkynetApi");
    SkynetApi = CreateShellSkynetApi(
        SkynetApiActionQueue->GetInvoker(),
        Config->SkynetPythonInterpreterPath,
        Config->SkynetMdsToolPath);

    HttpListener = CreateListener(TNetworkAddress::CreateIPv6Any(Config->Port), Poller);
    HttpServer = CreateServer(Config->HttpServer, HttpListener, Poller);

    HttpClient = CreateClient(Config->HttpClient, Poller);

    Manager = New<TSkynetManager>(this);
}

void TBootstrap::Run()
{
    WaitFor(HttpServer->Start())
        .ThrowOnError();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NSkynetManager
} // namespace NYT
