#include "bootstrap.h"

#include "config.h"
#include "heavy_scheduler.h"
#include "private.h"
#include "yt_connector.h"

#include <yt/ytlib/monitoring/http_integration.h>
#include <yt/ytlib/monitoring/monitoring_manager.h>
#include <yt/ytlib/program/build_attributes.h>

#include <yp/client/api/native/client.h>

#include <yt/core/concurrency/action_queue.h>
#include <yt/core/concurrency/thread_pool_poller.h>
#include <yt/core/http/server.h>
#include <yt/core/net/local_address.h>
#include <yt/core/ytree/virtual.h>
#include <yt/core/ytree/ypath_client.h>

namespace NYP::NServer::NHeavyScheduler {

////////////////////////////////////////////////////////////////////////////////

using namespace NYT;
using namespace NYT::NConcurrency;

////////////////////////////////////////////////////////////////////////////////

class TBootstrap::TImpl
{
public:
    TImpl(TBootstrap* bootstrap, THeavySchedulerProgramConfigPtr config)
        : Bootstrap_(bootstrap)
        , Config_(std::move(config))
    { }

    const IInvokerPtr& GetControlInvoker()
    {
        return ControlQueue_->GetInvoker();
    }

    const TString& GetFqdn()
    {
        return Fqdn_;
    }

    const NYP::NClient::NApi::NNative::IClientPtr& GetClient()
    {
        return Client_;
    }

    const TYTConnectorPtr& GetYTConnector()
    {
        return YTConnector_;
    }

    const THeavySchedulerPtr& GetHeavyScheduler()
    {
        return HeavyScheduler_;
    }

    void Run()
    {
        BIND(&TImpl::DoRun, this)
            .AsyncVia(GetControlInvoker())
            .Run()
            .Get()
            .ThrowOnError();
        Sleep(TDuration::Max());
    }

private:
    TBootstrap* const Bootstrap_;
    const THeavySchedulerProgramConfigPtr Config_;

    const TActionQueuePtr ControlQueue_ = New<TActionQueue>("Control");

    TString Fqdn_;

    NConcurrency::IPollerPtr HttpPoller_;

    NYP::NClient::NApi::NNative::IClientPtr Client_;

    TYTConnectorPtr YTConnector_;
    THeavySchedulerPtr HeavyScheduler_;
    NMonitoring::TMonitoringManagerPtr MonitoringManager_;

    NHttp::IServerPtr HttpMonitoringServer_;

    void DoRun()
    {
        Fqdn_ = NNet::GetLocalHostName();

        YT_LOG_INFO("Initializing Heavy Scheduler (Fqdn: %v)",
            Fqdn_);

        HttpPoller_ = NConcurrency::CreateThreadPoolPoller(1, "Http");

        Client_ = NYP::NClient::NApi::NNative::CreateClient(Config_->Client);

        YTConnector_ = New<TYTConnector>(Bootstrap_, Config_->YTConnector);
        HeavyScheduler_ = New<THeavyScheduler>(Bootstrap_, Config_->HeavyScheduler);

        if (Config_->MonitoringServer) {
            HttpMonitoringServer_ = NHttp::CreateServer(
                Config_->MonitoringServer,
                HttpPoller_);
        }

        NYTree::IMapNodePtr orchidRoot;
        NMonitoring::Initialize(HttpMonitoringServer_, &MonitoringManager_, &orchidRoot);

        SetNodeByYPath(
            orchidRoot,
            "/yt_connector",
            CreateVirtualNode(YTConnector_->CreateOrchidService()->Via(GetControlInvoker())));

        SetBuildAttributes(orchidRoot, "yp_heavy_scheduler");

        YTConnector_->Initialize();
        HeavyScheduler_->Initialize();

        if (HttpMonitoringServer_) {
            HttpMonitoringServer_->Start();
        }

        YT_LOG_INFO("Heavy Scheduler is initialized");
    }
};

////////////////////////////////////////////////////////////////////////////////

TBootstrap::TBootstrap(THeavySchedulerProgramConfigPtr config)
    : Impl_(std::make_unique<TImpl>(this, std::move(config)))
{ }

const IInvokerPtr& TBootstrap::GetControlInvoker()
{
    return Impl_->GetControlInvoker();
}

const TString& TBootstrap::GetFqdn()
{
    return Impl_->GetFqdn();
}

const NYP::NClient::NApi::NNative::IClientPtr& TBootstrap::GetClient()
{
    return Impl_->GetClient();
}

const TYTConnectorPtr& TBootstrap::GetYTConnector()
{
    return Impl_->GetYTConnector();
}

const THeavySchedulerPtr& TBootstrap::GetHeavyScheduler()
{
    return Impl_->GetHeavyScheduler();
}

void TBootstrap::Run()
{
    Impl_->Run();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NServer::NHeavyScheduler
