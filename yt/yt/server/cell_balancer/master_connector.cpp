#include "master_connector.h"

#include "bootstrap.h"
#include "config.h"

#include <yt/yt/ytlib/object_client/object_service_proxy.h>

#include <yt/yt/ytlib/api/native/client.h>

#include <yt/yt/ytlib/cypress_client/cypress_ypath_proxy.h>

#include <yt/yt/client/node_tracker_client/node_directory.h>

#include <yt/yt/core/concurrency/delayed_executor.h>
#include <yt/yt/core/concurrency/thread_affinity.h>

namespace NYT::NCellBalancer {

using namespace NApi;
using namespace NConcurrency;
using namespace NCypressClient;
using namespace NNodeTrackerClient;
using namespace NObjectClient;
using namespace NYPath;
using namespace NYTree;

using NYT::FromProto;
using NYT::ToProto;

////////////////////////////////////////////////////////////////////////////////

static inline const NLogging::TLogger Logger("MasterConnector");

////////////////////////////////////////////////////////////////////////////////

class TMasterConnector
    : public IMasterConnector
{
public:
    TMasterConnector(
        IBootstrap* bootstrap,
        TCellBalancerMasterConnectorConfigPtr config)
        : Bootstrap_(bootstrap)
        , Config_(std::move(config))
    { }

    void Start()
    {
        VERIFY_THREAD_AFFINITY_ANY();

        YT_LOG_DEBUG("Stating master connector");

        StartConnecting(/*immediate*/ true);
    }

private:
    IBootstrap* const Bootstrap_;
    const TCellBalancerMasterConnectorConfigPtr Config_;

    void StartConnecting(bool immediate)
    {
        VERIFY_THREAD_AFFINITY_ANY();

        TDelayedExecutor::Submit(
            BIND(&TMasterConnector::DoStartConnecting, MakeStrong(this))
                .Via(Bootstrap_->GetControlInvoker()),
            immediate ? TDuration::Zero() : Config_->ConnectRetryBackoffTime);
    }

    void DoStartConnecting()
    {
        VERIFY_INVOKER_AFFINITY(Bootstrap_->GetControlInvoker());

        YT_LOG_INFO("Connecting to master");

        BIND(&TMasterConnector::RegisterInstance, MakeStrong(this))
            .AsyncVia(Bootstrap_->GetControlInvoker())
            .Run()
            .Subscribe(BIND(&TMasterConnector::OnConnected, MakeStrong(this))
                .Via(Bootstrap_->GetControlInvoker()));
    }

    void OnConnected(const TError& error) noexcept
    {
        VERIFY_INVOKER_AFFINITY(Bootstrap_->GetControlInvoker());

        if (!error.IsOK()) {
            YT_LOG_WARNING(error, "Error connecting to master");
            StartConnecting(/*immediate*/ false);
            return;
        }

        YT_LOG_DEBUG("Cell balancer is connected to master");
    }

    void RegisterInstance()
    {
        auto proxy = CreateObjectServiceWriteProxy(Bootstrap_->GetClient());
        auto batchReq = proxy.ExecuteBatch();
        auto path = "//sys/cell_balancers/instances/" + ToYPathLiteral(GetDefaultAddress(Bootstrap_->GetLocalAddresses()));
        {
            auto req = TCypressYPathProxy::Create(path);
            req->set_ignore_existing(true);
            req->set_type(static_cast<int>(EObjectType::MapNode));
            GenerateMutationId(req);
            batchReq->AddRequest(req);
        }
        {
            auto req = TCypressYPathProxy::Create(path + "/orchid");
            req->set_ignore_existing(true);
            req->set_type(static_cast<int>(EObjectType::Orchid));
            auto attributes = CreateEphemeralAttributes();
            attributes->Set("remote_addresses", Bootstrap_->GetLocalAddresses());
            ToProto(req->mutable_node_attributes(), *attributes);
            GenerateMutationId(req);
            batchReq->AddRequest(req);
        }

        auto batchRspOrError = WaitFor(batchReq->Invoke());
        THROW_ERROR_EXCEPTION_IF_FAILED(GetCumulativeError(batchRspOrError));
    }
};

////////////////////////////////////////////////////////////////////////////////

IMasterConnectorPtr CreateMasterConnector(
    IBootstrap* bootstrap,
    TCellBalancerMasterConnectorConfigPtr config)
{
    return New<TMasterConnector>(bootstrap, std::move(config));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellBalancer
