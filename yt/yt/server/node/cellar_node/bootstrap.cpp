#include "bootstrap.h"

#include "master_connector.h"

#include <yt/yt/server/node/cluster_node/bootstrap.h>
#include <yt/yt/server/node/cluster_node/config.h>
#include <yt/yt/server/node/cluster_node/dynamic_config_manager.h>

#include <yt/yt/server/node/tablet_node/security_manager.h>

#include <yt/yt/server/lib/cellar_agent/bootstrap_proxy.h>
#include <yt/yt/server/lib/cellar_agent/cellar_manager.h>

namespace NYT::NCellarNode {

using namespace NApi::NNative;
using namespace NCellarAgent;
using namespace NCellarClient;
using namespace NClusterNode;
using namespace NConcurrency;
using namespace NElection;
using namespace NNodeTrackerClient;
using namespace NRpc;
using namespace NSecurityServer;

////////////////////////////////////////////////////////////////////////////////

class TCellarBootstrapProxy
    : public ICellarBootstrapProxy
{
public:
    explicit TCellarBootstrapProxy(
        IBootstrap* bootstrap)
        : Bootstrap_(bootstrap)
    { }

    virtual TCellId GetCellId() const override
    {
        return Bootstrap_->GetCellId();
    }

    virtual IClientPtr GetMasterClient() const override
    {
        return Bootstrap_->GetMasterClient();
    }

    virtual TNetworkPreferenceList GetLocalNetworks() const override
    {
        return Bootstrap_->GetLocalNetworks();
    }

    virtual IInvokerPtr GetControlInvoker() const override
    {
        return Bootstrap_->GetControlInvoker();
    }

    virtual IInvokerPtr GetTransactionTrackerInvoker() const override
    {
        return Bootstrap_->GetTransactionTrackerInvoker();
    }

    virtual IServerPtr GetRpcServer() const override
    {
        return Bootstrap_->GetRpcServer();
    }

    virtual IResourceLimitsManagerPtr GetResourceLimitsManager() const override
    {
        return Bootstrap_->GetResourceLimitsManager();
    }

private:
    IBootstrap* const Bootstrap_;
};

////////////////////////////////////////////////////////////////////////////////

class TBootstrap
    : public IBootstrap
    , public NClusterNode::TBootstrapBase
{
public:
    explicit TBootstrap(NClusterNode::IBootstrap* bootstrap)
        : TBootstrapBase(bootstrap)
        , ClusterNodeBootstrap_(bootstrap)
    { }

    virtual void Initialize() override
    {
        GetDynamicConfigManager()
            ->SubscribeConfigChanged(BIND(&TBootstrap::OnDynamicConfigChanged, this));

        TransactionTrackerQueue_ = New<TActionQueue>("TxTracker");

        // TODO(gritukan): Move TSecurityManager from Tablet Node.
        ResourceLimitsManager_ = New<NTabletNode::TSecurityManager>(GetConfig()->TabletNode->SecurityManager, this);

        // COMPAT(savrus)
        auto getCellarManagerConfig = [&] {
            auto& config = GetConfig()->CellarNode->CellarManager;

            if (!ClusterNodeBootstrap_->IsTabletNode()) {
                return config;
            }

            for (const auto& [type, _] : config->Cellars) {
                if (type == ECellarType::Tablet) {
                    return config;
                }
            }

            auto cellarConfig = New<TCellarConfig>();
            cellarConfig->Size = GetConfig()->TabletNode->ResourceLimits->Slots;
            cellarConfig->Occupant = New<TCellarOccupantConfig>();
            cellarConfig->Occupant->Snapshots = GetConfig()->TabletNode->Snapshots;
            cellarConfig->Occupant->Changelogs = GetConfig()->TabletNode->Changelogs;
            cellarConfig->Occupant->HydraManager = GetConfig()->TabletNode->HydraManager;
            cellarConfig->Occupant->ElectionManager = GetConfig()->TabletNode->ElectionManager;
            cellarConfig->Occupant->HiveManager = GetConfig()->TabletNode->HiveManager;
            cellarConfig->Occupant->TransactionSupervisor = GetConfig()->TabletNode->TransactionSupervisor;
            cellarConfig->Occupant->ResponseKeeper = GetConfig()->TabletNode->HydraManager->ResponseKeeper;

            auto cellarManagerConfig = CloneYsonSerializable(config);
            cellarManagerConfig->Cellars.insert({ECellarType::Tablet, std::move(cellarConfig)});
            return cellarManagerConfig;
        };

        auto cellarBootstrapProxy = New<TCellarBootstrapProxy>(this);
        CellarManager_ = CreateCellarManager(getCellarManagerConfig(), std::move(cellarBootstrapProxy));

        MasterConnector_ = CreateMasterConnector(this);

        CellarManager_->Initialize();
    }

    virtual void Run() override
    {
        MasterConnector_->Initialize();
    }

    virtual const IInvokerPtr& GetTransactionTrackerInvoker() const override
    {
        return TransactionTrackerQueue_->GetInvoker();
    }

    virtual const IResourceLimitsManagerPtr& GetResourceLimitsManager() const override
    {
        return ResourceLimitsManager_;
    }

    virtual const ICellarManagerPtr& GetCellarManager() const override
    {
        return CellarManager_;
    }

    virtual const IMasterConnectorPtr& GetMasterConnector() const override
    {
        return MasterConnector_;
    }

private:
    NClusterNode::IBootstrap* const ClusterNodeBootstrap_;

    TActionQueuePtr TransactionTrackerQueue_;

    IResourceLimitsManagerPtr ResourceLimitsManager_;

    ICellarManagerPtr CellarManager_;

    IMasterConnectorPtr MasterConnector_;

    void OnDynamicConfigChanged(
        const TClusterNodeDynamicConfigPtr& /*oldConfig*/,
        const TClusterNodeDynamicConfigPtr& newConfig)
    {
        // COMPAT(savrus)
        auto getCellarManagerConfig = [&] {
            auto& config = newConfig->CellarNode->CellarManager;
            if (!newConfig->TabletNode->Slots) {
                return config;
            } else {
                for (const auto& [type, _] : config->Cellars) {
                    if (type == ECellarType::Tablet) {
                        return config;
                    }
                }

                auto cellarManagerConfig = CloneYsonSerializable(config);
                auto cellarConfig = New<TCellarDynamicConfig>();
                cellarConfig->Size = newConfig->TabletNode->Slots;
                cellarManagerConfig->Cellars.insert({ECellarType::Tablet, std::move(cellarConfig)});
                return cellarManagerConfig;
            }
        };

        CellarManager_->Reconfigure(getCellarManagerConfig());
    }
};

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<IBootstrap> CreateBootstrap(NClusterNode::IBootstrap* bootstrap)
{
    return std::make_unique<TBootstrap>(bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellarNode
