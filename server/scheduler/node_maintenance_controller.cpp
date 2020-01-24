#include "node_maintenance_controller.h"

#include "private.h"

#include <yp/server/master/bootstrap.h>

#include <yp/server/objects/node.h>
#include <yp/server/objects/transaction.h>
#include <yp/server/objects/transaction_manager.h>

#include <yp/server/lib/cluster/cluster.h>
#include <yp/server/lib/cluster/node.h>
#include <yp/server/lib/cluster/pod.h>

namespace NYP::NServer::NScheduler {

using namespace NServer::NMaster;
using namespace NServer::NObjects;

using namespace NYT::NConcurrency;

////////////////////////////////////////////////////////////////////////////////

class TNodeMaintenanceController::TImpl
    : public TRefCounted
{
public:
    explicit TImpl(TBootstrap* bootstrap)
        : Bootstrap_(bootstrap)
    { }

    void Acknowledge(const NCluster::TClusterPtr& cluster)
    {
        ExecuteTransition(
            cluster,
            AcknowledgeTransition,
            "Acknowledge");
    }

private:
    TBootstrap* const Bootstrap_;


    static bool ArePodMaintenancesAcknowledged(const NCluster::TNode* node)
    {
        for (auto* pod : node->Pods()) {
            if (pod->Maintenance().state() != NClient::NApi::NProto::PMS_ACKNOWLEDGED) {
                return false;
            }
        }
        return true;
    }

    static void AcknowledgeTransition(const TTransactionPtr& transaction, const NCluster::TNode* node)
    {
        const auto& maintenance = node->Maintenance();

        if (maintenance.state() != NClient::NApi::NProto::NMS_REQUESTED) {
            return;
        }

        if (!ArePodMaintenancesAcknowledged(node)) {
            return;
        }

        auto* transactionNode = transaction->GetNode(node->GetId());

        transactionNode->Status().Etc().ScheduleLoad();

        if (!transactionNode->DoesExist()) {
            YT_LOG_DEBUG("Node maintenance acknowledgement is skipped since transaction node does not exist (NodeId: %v)",
                node->GetId());
            return;
        }
        const auto& transactionMaintenance = transactionNode->Status().Etc().Load().maintenance();
        if (transactionMaintenance.state() != NClient::NApi::NProto::NMS_REQUESTED) {
            YT_LOG_DEBUG("Node maintenance acknowledgement is skipped since maintenance is not requested for transaction node (NodeId: %v)",
                node->GetId());
            return;
        }
        if (transactionMaintenance.info().uuid() != maintenance.info().uuid()) {
            YT_LOG_DEBUG("Node maintenance acknowledgement is skipped since maintenance request has different uuid in transaction (NodeId: %v, Uuid: %v, TransactionUuid: %v)",
                node->GetId(),
                maintenance.info().uuid(),
                transactionMaintenance.info().uuid());
            return;
        }

        YT_LOG_DEBUG("Node maintenance acknowledged (NodeId: %v)",
            node->GetId());

        transactionNode->UpdateMaintenanceStatus(
            ENodeMaintenanceState::Acknowledged,
            "Maintenance acknowledged by scheduler since all pod maintenancens are acknowledged on this node",
            TGenericPreserveUpdate());
    }

    template <class TTransition>
    void ExecuteTransition(
        const NCluster::TClusterPtr& cluster,
        TTransition&& transition,
        TStringBuf transitionName)
    {
        try {
            const auto& transactionManager = Bootstrap_->GetTransactionManager();
            auto transaction = WaitFor(transactionManager->StartReadWriteTransaction())
                .ValueOrThrow();

            YT_LOG_DEBUG("Started node maintenance transition transaction (TransactionId: %v, StartTimestamp: %llx, TransitionName: %v)",
                transaction->GetId(),
                transaction->GetStartTimestamp(),
                transitionName);

            auto nodes = cluster->GetNodes();
            for (auto* node : nodes) {
                transition(transaction, node);
            }

            WaitFor(transaction->Commit())
                .ThrowOnError();
        } catch (const std::exception& ex) {
            YT_LOG_WARNING(ex, "Error executing node maintenance transition (TransitionName: %v)",
                transitionName);
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

TNodeMaintenanceController::TNodeMaintenanceController(TBootstrap* bootstrap)
    : Impl_(New<TNodeMaintenanceController::TImpl>(bootstrap))
{ }

void TNodeMaintenanceController::Acknowledge(const NCluster::TClusterPtr& cluster)
{
    Impl_->Acknowledge(cluster);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NServer::NScheduler
