#include "yt_connector.h"

#include "config.h"
#include "dyntable_election_manager.h"
#include "flow_executor.h"
#include "private.h"

#include <yt/yt/flow/library/cpp/common/control_table.h>
#include <yt/yt/flow/library/cpp/common/dyntable_lease.h>

#include <yt/yt/flow/library/cpp/misc/node_info.h>

#include <yt/yt/flow/library/cpp/native_client/public.h>

#include <yt/yt/client/api/client.h>
#include <yt/yt/client/api/transaction.h>
#include <yt/yt/client/table_client/name_table.h>
#include <yt/yt/client/table_client/unversioned_row.h>

#include <yt/yt/core/concurrency/fair_share_action_queue.h>
#include <yt/yt/core/concurrency/scheduler.h>
#include <yt/yt/core/concurrency/thread_affinity.h>

#include <yt/yt/core/net/local_address.h>

#include <yt/yt/core/ypath/helpers.h>

#include <yt/yt/core/ytree/virtual.h>

#include <yt/yt/library/cypress_election/config.h>
#include <yt/yt/library/cypress_election/election_manager.h>

#include <yt/yt/library/lock_election/election_manager.h>

namespace NYT::NFlow::NController {

////////////////////////////////////////////////////////////////////////////////

using namespace NApi;
using namespace NConcurrency;
using namespace NCypressElection;
using namespace NLockElection;
using namespace NPrerequisiteClient;
using namespace NTableClient;
using namespace NTransactionClient;
using namespace NYPath;
using namespace NYTree;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

constinit const auto Logger = ControllerLogger;

////////////////////////////////////////////////////////////////////////////////

class TYTConnector
    : public IYTConnector
{
public:
    TYTConnector(
        TControllerConfigPtr config,
        TNodeInfoPtr nodeInfo,
        ICommonYTConnectorPtr commonYTConnector,
        TControlActionQueuePtr controlQueue)
        : Config_(std::move(config))
        , NodeInfo_(std::move(nodeInfo))
        , CommonYTConnector_(std::move(commonYTConnector))
        , ControlQueue_(std::move(controlQueue))
        , SerializedInvoker_(ControlQueue_->GetInvoker(EControlQueue::YTConnector))
        , DyntableLeases_(FlowControlTablePath(), LeasesTablePath())
    {
        YT_VERIFY(SerializedInvoker_->IsSerialized());
    }

    TRichYPath GetPipelinePath() override
    {
        return CommonYTConnector_->GetPipelinePath();
    }

    IClientPtr GetClient() override
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

        return CommonYTConnector_->GetClient();
    }

    NClient::NCache::IClientsCachePtr GetClientsCache() override
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

        return CommonYTConnector_->GetClientsCache();
    }

    TFuture<TPipelineAttributes> GetPipelineAttributes() override
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

        return CommonYTConnector_->GetPipelineAttributes();
    }

    TFuture<TFlowTablesBundleInfo> GetFlowTablesBundle() override
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

        return CommonYTConnector_->GetFlowTablesBundle();
    }

    void Start() override
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

        BIND(&TYTConnector::DoConnect, MakeStrong(this))
            .Via(SerializedInvoker_)
            .Run();
    }

    EYTConnectorState GetState() const override
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

        return State_.load();
    }

    bool IsLeader() const override
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

        return State_ == EYTConnectorState::Leader;
    }

    TInstant GetLeadershipPublishTime() const override
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

        return LeadershipPublishTime_.load();
    }

    void ValidateLeader() const override
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

        if (!IsLeader()) {
            THROW_ERROR_EXCEPTION("Connector is not leading");
        }
    }

    void Disconnect() override
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

        WaitFor(
            BIND(&TYTConnector::DoDisconnect, MakeStrong(this))
                .AsyncVia(SerializedInvoker_)
                .Run())
            .ThrowOnError();
    }

    //! Fences the transaction with the current leadership: with the Cypress backend the
    //! leadership prerequisite id is added to prerequisites; with the Dyntable backend a tablet
    //! transaction is fenced by validating and touching the leader row inside it (master
    //! transactions cannot be fenced this way and are started as is — they must stay advisory).
    TFuture<ITransactionPtr> StartTransaction(
        ETransactionType type,
        TTransactionStartOptions options = {}) override
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

        if (Config_->ElectionManager.GetType() == EElectionBackend::Cypress) {
            options.PrerequisiteTransactionIds.push_back(GetPrerequisiteId());
            return GetClient()->StartTransaction(type, options);
        }

        ValidateLeader();
        auto future = GetClient()->StartTransaction(type, options);
        if (type != ETransactionType::Tablet) {
            return future;
        }
        return future.Apply(BIND([this, this_ = MakeStrong(this), type, options] (const ITransactionPtr& transaction) {
            auto workTransaction = transaction;
            try {
                auto ttl = Config_->ElectionManager.GetConcrete<TDyntableElectionBackendConfig>()->LeaderLeaseTtl;
                // The touch refreshes the lease only when the work transaction commits, and that
                // may be far away. If less than half of the ttl remains, commit the touch alone
                // right now as an urgent prolongation and fence the work with a fresh transaction.
                // One extra pass suffices: the second transaction observes the just-committed
                // refresh.
                for (int attempt = 0;; ++attempt) {
                    auto remaining = DyntableLeases_.ValidateAndTouchLeader(
                        workTransaction,
                        NodeInfo_->IncarnationId,
                        NodeInfo_->RpcAddress,
                        ttl);
                    if (remaining >= ttl / 2 || attempt > 0) {
                        return workTransaction;
                    }
                    NYT::NConcurrency::WaitFor(workTransaction->Commit())
                        .ThrowOnError();
                    YT_TLOG_INFO("Committed an urgent leader lease prolongation")
                        .With("Remaining", remaining);
                    workTransaction = NYT::NConcurrency::WaitFor(GetClient()->StartTransaction(type, options))
                        .ValueOrThrow();
                }
            } catch (const std::exception&) {
                // The transaction is already started, and losing the leadership check here is a
                // routine outcome for a demoted controller that keeps trying. Without this it
                // would leak one tablet transaction per attempt, each lingering until its own
                // timeout.
                YT_UNUSED_FUTURE(workTransaction->Abort());
                throw;
            }
        }).AsyncVia(SerializedInvoker_));
    }

    TPrerequisiteId GetPrerequisiteId() const override
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

        return WaitFor(BIND(&TYTConnector::GetPrerequisiteIdImpl, MakeStrong(this))
                .AsyncVia(SerializedInvoker_)
                .Run())
            .ValueOrThrow();
    }

    void OnLeaderRecoveryFinished(ui64 leadershipEpoch) override
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

        // The caller is the scheduler, on its own invoker, while #DoDisconnect resets the manager
        // on the connector's serialized one: the pointer must be snapshotted under the lock, not
        // dereferenced in place. Whether the callback still applies is decided by the epoch it
        // carries, not by the connector state, so a demotion and a re-acquisition in between
        // cannot make a stale callback disarm the renewal of the current leadership.
        auto manager = GetDyntableElectionManager();
        if (manager) {
            manager->SetRecoveryRenewalEnabled(false, leadershipEpoch);
        }
    }

    ui64 GetLeadershipEpoch() const override
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

        auto manager = GetDyntableElectionManager();
        return manager ? manager->GetLeadershipEpoch() : 0;
    }

    DEFINE_SIGNAL_OVERRIDE(void(), LeadingStarted);
    DEFINE_SIGNAL_OVERRIDE(void(), LeadingEnded);

private:
    const TControllerConfigPtr Config_;
    const TNodeInfoPtr NodeInfo_;
    const ICommonYTConnectorPtr CommonYTConnector_;
    const TControlActionQueuePtr ControlQueue_;
    const IInvokerPtr SerializedInvoker_;
    const TDyntableLeases DyntableLeases_;
    TFuture<void> PublisherFuture_;

    std::atomic<EYTConnectorState> State_ = EYTConnectorState::Disconnected;
    //! Set once TryPublishLeadership succeeds, cleared when leading ends. Read by the controller to
    //! tell "we lead" from "workers can actually find us".
    std::atomic<TInstant> LeadershipPublishTime_ = TInstant::Zero();

    ILockElectionManagerPtr ElectionManager_;
    //! Set alongside #ElectionManager_ when the Dyntable backend is selected; null otherwise.
    //! Written on the serialized invoker, read from any thread by #GetDyntableElectionManager.
    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, DyntableElectionManagerLock_);
    IDyntableElectionManagerPtr DyntableElectionManager_;

    IDyntableElectionManagerPtr GetDyntableElectionManager() const
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

        auto guard = Guard(DyntableElectionManagerLock_);
        return DyntableElectionManager_;
    }

private:
    TPrerequisiteId GetPrerequisiteIdImpl() const
    {
        YT_ASSERT_SERIALIZED_INVOKER_AFFINITY(SerializedInvoker_);

        ValidateLeader();

        return ElectionManager_->GetPrerequisiteId();
    }

    NYPath::TYPath FlowControlTablePath() const
    {
        return YPathJoin(CommonYTConnector_->GetPipelinePath().GetPath(), NFlow::FlowControlTableName);
    }

    NYPath::TYPath LeasesTablePath() const
    {
        return YPathJoin(CommonYTConnector_->GetPipelinePath().GetPath(), NFlow::LeasesTableName);
    }

    ILockElectionManagerPtr CreateElectionManager()
    {
        YT_ASSERT_SERIALIZED_INVOKER_AFFINITY(SerializedInvoker_);

        const auto& electionConfig = Config_->ElectionManager;
        switch (electionConfig.GetType()) {
            case EElectionBackend::Cypress: {
                auto backendConfig = electionConfig.GetConcrete<TCypressElectionBackendConfig>();

                auto config = New<TCypressElectionManagerConfig>();
                config->LockPath = YPathJoin(GetPipelinePath().GetPath(), "leader_controller_lock");
                config->TransactionTimeout = backendConfig->TransactionTimeout;
                config->TransactionPingPeriod = backendConfig->TransactionPingPeriod;
                config->LockAcquisitionPeriod = backendConfig->LockAcquisitionPeriod;
                config->LeaderCacheUpdatePeriod = backendConfig->LeaderCacheUpdatePeriod;
                config->MasterTransactionExpirationMode = NTransactionClient::EMasterTransactionExpirationMode::Pessimistic;

                auto options = New<TCypressElectionManagerOptions>();
                auto attrs = CreateEphemeralAttributes();
                attrs->Set("host", NodeInfo_->GetIdentifyingString());
                options->GroupName = "FlowController";
                options->MemberName = Format("%v(%v;%v)", NodeInfo_->Name, NodeInfo_->RpcAddress, NodeInfo_->IncarnationId);
                options->TransactionAttributes = std::move(attrs);
                return CreateCypressElectionManager(
                    GetClient(),
                    SerializedInvoker_,
                    std::move(config),
                    std::move(options));
            }
            case EElectionBackend::Dyntable: {
                auto backendConfig = electionConfig.GetConcrete<TDyntableElectionBackendConfig>();

                auto manager = CreateDyntableElectionManager(
                    GetClient(),
                    SerializedInvoker_,
                    TDyntableElectionManagerOptions{
                        .FlowControlTablePath = FlowControlTablePath(),
                        .LeasesTablePath = LeasesTablePath(),
                        .IncarnationId = NodeInfo_->IncarnationId,
                        .Address = NodeInfo_->RpcAddress,
                        .LeaseTtl = backendConfig->LeaderLeaseTtl,
                        .CapturePeriod = backendConfig->LockAcquisitionPeriod,
                        .DetachTimeout = backendConfig->DetachTimeout,
                    });
                {
                    auto guard = Guard(DyntableElectionManagerLock_);
                    DyntableElectionManager_ = manager;
                }
                return manager;
            }
        }
        YT_ABORT();
    }

    void DoConnect()
    {
        YT_ASSERT_SERIALIZED_INVOKER_AFFINITY(SerializedInvoker_);

        if (State_ != EYTConnectorState::Disconnected) {
            return;
        }

        DoCleanUp();

        State_.store(EYTConnectorState::Connecting);

        ElectionManager_ = CreateElectionManager();
        ElectionManager_->SubscribeLeadingStarted(BIND(&TYTConnector::DoLeadingStarted, MakeWeak(this)));
        ElectionManager_->SubscribeLeadingEnded(BIND(&TYTConnector::DoLeadingEnded, MakeWeak(this)));
        // Become a follower before starting the election manager: an instantly won election may
        // fire LeadingStarted outside this serialized invoker, and DoLeadingStarted requires the
        // Follower state.
        State_.store(EYTConnectorState::Follower);
        ElectionManager_->Start();
        YT_TLOG_INFO("YTConnector following started");
    }

    void DoDisconnect()
    {
        YT_ASSERT_SERIALIZED_INVOKER_AFFINITY(SerializedInvoker_);

        if (State_ == EYTConnectorState::Disconnected) {
            return;
        }

        NYT::NConcurrency::WaitFor(ElectionManager_->Stop()).ThrowOnError();
        YT_VERIFY(State_ == EYTConnectorState::Follower);
        ElectionManager_.Reset();
        {
            auto guard = Guard(DyntableElectionManagerLock_);
            DyntableElectionManager_.Reset();
        }
        State_.store(EYTConnectorState::Disconnected);
        YT_TLOG_INFO("YTConnector following stopped");
    }

    // Checks that controller node incarnation id available from YT is the same as provided.
    TFuture<void> CheckControllerLeaderNodeIncarnationIdExternally(TIncarnationId incarnationId)
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

        TGetControllerOrchidArg argument;
        argument.Path = "/node_info/incarnation_id";

        // Controller must have write access to pipeline path. So it can perform flow execute requests.
        return GetClient()
            ->FlowExecute(GetPipelinePath().GetPath(), "get-controller-orchid", ConvertToYsonString(argument))
            .Apply(BIND([incarnationId] (const TFlowExecuteResult& result) {
                auto parsedGetOrchidResult = ConvertTo<TGetControllerOrchidResult>(result.Result);
                auto actualIncarnationId = ConvertTo<TIncarnationId>(parsedGetOrchidResult.Value);
                if (actualIncarnationId != incarnationId) {
                    THROW_ERROR_EXCEPTION("Controller leader node incarnation ids mismatch")
                        .With("actual_incarnation_id", actualIncarnationId)
                        .With("expected_incarnation_id", incarnationId);
                }
            }));
    }

    bool TryPublishLeadership()
    {
        YT_ASSERT_SERIALIZED_INVOKER_AFFINITY(SerializedInvoker_);

        // The fenced flow_control publication goes first on purpose. The master attribute below is
        // written in a master transaction, which no backend can fence, so a controller that has
        // already lost its leadership would otherwise still overwrite the routing attribute with
        // its own address and steer FlowExecute requests to itself. Losing the lease makes this
        // tablet commit fail, and the master write is never reached.
        try {
            TTransactionStartOptions options;
            options.Timeout = TDuration::Seconds(1);
            auto transaction = WaitFor(StartTransaction(ETransactionType::Tablet, options)).ValueOrThrow();

            // Publish the full node info (address, fqdn-ish name, incarnation, versions, ...) so the
            // value can be extended without a schema change; readers pick out the fields they need.
            TControlTable::Write(
                transaction,
                YPathJoin(GetPipelinePath().GetPath(), FlowControlTableName),
                LeaderControllerKey,
                ConvertToYsonString(NodeInfo_));
            WaitFor(transaction->Commit()).ThrowOnError();
            YT_TLOG_INFO("Published leader controller address to flow_control table")
                .With("Address", NodeInfo_->RpcAddress);
            // Only now can a worker discover this leader, so this is where the warm-up window
            // during which the controller must not touch jobs starts.
            LeadershipPublishTime_.store(TInstant::Now());
        } catch (const std::exception& ex) {
            YT_TLOG_EVENT(PublicControllerLogger, NLogging::ELogLevel::Warning, "Failed to publish leader_controller to flow_control table")
                .With(ex);
            return false;
        }

        try {
            TTransactionStartOptions options;
            options.Timeout = TDuration::Seconds(1);
            auto transaction = WaitFor(StartTransaction(ETransactionType::Master, options)).ValueOrThrow();
            TSetNodeOptions setOptions;
            setOptions.Recursive = true;
            setOptions.Timeout = TDuration::Seconds(1);
            WaitFor(transaction->SetNode(
                Format("%v/@%v", GetPipelinePath().GetPath(), LeaderControllerAddressAttribute),
                ConvertToYsonString(NodeInfo_->RpcAddress),
                setOptions))
                .ThrowOnError();
            WaitFor(transaction->Commit()).ThrowOnError();
            YT_TLOG_INFO("Published leader controller address")
                .With("Address", NodeInfo_->RpcAddress);
        } catch (const std::exception& ex) {
            YT_TLOG_EVENT(PublicControllerLogger, NLogging::ELogLevel::Warning, "Failed to publish leader_controller_address")
                .With(ex);
            return false;
        }

        try {
            WaitFor(CheckControllerLeaderNodeIncarnationIdExternally(NodeInfo_->IncarnationId)).ThrowOnError();
            YT_TLOG_INFO("Confirmed published leader controller address")
                .With("Address", NodeInfo_->RpcAddress);
        } catch (const std::exception& ex) {
            YT_TLOG_EVENT(PublicControllerLogger, NLogging::ELogLevel::Warning, "Failed to confirm leader_controller_address")
                .With(ex);
            return false;
        }

        return true;
    }

    static void PublishLeadership(TWeakPtr<TYTConnector> weakConnector, TPrerequisiteId prerequisiteId)
    {
        auto startTime = TInstant::Now();
        while (true) {
            auto connector = weakConnector.Lock();
            if (!connector || !connector->ElectionManager_ || !connector->ElectionManager_->IsLeader() ||
                connector->ElectionManager_->GetPrerequisiteId() != prerequisiteId) {
                return;
            }

            if (connector->TryPublishLeadership()) {
                return;
            }

            if (TInstant::Now() - startTime > connector->Config_->PublishTimeout) {
                YT_TLOG_EVENT(PublicControllerLogger, NLogging::ELogLevel::Error, "Giving up leadership; failed to publish leader controller address")
                    .With("PublishTimeout", connector->Config_->PublishTimeout);
                WaitUntilSet(connector->ElectionManager_->StopLeading());
            }

            auto delay = connector->Config_->PublishRetryPeriod;
            connector.Reset();
            TDelayedExecutor::WaitForDuration(delay);
        }
    }

    void DoLeadingStarted()
    {
        YT_ASSERT_SERIALIZED_INVOKER_AFFINITY(SerializedInvoker_);

        YT_VERIFY(State_ == EYTConnectorState::Follower);

        TForbidContextSwitchGuard contextSwitchGuard;

        State_.store(EYTConnectorState::Leader);
        LeadershipPublishTime_.store(TInstant::Zero());

        PublisherFuture_ = BIND(&TYTConnector::PublishLeadership, MakeWeak(this), ElectionManager_->GetPrerequisiteId())
            .AsyncVia(SerializedInvoker_)
            .Run();

        LeadingStarted_.Fire();
        YT_TLOG_INFO("YTConnector leading started");
    }

    void DoLeadingEnded()
    {
        YT_ASSERT_SERIALIZED_INVOKER_AFFINITY(SerializedInvoker_);

        YT_VERIFY(State_ == EYTConnectorState::Leader);

        TForbidContextSwitchGuard contextSwitchGuard;
        DoCleanUp();

        State_.store(EYTConnectorState::Follower);
        LeadershipPublishTime_.store(TInstant::Zero());

        PublisherFuture_.Cancel(TError("Leading ended"));

        LeadingEnded_.Fire();
        YT_TLOG_INFO("YTConnector leading ended");
    }

    void DoCleanUp()
    {
        YT_ASSERT_SERIALIZED_INVOKER_AFFINITY(SerializedInvoker_);
    }
};

////////////////////////////////////////////////////////////////////////////////

IYTConnectorPtr CreateYTConnector(
    TControllerConfigPtr config,
    TNodeInfoPtr nodeInfo,
    ICommonYTConnectorPtr commonYTConnector,
    TControlActionQueuePtr controlQueue)
{
    return New<TYTConnector>(std::move(config), std::move(nodeInfo), std::move(commonYTConnector), std::move(controlQueue));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NController
