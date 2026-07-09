#include "yt_connector.h"

#include "config.h"
#include "flow_executor.h"
#include "private.h"

#include <yt/yt/flow/library/cpp/common/control_table.h>

#include <yt/yt/flow/library/cpp/misc/node_info.h>

#include <yt/yt/flow/lib/native_client/public.h>

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

    //! Adds the current leadership prerequisite id into prerequisites.
    TFuture<ITransactionPtr> StartTransaction(
        ETransactionType type,
        TTransactionStartOptions options = {}) override
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

        options.PrerequisiteTransactionIds.push_back(GetPrerequisiteId());
        return GetClient()->StartTransaction(type, options);
    }

    TPrerequisiteId GetPrerequisiteId() const override
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

        return WaitFor(BIND(&TYTConnector::GetPrerequisiteIdImpl, MakeStrong(this))
                .AsyncVia(SerializedInvoker_)
                .Run())
            .ValueOrThrow();
    }

    DEFINE_SIGNAL_OVERRIDE(void(), LeadingStarted);
    DEFINE_SIGNAL_OVERRIDE(void(), LeadingEnded);

private:
    const TControllerConfigPtr Config_;
    const TNodeInfoPtr NodeInfo_;
    const ICommonYTConnectorPtr CommonYTConnector_;
    const TControlActionQueuePtr ControlQueue_;
    const IInvokerPtr SerializedInvoker_;
    TFuture<void> PublisherFuture_;

    std::atomic<EYTConnectorState> State_ = EYTConnectorState::Disconnected;

    ILockElectionManagerPtr ElectionManager_;

private:
    TPrerequisiteId GetPrerequisiteIdImpl() const
    {
        YT_ASSERT_SERIALIZED_INVOKER_AFFINITY(SerializedInvoker_);

        ValidateLeader();

        return ElectionManager_->GetPrerequisiteId();
    }

    void DoConnect()
    {
        YT_ASSERT_SERIALIZED_INVOKER_AFFINITY(SerializedInvoker_);

        if (State_ != EYTConnectorState::Disconnected) {
            return;
        }

        DoCleanUp();

        State_.store(EYTConnectorState::Connecting);

        auto config = New<TCypressElectionManagerConfig>();
        config->LockPath = YPathJoin(GetPipelinePath().GetPath(), "leader_controller_lock");
        config->TransactionTimeout = Config_->ElectionManager->TransactionTimeout;
        config->TransactionPingPeriod = Config_->ElectionManager->TransactionPingPeriod;
        config->LockAcquisitionPeriod = Config_->ElectionManager->LockAcquisitionPeriod;
        config->LeaderCacheUpdatePeriod = Config_->ElectionManager->LeaderCacheUpdatePeriod;

        auto options = NYT::New<TCypressElectionManagerOptions>();
        auto attrs = NYT::NYTree::CreateEphemeralAttributes();
        attrs->Set("host", NodeInfo_->GetIdentifyingString());
        options->GroupName = "FlowController";
        options->MemberName = Format("%v(%v;%v)", NodeInfo_->Name, NodeInfo_->RpcAddress, NodeInfo_->IncarnationId);
        options->TransactionAttributes = std::move(attrs);
        ElectionManager_ = CreateCypressElectionManager(
            GetClient(),
            SerializedInvoker_,
            config,
            std::move(options));
        ElectionManager_->SubscribeLeadingStarted(BIND(&TYTConnector::DoLeadingStarted, MakeWeak(this)));
        ElectionManager_->SubscribeLeadingEnded(BIND(&TYTConnector::DoLeadingEnded, MakeWeak(this)));
        ElectionManager_->Start();
        State_.store(EYTConnectorState::Follower);
        YT_LOG_INFO("YTConnector following started");
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
        State_.store(EYTConnectorState::Disconnected);
        YT_LOG_INFO("YTConnector following stopped");
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
                        << TErrorAttribute("actual_incarnation_id", actualIncarnationId)
                        << TErrorAttribute("expected_incarnation_id", incarnationId);
                }
            }));
    }

    bool TryPublishLeadership()
    {
        YT_ASSERT_SERIALIZED_INVOKER_AFFINITY(SerializedInvoker_);
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
            YT_LOG_INFO("Published leader controller address (Address: %v)", NodeInfo_->RpcAddress);
        } catch (const std::exception& ex) {
            YT_LOG_EVENT(PublicControllerLogger, NLogging::ELogLevel::Error, ex, "Failed to publish leader_controller_address");
            return false;
        }

        try {
            WaitFor(CheckControllerLeaderNodeIncarnationIdExternally(NodeInfo_->IncarnationId)).ThrowOnError();
            YT_LOG_INFO("Confirmed published leader controller address (Address: %v)", NodeInfo_->RpcAddress);
        } catch (const std::exception& ex) {
            YT_LOG_EVENT(PublicControllerLogger, NLogging::ELogLevel::Error, ex, "Failed to confirm leader_controller_address");
            return false;
        }

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
            YT_LOG_INFO("Published leader controller address to flow_control table (Address: %v)", NodeInfo_->RpcAddress);
        } catch (const std::exception& ex) {
            YT_LOG_EVENT(PublicControllerLogger, NLogging::ELogLevel::Error, ex, "Failed to publish leader_controller to flow_control table");
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

        PublisherFuture_ = BIND(&TYTConnector::PublishLeadership, MakeWeak(this), ElectionManager_->GetPrerequisiteId())
            .AsyncVia(SerializedInvoker_)
            .Run();

        LeadingStarted_.Fire();
        YT_LOG_INFO("YTConnector leading started");
    }

    void DoLeadingEnded()
    {
        YT_ASSERT_SERIALIZED_INVOKER_AFFINITY(SerializedInvoker_);

        YT_VERIFY(State_ == EYTConnectorState::Leader);

        TForbidContextSwitchGuard contextSwitchGuard;
        DoCleanUp();

        State_.store(EYTConnectorState::Follower);

        PublisherFuture_.Cancel(TError("Leading ended"));

        LeadingEnded_.Fire();
        YT_LOG_INFO("YTConnector leading ended");
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
