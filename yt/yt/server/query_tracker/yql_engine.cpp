#include "yql_engine.h"

#include "config.h"
#include "handler_base.h"

#include <yt/yt/ytlib/query_tracker_client/records/query.record.h>

#include <yt/yt/ytlib/api/native/client.h>
#include <yt/yt/ytlib/api/native/connection.h>

#include <yt/yt/ytlib/yql_client/yql_service_proxy.h>
#include <yt/yt/ytlib/yql_client/public.h>

#include <yt/yt/core/ytree/convert.h>
#include <yt/yt/core/ytree/attributes.h>

#include <yt/yt/core/rpc/roaming_channel.h>

namespace NYT::NQueryTracker {

using namespace NQueryTrackerClient;
using namespace NApi;
using namespace NYPath;
using namespace NHiveClient;
using namespace NYTree;
using namespace NRpc;
using namespace NYqlClient;
using namespace NYqlClient::NProto;
using namespace NYson;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

//! This macro may be used to extract std::optional<TYsonString> from protobuf message field of type string.
#define YT_PROTO_YSON_OPTIONAL(message, field) (((message).has_##field()) ? std::optional(TYsonString((message).field())) : std::nullopt)

////////////////////////////////////////////////////////////////////////////////

class TYqlSettings
    : public TYsonStruct
{
public:
    std::optional<TString> Stage;
    EExecuteMode ExecuteMode;

    REGISTER_YSON_STRUCT(TYqlSettings);

    static void Register(TRegistrar registrar)
    {
        registrar.Parameter("stage", &TThis::Stage)
            .Optional();
        registrar.Parameter("execution_mode", &TThis::ExecuteMode)
            .Default(EExecuteMode::Run);
    }
};

DEFINE_REFCOUNTED_TYPE(TYqlSettings)
DECLARE_REFCOUNTED_CLASS(TYqlSettings)

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EYqlQueryState,
    ((Invalid)   (-1))
    ((Pending)   (0))
    ((Running)   (2))
    ((Throttled) (3))
    ((Aborted)   (4))
);

class TYqlQueryHandler
    : public TQueryHandlerBase
{
public:
    TYqlQueryHandler(
        const NApi::IClientPtr& stateClient,
        const NYPath::TYPath& stateRoot,
        const TYqlEngineConfigPtr& config,
        const NQueryTrackerClient::NRecords::TActiveQuery& activeQuery,
        const NApi::NNative::IConnectionPtr& connection,
        const IInvokerPtr& controlInvoker)
        : TQueryHandlerBase(stateClient, stateRoot, controlInvoker, config, activeQuery)
        , Query_(activeQuery.Query)
        , Config_(config)
        , Files_(ConvertTo<std::optional<std::vector<TQueryFilePtr>>>(activeQuery.Files).value_or(std::vector<TQueryFilePtr>()))
        , Connection_(connection)
        , Settings_(ConvertTo<TYqlSettingsPtr>(SettingsNode_))
        , Stage_(Settings_->Stage.value_or(Config_->Stage))
        , ExecuteMode_(Settings_->ExecuteMode)
        , ProgressGetterExecutor_(New<TPeriodicExecutor>(controlInvoker, BIND(&TYqlQueryHandler::GetProgress, MakeWeak(this)), Config_->QueryProgressGetPeriod))
    { }

    void Start() override
    {
        YqlAgentChannelProvider_ = Connection_->GetYqlAgentChannelProviderOrThrow(Stage_);
        YqlServiceName_ = TYqlServiceProxy::GetDescriptor().ServiceName;
        TryStart();
    }

    void Abort() override
    {
        auto guard = Guard(QueryStateSpinLock_);

        if (QueryState_ == EYqlQueryState::Running) {
            // Nothing smarter than that for now.
            YT_UNUSED_FUTURE(ProgressGetterExecutor_->Stop());
            StopProgressWriter();
            AsyncQueryResult_.Cancel(TError("Query aborted"));
        }

        QueryState_ = EYqlQueryState::Aborted;
    }

    void Detach() override
    {
        auto guard = Guard(QueryStateSpinLock_);

        if (QueryState_ == EYqlQueryState::Running) {
            // Nothing smarter than that for now.
            YT_UNUSED_FUTURE(ProgressGetterExecutor_->Stop());
            StopProgressWriter();
            AsyncQueryResult_.Cancel(TError("Query detached"));
        }

        QueryState_ = EYqlQueryState::Aborted;
    }

private:
    const TString Query_;
    const TYqlEngineConfigPtr Config_;
    const std::vector<TQueryFilePtr> Files_;
    const NApi::NNative::IConnectionPtr Connection_;
    const TYqlSettingsPtr Settings_;
    const TString Stage_;
    const EExecuteMode ExecuteMode_;
    const IInvokerPtr ProgressInvoker_;

    IRoamingChannelProviderPtr YqlAgentChannelProvider_;
    TString YqlServiceName_;

    IChannelPtr YqlServiceChannel_;
    TPeriodicExecutorPtr ProgressGetterExecutor_;

    TFuture<TTypedClientResponse<TRspStartQuery>::TResult> AsyncQueryResult_;

    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, QueryStateSpinLock_);
    EYqlQueryState QueryState_ = EYqlQueryState::Pending;

    void TryStart()
    {
        YT_LOG_DEBUG("Start YQL query attempt (Stage: %v)", Stage_);
        auto yqlServiceChannel = WaitForFast(YqlAgentChannelProvider_->GetChannel(YqlServiceName_))
            .ValueOrThrow();

        // TODO(max42, gritukan): Implement long polling for YQL queries.
        auto yqlServiceChannelWithBigTimeout = CreateDefaultTimeoutChannel(yqlServiceChannel, TDuration::Days(1));

        TYqlServiceProxy proxy(yqlServiceChannelWithBigTimeout);
        auto startQueryReq = proxy.StartQuery();
        SetAuthenticationIdentity(startQueryReq, TAuthenticationIdentity(User_));
        auto* yqlRequest = startQueryReq->mutable_yql_request();
        startQueryReq->set_row_count_limit(Config_->RowCountLimit);
        ToProto(startQueryReq->mutable_query_id(), QueryId_);
        yqlRequest->set_query(Query_);
        yqlRequest->set_settings(ConvertToYsonString(SettingsNode_).ToString());
        yqlRequest->set_mode(ToProto<int>(ExecuteMode_));

        for (const auto& file : Files_) {
            auto* protoFile = yqlRequest->add_files();
            protoFile->set_name(file->Name);
            protoFile->set_content(file->Content);
            protoFile->set_type(static_cast<TYqlQueryFile_EContentType>(file->Type));
        }
        startQueryReq->set_build_rowsets(true);

        {
            auto guard = Guard(QueryStateSpinLock_);
            if (QueryState_ != EYqlQueryState::Pending && QueryState_ != EYqlQueryState::Throttled) {
                YT_LOG_DEBUG("Start YQL query attempt failed, query is not in pending or throttled state (State: %v)", QueryState_);
                return;
            }

            YT_LOG_DEBUG("Start YQL query (Stage: %v, Channel: %v)",
                Stage_,
                yqlServiceChannel->GetEndpointDescription());

            QueryState_ = EYqlQueryState::Running;

            AsyncQueryResult_ = startQueryReq->Invoke();
            AsyncQueryResult_.Subscribe(BIND(&TYqlQueryHandler::OnYqlResponse, MakeWeak(this)).Via(GetCurrentInvoker()));

            YqlServiceChannel_ = yqlServiceChannel;
            ProgressGetterExecutor_->Start();
            StartProgressWriter();
        }

        OnQueryStarted();
    }

    void GetProgress()
    {
        TYqlServiceProxy proxy(YqlServiceChannel_);
        auto req = proxy.GetQueryProgress();
        ToProto(req->mutable_query_id(), QueryId_);

        auto rspOrError = WaitFor(req->Invoke());
        if (!rspOrError.IsOK()) {
            YT_LOG_INFO(rspOrError, "Error getting query progress (QueryId: %v)", QueryId_);
            return;
        }

        const auto& rsp = rspOrError.Value();
        if (!rsp->has_yql_response()) {
            // There are no changes in progress since last request.
            return;
        }

        auto optionalPlan = YT_PROTO_YSON_OPTIONAL(rsp->yql_response(), plan);
        auto optionalProgress = YT_PROTO_YSON_OPTIONAL(rsp->yql_response(), progress);

        auto progress = BuildYsonStringFluently()
            .BeginMap()
                .OptionalItem("yql_plan", optionalPlan)
                .OptionalItem("yql_progress", optionalProgress)
            .EndMap();
        OnProgress(std::move(progress));
    }

    void OnYqlResponse(const TErrorOr<TTypedClientResponse<TRspStartQuery>::TResult>& rspOrError)
    {
        // Waiting to exclude the possibility of overwriting the final progress.
        WaitFor(ProgressGetterExecutor_->Stop())
            .ThrowOnError();
        StopProgressWriter();
        if (rspOrError.FindMatching(NYT::EErrorCode::Canceled)) {
            return;
        }

        if (rspOrError.FindMatching(NYqlClient::EErrorCode::RequestThrottled)) {
            {
                auto guard = Guard(QueryStateSpinLock_);
                QueryState_ = EYqlQueryState::Throttled;
            }
            OnQueryThrottled();
            TDelayedExecutor::WaitForDuration(Config_->StartQueryAttemptPeriod);
            TryStart();
            return;
        }

        if (!rspOrError.IsOK()) {
            OnQueryFailed(rspOrError);
            return;
        }

        const auto& rsp = rspOrError.Value();

        auto optionalPlan = YT_PROTO_YSON_OPTIONAL(rsp->yql_response(), plan);
        auto optionalStatistics = YT_PROTO_YSON_OPTIONAL(rsp->yql_response(), statistics);
        auto optionalProgress = YT_PROTO_YSON_OPTIONAL(rsp->yql_response(), progress);
        auto optionalTaskInfo = YT_PROTO_YSON_OPTIONAL(rsp->yql_response(), task_info);
        auto progress = BuildYsonStringFluently()
            .BeginMap()
                .OptionalItem("yql_plan", optionalPlan)
                .OptionalItem("yql_statistics", optionalStatistics)
                .OptionalItem("yql_progress", optionalProgress)
                .OptionalItem("yql_task_info", optionalTaskInfo)
            .EndMap();
        OnProgress(std::move(progress));

        std::vector<TErrorOr<TWireRowset>> wireRowsetOrErrors;
        for (int index = 0; index < rsp->rowset_errors_size(); ++index) {
            auto error = FromProto<TError>(rsp->rowset_errors()[index]);
            if (error.IsOK()) {
                wireRowsetOrErrors.push_back(TWireRowset{.Rowset = rsp->Attachments()[index], .IsTruncated = rsp->incomplete()[index]});
            } else {
                wireRowsetOrErrors.push_back(error);
            }
        }
        OnQueryCompletedWire(wireRowsetOrErrors);
    }
};

////////////////////////////////////////////////////////////////////////////////

class TYqlEngine
    : public IQueryEngine
{
public:
    TYqlEngine(IClientPtr stateClient, TYPath stateRoot)
        : StateClient_(std::move(stateClient))
        , StateRoot_(std::move(stateRoot))
        , ControlQueue_(New<TActionQueue>("YqlEngineControl"))
    { }

    IQueryHandlerPtr StartOrAttachQuery(NRecords::TActiveQuery activeQuery) override
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        return New<TYqlQueryHandler>(
            StateClient_,
            StateRoot_,
            Config_,
            activeQuery,
            DynamicPointerCast<NNative::IConnection>(StateClient_->GetConnection()),
            ControlQueue_->GetInvoker());
    }

    void Reconfigure(const TEngineConfigBasePtr& config) override
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        Config_ = DynamicPointerCast<TYqlEngineConfig>(config);
    }

private:
    const IClientPtr StateClient_;
    const TYPath StateRoot_;
    const TActionQueuePtr ControlQueue_;
    TYqlEngineConfigPtr Config_;

    DECLARE_THREAD_AFFINITY_SLOT(ControlThread);
};

////////////////////////////////////////////////////////////////////////////////

IQueryEnginePtr CreateYqlEngine(const IClientPtr& stateClient, const TYPath& stateRoot)
{
    return New<TYqlEngine>(stateClient, stateRoot);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryClient
