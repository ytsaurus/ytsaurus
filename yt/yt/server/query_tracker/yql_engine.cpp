#include "yql_engine.h"

#include "config.h"
#include "handler_base.h"
#include "helpers.h"

#include <yt/yt/client/api/transaction.h>

#include <yt/yt/ytlib/query_tracker_client/records/query.record.h>

#include <yt/yt/ytlib/api/native/client.h>
#include <yt/yt/ytlib/api/native/connection.h>

#include <yt/yt/client/table_client/row_buffer.h>
#include <yt/yt/client/table_client/record_helpers.h>

#include <yt/yt/ytlib/yql_client/yql_service_proxy.h>
#include <yt/yt/ytlib/yql_client/public.h>
#include <yt/yt/ytlib/yql_client/config.h>

#include <yt/yt/core/ytree/convert.h>
#include <yt/yt/core/ytree/attributes.h>

#include <yt/yt/core/concurrency/action_queue.h>

#include <yt/yt/core/rpc/roaming_channel.h>

#include <yt/yt/core/yson/protobuf_helpers.h>

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
using namespace NSecurityClient;

////////////////////////////////////////////////////////////////////////////////

//! This macro may be used to extract std::optional<TYsonString> from protobuf message field of type string.
#define YT_PROTO_YSON_OPTIONAL(message, field) (((message).has_##field()) ? std::optional(TYsonString((message).field())) : std::nullopt)

////////////////////////////////////////////////////////////////////////////////

static NLogging::TLogger Logger("YqlEngine");

const std::string DefaultYqlAgentStageName = "production";

////////////////////////////////////////////////////////////////////////////////

struct TYqlSettings
    : public TYsonStruct
{
    std::optional<std::string> Stage;
    EExecuteMode ExecuteMode;
    EQueryType QueryType;

    REGISTER_YSON_STRUCT(TYqlSettings);

    static void Register(TRegistrar registrar)
    {
        registrar.Parameter("stage", &TThis::Stage)
            .Optional();
        registrar.Parameter("execution_mode", &TThis::ExecuteMode)
            .Default(EExecuteMode::Run);
        registrar.Parameter("query_type", &TThis::QueryType)
            .Default(EQueryType::Regular);
    }
};

DEFINE_REFCOUNTED_TYPE(TYqlSettings)
DECLARE_REFCOUNTED_STRUCT(TYqlSettings)

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
        , QueryType_(Settings_->QueryType)
        , Secrets_(MakeSecrets(activeQuery.Secrets))
        , ProgressGetterExecutor_(New<TPeriodicExecutor>(controlInvoker, BIND(&TYqlQueryHandler::GetProgress, MakeWeak(this)), Config_->QueryProgressGetPeriod))
    { }

    static std::vector<TQuerySecretPtr> MakeSecrets(const std::optional<TYsonString>& secrets)
    {
        return ConvertTo<std::optional<std::vector<TQuerySecretPtr>>>(secrets).value_or(std::vector<TQuerySecretPtr>());
    }

    void Start() override
    {
        if (QueryType_ != EQueryType::Regular) {
            if (IsIndexed_) {
                THROW_ERROR_EXCEPTION("Query of type %Qlv must not be indexed", QueryType_);
            }

            auto accessControlObjectList = ConvertTo<std::optional<std::vector<std::string>>>(AccessControlObjects_);
            if (!accessControlObjectList || accessControlObjectList->size() != 1 || (*accessControlObjectList)[0] != AdminAccessControlObjectName) {
                THROW_ERROR_EXCEPTION("Query of type %Qlv is expected to have only %Qv access control object set",
                    QueryType_,
                    AdminAccessControlObjectName);
            }

            if (CheckAccessControl(User_, AccessControlObjects_, StateClient_, EPermission::Administer) == ESecurityAction::Deny) {
                THROW_ERROR_EXCEPTION(NSecurityClient::EErrorCode::AuthorizationError,
                    "%Qlv permission required to run %Qlv queries",
                    EPermission::Administer,
                    QueryType_)
                    .With("user", User_)
                    .With("access_control_objects", AccessControlObjects_);
            }
        }

        auto providerInfo = Connection_->GetYqlAgentChannelProviderOrThrow(Stage_);
        YqlAgentChannelProvider_ = providerInfo.first;
        YqlAgentChannelProviderConfig_ = providerInfo.second;
        YqlServiceName_ = TYqlServiceProxy::GetDescriptor().ServiceName;
        TryStart();
    }

    void Abort() override
    {
        auto guard = Guard(QueryStateSpinLock_);

        if (QueryState_ == EYqlQueryState::Running) {
            // Nothing smarter than that for now.
            YT_UNUSED_FUTURE(ProgressGetterExecutor_->Stop());
            YT_UNUSED_FUTURE(StopProgressWriter());
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
            YT_UNUSED_FUTURE(StopProgressWriter());
            AsyncQueryResult_.Cancel(TError("Query detached"));
        }

        QueryState_ = EYqlQueryState::Aborted;
    }

private:
    const std::string Query_;
    const TYqlEngineConfigPtr Config_;
    const std::vector<TQueryFilePtr> Files_;
    const NApi::NNative::IConnectionPtr Connection_;
    const TYqlSettingsPtr Settings_;
    const std::string Stage_;
    const EExecuteMode ExecuteMode_;
    const EQueryType QueryType_;
    const IInvokerPtr ProgressInvoker_;
    const std::vector<TQuerySecretPtr> Secrets_;

    IRoamingChannelProviderPtr YqlAgentChannelProvider_;
    TYqlAgentChannelConfigPtr YqlAgentChannelProviderConfig_;
    std::string YqlServiceName_;

    IChannelPtr YqlServiceChannel_;
    TPeriodicExecutorPtr ProgressGetterExecutor_;

    TFuture<TTypedClientResponse<TRspStartQuery>::TResult> AsyncQueryResult_;

    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, QueryStateSpinLock_);
    EYqlQueryState QueryState_ = EYqlQueryState::Pending;

    std::optional<ui32> LastInMemoryProgressRevision_;
    std::optional<ui32> LastSavedProgressRevision_;
    TYsonString LastSavedYqlProgress_;
    THashMap<EProgressPart, std::pair<ui32, TYsonString>> ProgressPartsToSave_;

    void OnProgress(const TYqlResponse& rsp)
    {
        auto optionalRevision = rsp.has_revision() ? std::optional(rsp.revision()) : std::nullopt;
        auto optionalPlan = YT_PROTO_YSON_OPTIONAL(rsp, plan);
        auto optionalStatistics = YT_PROTO_YSON_OPTIONAL(rsp, statistics);
        auto optionalProgress = YT_PROTO_YSON_OPTIONAL(rsp, progress);
        auto optionalTaskInfo = YT_PROTO_YSON_OPTIONAL(rsp, task_info);
        auto optionalAst = rsp.has_ast() ? std::optional(rsp.ast()) : std::nullopt;

        if (!optionalRevision) {
            const bool hasContent = optionalPlan || optionalStatistics || optionalTaskInfo || optionalAst ||
                (optionalProgress && optionalProgress->AsStringBuf() != "{}");
            if (hasContent) {
                auto progress = BuildYsonStringFluently()
                    .BeginMap()
                        .OptionalItem(FormatEnum(EProgressPart::YqlPlan), optionalPlan)
                        .OptionalItem(FormatEnum(EProgressPart::YqlStatistics), optionalStatistics)
                        .OptionalItem(FormatEnum(EProgressPart::YqlProgress), optionalProgress)
                        .OptionalItem(FormatEnum(EProgressPart::YqlTaskInfo), optionalTaskInfo)
                        .OptionalItem(FormatEnum(EProgressPart::YqlAst), optionalAst)
                    .EndMap();
                TQueryHandlerBase::OnProgress(std::move(progress));
            }
            return;
        }
        auto revision = *optionalRevision;

        THashMap<EProgressPart, std::pair<ui32, TYsonString>> partsToSave;
        auto convertIfPresent = [&] (EProgressPart part, const auto& value) {
            if (value) {
                if (part == EProgressPart::YqlProgress) {
                    // yql_progress carries no revision of its own; Max makes it pass any min-revision filter.
                    partsToSave[part] = {Max<ui32>(), ConvertToYsonString(*value)};
                } else {
                    partsToSave[part] = {revision, ConvertToYsonString(*value)};
                }
            }
        };

        convertIfPresent(EProgressPart::YqlProgress, optionalProgress);
        convertIfPresent(EProgressPart::YqlPlan, optionalPlan);
        convertIfPresent(EProgressPart::YqlStatistics, optionalStatistics);
        convertIfPresent(EProgressPart::YqlTaskInfo, optionalTaskInfo);
        convertIfPresent(EProgressPart::YqlAst, optionalAst);
        convertIfPresent(EProgressPart::YqlRevision, optionalRevision);

        {
            auto moveIfPresent = [&] (EProgressPart part) {
                if (auto it = partsToSave.find(part); it != partsToSave.end()) {
                    ProgressPartsToSave_[part] = std::move(it->second);
                }
            };

            auto guard = Guard(ProgressSpinLock_);
            moveIfPresent(EProgressPart::YqlProgress);

            if (LastInMemoryProgressRevision_ && revision <= *LastInMemoryProgressRevision_) {
                return;
            }
            LastInMemoryProgressRevision_ = revision;

            moveIfPresent(EProgressPart::YqlPlan);
            moveIfPresent(EProgressPart::YqlStatistics);
            moveIfPresent(EProgressPart::YqlTaskInfo);
            moveIfPresent(EProgressPart::YqlAst);
            moveIfPresent(EProgressPart::YqlRevision);
        }
    }

    bool TryWriteProgress() override
    {
        bool inPartsMode;
        {
            auto guard = Guard(ProgressSpinLock_);
            inPartsMode = LastInMemoryProgressRevision_.has_value();
        }
        if (!inPartsMode) {
            return TQueryHandlerBase::TryWriteProgress();
        }

        ui32 lastInMemoryProgressRevision = 0;
        TYsonString lastSavedYqlProgress;

        THashMap<EProgressPart, std::pair<ui32, TYsonString>> partsToCompressAndSave;
        {
            auto guard = Guard(ProgressSpinLock_);
            lastInMemoryProgressRevision = *LastInMemoryProgressRevision_;
            lastSavedYqlProgress = LastSavedYqlProgress_;

            for (const auto& [part, revisionWithValue] : ProgressPartsToSave_) {
                if (part == EProgressPart::YqlProgress) {
                    // yql_progress carries no revision, so track it by value.
                    if (LastSavedYqlProgress_ == revisionWithValue.second) {
                        continue;
                    }
                    lastSavedYqlProgress = revisionWithValue.second;
                } else if (LastSavedProgressRevision_ && *LastSavedProgressRevision_ >= revisionWithValue.first) {
                    continue;
                }
                partsToCompressAndSave[part] = revisionWithValue;
            }
        }

        std::vector<NTableClient::TUnversionedRow> newRows;
        auto rowBuffer = New<NTableClient::TRowBuffer>();
        for (const auto& [part, revisionWithValue] : partsToCompressAndSave) {
            auto compressedValue = Compress(revisionWithValue.second.ToString(), MaxDyntableStringSize);

            NRecords::TQueryProgressPartial newRecord{
                .Key = {.QueryId = QueryId_, .PartName = FormatEnum(part)},
                .Revision = revisionWithValue.first,
                .PartValue = compressedValue,
            };
            newRows.push_back(newRecord.ToUnversionedRow(rowBuffer, NRecords::TQueryProgressDescriptor::Get()->GetPartialIdMapping()));
        }

        if (!newRows.empty()) {
            try {
                auto transaction = StartIncarnationTransaction().first;
                transaction->WriteRows(
                    StateRoot_ + "/query_progresses",
                    NRecords::TQueryProgressDescriptor::Get()->GetNameTable(),
                    MakeSharedRange(std::move(newRows), rowBuffer));

                WaitFor(transaction->Commit())
                    .ThrowOnError();

                {
                    auto guard = Guard(ProgressSpinLock_);
                    LastSavedProgressRevision_ = lastInMemoryProgressRevision;
                    LastSavedYqlProgress_ = lastSavedYqlProgress;
                }
            } catch (const std::exception& ex) {
                return OnProgressWriteFailed(ex);
            }
        }
        return true;
    }

    void TryStart()
    {
        YT_TLOG_DEBUG("Start YQL query attempt")
            .With("Stage", Stage_);
        auto yqlServiceChannel = WaitForFast(YqlAgentChannelProvider_->GetChannel(YqlServiceName_))
            .ValueOrThrow();

        // TODO(max42, gritukan): Implement long polling for YQL queries.
        auto yqlServiceChannelWithBigTimeout = CreateDefaultTimeoutChannel(yqlServiceChannel, Config_->StartQueryRpcTimeout);

        TYqlServiceProxy proxy(yqlServiceChannelWithBigTimeout);
        auto startQueryReq = proxy.StartQuery();
        SetAuthenticationIdentity(startQueryReq, TAuthenticationIdentity(User_));
        auto* yqlRequest = startQueryReq->mutable_yql_request();
        startQueryReq->set_row_count_limit(Config_->RowCountLimit);
        ToProto(startQueryReq->mutable_query_id(), QueryId_);
        yqlRequest->set_query(Query_);
        yqlRequest->set_settings(ToProto(ConvertToYsonString(SettingsNode_)));
        yqlRequest->set_mode(ToProto(ExecuteMode_));
        yqlRequest->set_query_type(ToProto(QueryType_));

        for (const auto& file : Files_) {
            auto* protoFile = yqlRequest->add_files();
            protoFile->set_name(file->Name);
            protoFile->set_content(file->Content);
            protoFile->set_type(static_cast<TYqlQueryFile_EContentType>(file->Type));
        }

        for (const auto& secret : Secrets_) {
            const auto protoSecret = yqlRequest->add_secrets();
            protoSecret->set_id(secret->Id);
            protoSecret->set_category(secret->Category);
            protoSecret->set_subcategory(secret->Subcategory);
            protoSecret->set_ypath(secret->YPath);
        }

        startQueryReq->set_build_rowsets(true);

        {
            auto guard = Guard(QueryStateSpinLock_);
            if (QueryState_ != EYqlQueryState::Pending && QueryState_ != EYqlQueryState::Throttled) {
                YT_TLOG_DEBUG("Start YQL query attempt failed, query is not in pending or throttled state")
                    .With("State", QueryState_);
                return;
            }

            YT_TLOG_DEBUG("Start YQL query")
                .With("Stage", Stage_)
                .With("Channel", yqlServiceChannel->GetEndpointDescription());

            QueryState_ = EYqlQueryState::Running;

            AsyncQueryResult_ = startQueryReq->Invoke();
            AsyncQueryResult_.Subscribe(BIND(&TYqlQueryHandler::OnYqlResponse, MakeWeak(this)).Via(GetCurrentInvoker()));

            YqlServiceChannel_ = yqlServiceChannel;
            ProgressGetterExecutor_->Start();
            StartProgressWriter();
        }

        OnQueryStarted(yqlServiceChannel->GetEndpointDescription());
    }

    void GetProgress()
    {
        TYqlServiceProxy proxy(YqlServiceChannel_);
        proxy.SetDefaultTimeout(YqlAgentChannelProviderConfig_->DefaultProgressRequestTimeout);
        auto req = proxy.GetQueryProgress();
        ToProto(req->mutable_query_id(), QueryId_);
        {
            auto guard = Guard(ProgressSpinLock_);
            if (LastInMemoryProgressRevision_) {
                req->set_revision(*LastInMemoryProgressRevision_);
            }
        }

        auto rspOrError = WaitFor(req->Invoke());
        if (!rspOrError.IsOK()) {
            YT_TLOG_INFO("Error getting query progress")
                .With("QueryId", QueryId_)
                .With(rspOrError);
            return;
        }

        const auto& rsp = rspOrError.Value();
        if (!rsp->has_yql_response()) {
            // There are no changes in progress since last request.
            return;
        }

        OnProgress(rsp->yql_response());
    }

    void OnYqlResponse(const TErrorOr<TTypedClientResponse<TRspStartQuery>::TResult>& rspOrError)
    {
        // Waiting to exclude the possibility of overwriting the final progress.
        WaitFor(ProgressGetterExecutor_->Stop())
            .ThrowOnError();
        WaitFor(StopProgressWriter())
            .ThrowOnError();
        if (rspOrError.FindMatching(NYT::EErrorCode::Canceled)) {
            return;
        }

        if (rspOrError.FindMatching({
            NYqlClient::EErrorCode::RequestThrottled,
            NYqlClient::EErrorCode::YqlAgentBanned,
            NYqlClient::EErrorCode::YqlAgentNotReady,
        }))
        {
            {
                auto guard = Guard(QueryStateSpinLock_);
                QueryState_ = EYqlQueryState::Throttled;
            }
            OnQueryThrottled();
            TDelayedExecutor::WaitForDuration(Config_->StartQueryAttemptPeriod);
            try {
                TryStart();
            } catch (const std::exception& ex) {
                YT_TLOG_INFO("Unrecoverable error on query start, finishing query")
                    .With(ex);
                OnQueryFailed(TError(ex));
            }
            return;
        }

        if (!rspOrError.IsOK()) {
            WriteProgress();
            OnQueryFailed(rspOrError);
            return;
        }

        const auto& rsp = rspOrError.Value();
        OnProgress(rsp->yql_response());

        if (rsp->yql_response().has_error()) {
            WriteProgress();

            auto error = ConvertTo<TError>(TYsonString(rsp->yql_response().error()));
            OnQueryFailed(TError("Failed to run query")
                .With("query_id", QueryId_)
                .With(std::move(error)));
            return;
        }

        std::vector<TErrorOr<TWireRowset>> wireRowsetOrErrors;
        for (int index = 0; index < rsp->rowset_errors_size(); ++index) {
            auto error = FromProto<TError>(rsp->rowset_errors()[index]);
            if (error.IsOK()) {
                TYsonString fullResult;
                if (index < rsp->full_result_size()) {
                    if (const auto& rawFullResult = rsp->full_result()[index]) {
                        fullResult = TYsonString(rawFullResult);
                    }
                }
                wireRowsetOrErrors.push_back(TWireRowset{
                    .Rowset = rsp->Attachments()[index],
                    .IsTruncated = rsp->incomplete()[index],
                    .FullResult = std::move(fullResult),
                });
            } else {
                wireRowsetOrErrors.push_back(error);
            }
        }
        WriteProgress();
        OnQueryCompletedWire(wireRowsetOrErrors);
    }
};

////////////////////////////////////////////////////////////////////////////////

class TProxyYqlEngineProvider
    : public IProxyEngineProvider
{
public:
    TProxyYqlEngineProvider(IClientPtr stateClient, TYPath stateRoot)
        : StateClient_(std::move(stateClient))
        , StateRoot_(std::move(stateRoot))
    { }

    TYsonString GetEngineInfo(IMapNodePtr settingsMap) override
    {
        auto stage = settingsMap->GetChildValueOrDefault("yql_agent_stage", DefaultYqlAgentStageName);

        auto proxy = CreateProxy(stage);

        YT_TLOG_DEBUG("Sending YQLA GetYqlAgentInfo request");

        auto getYqlAgentInfoRequest = proxy.GetYqlAgentInfo();
        getYqlAgentInfoRequest->SetTimeout(TDuration::Seconds(10));
        auto getYqlAgentInfoResponse = WaitFor(getYqlAgentInfoRequest->Invoke())
            .ValueOrThrow();

        std::vector<std::string> availableYqlVersions;
        std::string defaultYqlUIVersion;

        FromProto(&availableYqlVersions, getYqlAgentInfoResponse->available_yql_versions());
        FromProto(&defaultYqlUIVersion, getYqlAgentInfoResponse->default_ui_yql_version());
        YT_TLOG_DEBUG("GetYqlAgentInfo response received")
            .With("AvailableVersions", availableYqlVersions)
            .With("DefaultYqlUIVersion", defaultYqlUIVersion);

        return BuildYsonStringFluently()
            .BeginMap()
                .Item("available_yql_versions").Value(availableYqlVersions)
                .Item("default_yql_ui_version").Value(defaultYqlUIVersion)
                .Item("supported_features").Value(getYqlAgentInfoResponse->has_supported_features()
                    ? TYsonString(getYqlAgentInfoResponse->supported_features())
                    : TYsonString(TString("{}")))
            .EndMap();
    }

    TYsonString GetDeclaredParametersInfo(const std::string& query, const TYsonString& settings) override
    {
        auto stage = ConvertTo<TYqlSettingsPtr>(ConvertToNode(settings))->Stage.value_or(DefaultYqlAgentStageName);

        auto proxy = CreateProxy(stage);

        YT_TLOG_DEBUG("Sending YQLA GetDeclaredParametersInfo request");

        auto getDeclaredParametersInfoRequest = proxy.GetDeclaredParametersInfo();
        getDeclaredParametersInfoRequest->SetTimeout(TDuration::Seconds(10));

        getDeclaredParametersInfoRequest->set_query(query);
        getDeclaredParametersInfoRequest->set_settings(ToProto(settings));

        auto getDeclaredParametersInfoResponse = WaitFor(getDeclaredParametersInfoRequest->Invoke())
            .ValueOrThrow();

        auto declaredParametersInfo = TYsonString(TString(getDeclaredParametersInfoResponse->declared_parameters_info()));
        YT_TLOG_DEBUG("GetDeclaredParametersInfo response received")
            .With("DeclaredParametersInfo", declaredParametersInfo);

        static const TYsonString EmptyMap = TYsonString(TString("{}"));
        auto rawParametersNode = ConvertToNode(declaredParametersInfo);
        if (rawParametersNode->GetType() != ENodeType::Map) {
            YT_TLOG_DEBUG("Declared parameters node received from YQL facade has incorrect type; expected map")
                .With("NodeType", rawParametersNode->GetType());
            return EmptyMap;
        }
        auto processedParameters = ConvertToNode(EmptyMap)->AsMap();
        for (const auto& [key, valueNode] : rawParametersNode->AsMap()->GetChildren()) {
            if (valueNode->GetType() != ENodeType::List) {
                YT_TLOG_DEBUG("Node with declared parameter info has incorrect type; expected list")
                    .With("NodeType", valueNode->GetType());
                continue;
            }
            auto list = valueNode->AsList()->GetChildren();
            if (list.size() != 2 || list[0]->AsString()->GetValue() != "DataType") {
                YT_TLOG_DEBUG("Declared parameter info list has incorrect format; expected first element to be 'DataType' and second to be parameter type")
                    .With("ParameterInfoList", ConvertToYsonString(valueNode));
                continue;
            }
            processedParameters->AddChild(key, ConvertToNode(list[1]->AsString()->GetValue()));
        }
        return ConvertToYsonString(processedParameters);
    }

private:
    const IClientPtr StateClient_;
    const TYPath StateRoot_;

    TYqlServiceProxy CreateProxy(const std::string& stage)
    {
        auto connection = DynamicPointerCast<NNative::IConnection>(StateClient_->GetConnection());

        auto providerInfo = connection->GetYqlAgentChannelProviderOrThrow(stage);
        auto yqlAgentChannelProvider = providerInfo.first;
        auto yqlServiceName = NYqlClient::TYqlServiceProxy::GetDescriptor().ServiceName;

        auto yqlAgentChannel = WaitForFast(yqlAgentChannelProvider->GetChannel(yqlServiceName))
            .ValueOrThrow();
        TYqlServiceProxy proxy(yqlAgentChannel);

        return proxy;
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
        , ProxyEngineProvider_(New<TProxyYqlEngineProvider>(StateClient_, StateRoot_))
    { }

    bool IsSafeToRestartQuery() const override
    {
        return false;
    }

    IQueryHandlerPtr StartOrAttachQuery(NRecords::TActiveQuery activeQuery) override
    {
        YT_ASSERT_THREAD_AFFINITY(ControlThread);

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
        YT_ASSERT_THREAD_AFFINITY(ControlThread);

        Config_ = DynamicPointerCast<TYqlEngineConfig>(config);
    }

    std::optional<IProxyEngineProviderPtr> GetProxyEngineProvider() override
    {
        return ProxyEngineProvider_;
    }

private:
    const IClientPtr StateClient_;
    const TYPath StateRoot_;
    const TActionQueuePtr ControlQueue_;
    const IProxyEngineProviderPtr ProxyEngineProvider_;
    TYqlEngineConfigPtr Config_;

    DECLARE_THREAD_AFFINITY_SLOT(ControlThread);
};

////////////////////////////////////////////////////////////////////////////////

IQueryEnginePtr CreateYqlEngine(const IClientPtr& stateClient, const TYPath& stateRoot)
{
    return New<TYqlEngine>(stateClient, stateRoot);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryTracker
