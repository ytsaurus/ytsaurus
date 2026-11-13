#include "yql_engine.h"

#include "config.h"
#include "handler_base.h"
#include "helpers.h"

#include <yt/yt/client/api/transaction.h>

#include <yt/yt/ytlib/query_tracker_client/records/query.record.h>

#include <yt/yt/ytlib/api/native/client.h>
#include <yt/yt/ytlib/api/native/connection.h>

#include <yt/yt/client/query_client/query_builder.h>

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

namespace {

EProgressPart GetTruncatedProgressPart(EProgressPart part)
{
    switch (part) {
        case EProgressPart::YqlPlan:
            return EProgressPart::YqlPlanTruncated;
        case EProgressPart::YqlStatistics:
            return EProgressPart::YqlStatisticsTruncated;
        case EProgressPart::YqlProgress:
            return EProgressPart::YqlProgressTruncated;
        case EProgressPart::YqlTaskInfo:
            return EProgressPart::YqlTaskInfoTruncated;
        case EProgressPart::YqlAst:
            return EProgressPart::YqlAstTruncated;
        default:
            YT_ABORT();
    }
}

} // namespace

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
        const IInvokerPtr& controlInvoker,
        std::optional<std::string> adminsGroup)
        : TQueryHandlerBase(stateClient, stateRoot, controlInvoker, config, activeQuery)
        , Query_(activeQuery.Query)
        , Config_(config)
        , AdminsGroup_(std::move(adminsGroup))
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

            auto securityAction = CheckAccessControl(
                User_,
                AccessControlObjects_,
                StateClient_,
                EPermission::Administer,
                AdminsGroup_);

            if (securityAction == ESecurityAction::Deny) {
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
    const std::optional<std::string> AdminsGroup_;
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

    YT_DECLARE_SPIN_LOCK(TSpinLock, QueryStateSpinLock_);
    EYqlQueryState QueryState_ = EYqlQueryState::Pending;

    std::optional<ui32> LastInMemoryProgressRevision_;
    std::optional<ui32> LastSavedProgressRevision_;
    std::optional<ui32> ProgressTruncatedRevision_;

    std::optional<ui32> LastInMemoryMainRevision_;
    std::optional<ui32> LastSavedMainRevision_;

    THashMap<std::string, std::pair<ui32, TYsonString>> ProgressSubPartsToSave_;
    THashMap<EProgressPart, std::pair<ui32, TYsonString>> MainProgressPartsToSave_;

    void OnProgress(const TYqlResponse& rsp)
    {
        auto optionalMainRevision = rsp.has_main_revision() ? std::optional(rsp.main_revision()) : std::nullopt;
        auto optionalProgressRevision = rsp.has_progress_revision() ? std::optional(rsp.progress_revision()) : std::nullopt;
        auto optionalPlan = YT_PROTO_YSON_OPTIONAL(rsp, plan);
        auto optionalStatistics = YT_PROTO_YSON_OPTIONAL(rsp, statistics);
        auto optionalProgress = YT_PROTO_YSON_OPTIONAL(rsp, progress);
        auto optionalProgressTruncated = rsp.has_progress_truncated() ? std::optional(rsp.progress_truncated()) : std::nullopt;
        auto optionalTaskInfo = YT_PROTO_YSON_OPTIONAL(rsp, task_info);
        auto optionalAst = rsp.has_ast() ? std::optional(rsp.ast()) : std::nullopt;

        if (!optionalMainRevision && !optionalProgressRevision) {
            const bool hasContent = optionalPlan || optionalStatistics || optionalTaskInfo || optionalAst || optionalProgressTruncated ||
                (optionalProgress && optionalProgress->AsStringBuf() != "{}");
            if (hasContent) {
                auto progress = BuildYsonStringFluently()
                    .BeginMap()
                        .OptionalItem(FormatEnum(EProgressPart::YqlPlan), optionalPlan)
                        .OptionalItem(FormatEnum(EProgressPart::YqlStatistics), optionalStatistics)
                        .OptionalItem(FormatEnum(EProgressPart::YqlProgress), optionalProgress)
                        .OptionalItem(FormatEnum(EProgressPart::YqlProgressTruncated), optionalProgressTruncated)
                        .OptionalItem(FormatEnum(EProgressPart::YqlTaskInfo), optionalTaskInfo)
                        .OptionalItem(FormatEnum(EProgressPart::YqlAst), optionalAst)
                    .EndMap();
                TQueryHandlerBase::OnProgress(std::move(progress));
            }
            return;
        }

        THashMap<std::string, TYsonString> progressSubParts;
        if (optionalProgressRevision && optionalProgress) {
            auto progressMap = ConvertToNode(*optionalProgress)->AsMap();
            for (const auto& [name, value] : progressMap->GetChildren()) {
                progressSubParts[name] = ConvertToYsonString(value);
            }
        }

        THashMap<EProgressPart, TYsonString> mainParts;
        if (optionalMainRevision) {
            auto convertIfPresent = [&] (EProgressPart part, const auto& value) {
                if (value) {
                    mainParts[part] = ConvertToYsonString(*value);
                }
            };
            convertIfPresent(EProgressPart::YqlPlan, optionalPlan);
            convertIfPresent(EProgressPart::YqlStatistics, optionalStatistics);
            convertIfPresent(EProgressPart::YqlTaskInfo, optionalTaskInfo);
            convertIfPresent(EProgressPart::YqlAst, optionalAst);
        }

        auto guard = Guard(ProgressSpinLock_);
        if (optionalProgressRevision &&
            (!LastInMemoryProgressRevision_ || *optionalProgressRevision > *LastInMemoryProgressRevision_))
        {
            for (auto& [name, value] : progressSubParts) {
                ProgressSubPartsToSave_[name] = {*optionalProgressRevision, std::move(value)};
            }
            if (optionalProgressTruncated.value_or(false)) {
                ProgressTruncatedRevision_ = optionalProgressRevision;
            }
            LastInMemoryProgressRevision_ = optionalProgressRevision;
        }

        if (optionalMainRevision &&
            (!LastInMemoryMainRevision_ || *optionalMainRevision > *LastInMemoryMainRevision_))
        {
            for (auto& [part, value] : mainParts) {
                MainProgressPartsToSave_[part] = {*optionalMainRevision, std::move(value)};
            }
            LastInMemoryMainRevision_ = optionalMainRevision;
        }
    }

    bool TryWriteProgress() override
    {
        bool inPartsMode;
        std::optional<ui32> lastInMemoryProgressRevision;
        std::optional<ui32> lastInMemoryMainRevision;
        bool progressTruncated = false;
        THashMap<std::string, std::pair<ui32, TYsonString>> progressSubPartsToSave;
        THashMap<EProgressPart, std::pair<ui32, TYsonString>> mainPartsToSave;
        {
            auto guard = Guard(ProgressSpinLock_);
            lastInMemoryProgressRevision = LastInMemoryProgressRevision_;
            lastInMemoryMainRevision = LastInMemoryMainRevision_;
            inPartsMode = lastInMemoryProgressRevision.has_value() || lastInMemoryMainRevision.has_value();
            progressTruncated = ProgressTruncatedRevision_ &&
                (!LastSavedProgressRevision_ || *LastSavedProgressRevision_ < *ProgressTruncatedRevision_);

            for (const auto& [name, revisionWithValue] : ProgressSubPartsToSave_) {
                if (!LastSavedProgressRevision_ || *LastSavedProgressRevision_ < revisionWithValue.first) {
                    progressSubPartsToSave[name] = revisionWithValue;
                }
            }
            for (const auto& [part, revisionWithValue] : MainProgressPartsToSave_) {
                if (!LastSavedMainRevision_ || *LastSavedMainRevision_ < revisionWithValue.first) {
                    mainPartsToSave[part] = revisionWithValue;
                }
            }
        }
        if (!inPartsMode) {
            return TQueryHandlerBase::TryWriteProgress();
        }

        std::vector<NTableClient::TUnversionedRow> newRows;
        newRows.reserve(progressSubPartsToSave.size() + mainPartsToSave.size() * 2 + 1);
        auto rowBuffer = New<NTableClient::TRowBuffer>();
        auto addRow = [&] (const std::string& partName, const std::string& subPartName, ui32 revision, const std::string& value) {
            auto compressedValue = Compress(value);
            bool truncated = compressedValue.size() > static_cast<size_t>(MaxDyntableStringSize);
            if (truncated) {
                compressedValue = CompressedEmptyMap;
            }
            NRecords::TQueryProgressPartial newRecord{
                .Key = {
                    .QueryId = QueryId_,
                    .PartName = partName,
                    .SubPartName = subPartName,
                },
                .Revision = revision,
                .PartValue = compressedValue,
            };
            newRows.push_back(newRecord.ToUnversionedRow(rowBuffer, NRecords::TQueryProgressDescriptor::Get()->GetPartialIdMapping()));
            return truncated;
        };
        for (const auto& [name, revisionWithValue] : progressSubPartsToSave) {
            progressTruncated |= addRow(
                FormatEnum(EProgressPart::YqlProgress),
                name,
                revisionWithValue.first,
                revisionWithValue.second.ToString());
        }
        if (progressTruncated) {
            addRow(
                FormatEnum(EProgressPart::YqlProgressTruncated),
                /*subPartName*/ {},
                lastInMemoryProgressRevision.value(),
                ConvertToYsonString(true).ToString());
        }
        for (const auto& [part, revisionWithValue] : mainPartsToSave) {
            auto partName = FormatEnum(part);
            bool truncated = addRow(partName, /*subPartName*/ {}, revisionWithValue.first, revisionWithValue.second.ToString());
            if (truncated) {
                addRow(
                    FormatEnum(GetTruncatedProgressPart(part)),
                    /*subPartName*/ {},
                    revisionWithValue.first,
                    ConvertToYsonString(true).ToString());
            }
        }

        if (!newRows.empty()) {
            try {
                auto transaction = StartIncarnationTransaction().first;
                // Prevent progress writes from racing with incarnation changes or query completion.
                NRecords::TActiveQueryKey key{.QueryId = QueryId_};
                std::vector keys{key.ToKey(rowBuffer)};
                transaction->LockRows(
                    StateRoot_ + "/active_queries",
                    NRecords::TActiveQueryDescriptor::Get()->GetNameTable(),
                    MakeSharedRange(std::move(keys), rowBuffer),
                    std::vector<std::string>{"query_tracker"},
                    NTableClient::ELockType::SharedStrong);

                transaction->WriteRows(
                    StateRoot_ + "/active_query_progresses",
                    NRecords::TQueryProgressDescriptor::Get()->GetNameTable(),
                    MakeSharedRange(std::move(newRows), rowBuffer));

                WaitFor(transaction->Commit())
                    .ThrowOnError();

                {
                    auto guard = Guard(ProgressSpinLock_);
                    LastSavedProgressRevision_ = lastInMemoryProgressRevision;
                    LastSavedMainRevision_ = lastInMemoryMainRevision;
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
            if (LastInMemoryMainRevision_) {
                req->mutable_progress_options()->set_main_revision(*LastInMemoryMainRevision_);
            }
            if (LastInMemoryProgressRevision_) {
                req->mutable_progress_options()->set_progress_revision(*LastInMemoryProgressRevision_);
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
    TYqlEngine(
        IClientPtr stateClient,
        TYPath stateRoot,
        std::optional<std::string> adminsGroup)
        : StateClient_(std::move(stateClient))
        , StateRoot_(std::move(stateRoot))
        , ControlQueue_(New<TActionQueue>("YqlEngineControl"))
        , ProxyEngineProvider_(New<TProxyYqlEngineProvider>(StateClient_, StateRoot_))
        , AdminsGroup_(std::move(adminsGroup))
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
            ControlQueue_->GetInvoker(),
            AdminsGroup_);
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
    const std::optional<std::string> AdminsGroup_;
    TYqlEngineConfigPtr Config_;

    DECLARE_THREAD_AFFINITY_SLOT(ControlThread);
};

////////////////////////////////////////////////////////////////////////////////

namespace {

struct TYqlProgressPart
{
    std::optional<TYsonString> Value;
    ui32 Revision = 0;
    bool Truncated = false;
};

std::optional<TYqlProgressPart> GetYqlProgressPart(
    TQueryId queryId,
    const IClientPtr& client,
    const TYPath& tablePath,
    TTimestamp timestamp,
    std::optional<ui32> afterProgressRevision,
    const std::vector<std::string>& progressParts)
{
    auto progressPartName = FormatEnum(EProgressPart::YqlProgress);
    auto progressTruncatedPartName = FormatEnum(EProgressPart::YqlProgressTruncated);
    bool allPartsRequested = progressParts.empty();
    bool includeProgress = allPartsRequested || std::find(progressParts.begin(), progressParts.end(), progressPartName) != progressParts.end();
    bool includeProgressTruncated = allPartsRequested || std::find(progressParts.begin(), progressParts.end(), progressTruncatedPartName) != progressParts.end();

    NQueryClient::TQueryBuilder builder;
    builder.SetSource(tablePath);
    builder.AddSelectExpression("[part_name]");
    builder.AddSelectExpression("[subpart_name]");
    builder.AddSelectExpression("[part_value]");
    builder.AddSelectExpression("[revision]");
    builder.AddWhereConjunct("[query_id] = {QueryId}");
    builder.AddWhereConjunct("[part_name] IN {ProgressParts}");

    auto placeholdersFluentMap = BuildYsonNodeFluently().BeginMap();
    placeholdersFluentMap.Item("QueryId").Value(queryId);
    std::vector<std::string> selectedParts;
    if (includeProgress) {
        selectedParts.push_back(progressPartName);
    }
    if (includeProgressTruncated) {
        selectedParts.push_back(progressTruncatedPartName);
    }
    placeholdersFluentMap.Item("ProgressParts").Value(selectedParts);

    if (afterProgressRevision && *afterProgressRevision > 0) {
        builder.AddWhereConjunct("[revision] > {AfterProgressRevision}");
        placeholdersFluentMap.Item("AfterProgressRevision").Value(*afterProgressRevision);
    }

    TSelectRowsOptions options;
    options.Timestamp = timestamp;
    options.PlaceholderValues = ConvertToYsonString(placeholdersFluentMap.EndMap());
    auto selectResult = WaitFor(client->SelectRows(builder.Build(), options))
        .ValueOrThrow();
    auto parts = NTableClient::ToRecords<NRecords::TQueryProgressPartial>(selectResult.Rowset);
    if (parts.empty()) {
        return std::nullopt;
    }

    ui32 progressRevision = 0;
    for (const auto& part : parts) {
        progressRevision = std::max(progressRevision, *part.Revision);
    }

    bool truncated = false;
    bool hasProgress = false;
    auto progress = BuildYsonStringFluently()
        .BeginMap()
            .DoFor(parts, [&] (TFluentMap fluent, const NRecords::TQueryProgressPartial& part) {
                auto value = TYsonString(Decompress(*part.PartValue));
                if (part.Key.PartName == progressTruncatedPartName) {
                    truncated = ConvertTo<bool>(ConvertToNode(value));
                } else if (includeProgress) {
                    hasProgress = true;
                    fluent.Item(part.Key.SubPartName).Value(value);
                }
            })
        .EndMap();
    return TYqlProgressPart{
        .Value = hasProgress ? std::make_optional(std::move(progress)) : std::nullopt,
        .Revision = progressRevision,
        .Truncated = truncated,
    };
}

std::vector<NRecords::TQueryProgressPartial> GetYqlMainParts(
    TQueryId queryId,
    const IClientPtr& client,
    const TYPath& tablePath,
    TTimestamp timestamp,
    std::optional<ui32> afterMainRevision,
    const std::vector<std::string>& progressParts)
{
    NQueryClient::TQueryBuilder builder;
    builder.SetSource(tablePath);
    builder.AddSelectExpression("[part_name]");
    builder.AddSelectExpression("[part_value]");
    builder.AddSelectExpression("[revision]");
    builder.AddWhereConjunct("[query_id] = {QueryId}");
    builder.AddWhereConjunct("[part_name] != {YqlProgressPart}");
    builder.AddWhereConjunct("[part_name] != {ProgressTruncatedPart}");

    auto placeholdersFluentMap = BuildYsonNodeFluently().BeginMap();
    placeholdersFluentMap.Item("QueryId").Value(queryId);
    placeholdersFluentMap.Item("YqlProgressPart").Value(FormatEnum(EProgressPart::YqlProgress));
    placeholdersFluentMap.Item("ProgressTruncatedPart").Value(FormatEnum(EProgressPart::YqlProgressTruncated));

    if (afterMainRevision && *afterMainRevision > 0) {
        builder.AddWhereConjunct("[revision] > {AfterMainRevision}");
        placeholdersFluentMap.Item("AfterMainRevision").Value(*afterMainRevision);
    }

    if (!progressParts.empty()) {
        builder.AddWhereConjunct("[part_name] IN {ProgressParts}");
        placeholdersFluentMap.Item("ProgressParts").Value(progressParts);
    }

    TSelectRowsOptions options;
    options.Timestamp = timestamp;
    options.PlaceholderValues = ConvertToYsonString(placeholdersFluentMap.EndMap());
    auto selectResult = WaitFor(client->SelectRows(builder.Build(), options))
        .ValueOrThrow();
    return NTableClient::ToRecords<NRecords::TQueryProgressPartial>(selectResult.Rowset);
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

template <bool IsActive>
TYsonString GetYqlProgressFromParts(
    TQueryId queryId,
    const IClientPtr& client,
    const NYPath::TYPath& root,
    TTimestamp timestamp,
    const IMapNodePtr& progressOptions)
{
    std::optional<ui32> afterMainRevision;
    std::optional<ui32> afterProgressRevision;
    std::vector<std::string> progressParts;
    if (progressOptions) {
        afterMainRevision = progressOptions->FindChildValue<ui32>("after_main_revision");
        afterProgressRevision = progressOptions->FindChildValue<ui32>("after_progress_revision");
        progressParts = progressOptions->GetChildValueOrDefault<std::vector<std::string>>("progress_parts", {});
    }
    auto tablePath = root + (IsActive ? "/active_query_progresses" : "/finished_query_progresses");

    auto progressPartName = FormatEnum(EProgressPart::YqlProgress);
    bool allPartsRequested = progressParts.empty();
    auto progressTruncatedPartName = FormatEnum(EProgressPart::YqlProgressTruncated);
    bool progressRequested = allPartsRequested || std::find(progressParts.begin(), progressParts.end(), progressPartName) != progressParts.end();
    bool progressTruncatedRequested = allPartsRequested || std::find(progressParts.begin(), progressParts.end(), progressTruncatedPartName) != progressParts.end();

    std::optional<TYsonString> progress;
    std::optional<ui32> progressRevision;
    bool progressTruncated = false;
    if (progressRequested || progressTruncatedRequested) {
        auto progressPart = GetYqlProgressPart(
            queryId,
            client,
            tablePath,
            timestamp,
            afterProgressRevision,
            progressParts);
        if (progressPart) {
            if (progressRequested) {
                progress = std::move(progressPart->Value);
            }
            progressRevision = progressPart->Revision;
            progressTruncated = progressPart->Truncated;
        }
        std::erase(progressParts, progressPartName);
        std::erase(progressParts, progressTruncatedPartName);
    }

    std::vector<NRecords::TQueryProgressPartial> mainRecords;
    if (allPartsRequested || !progressParts.empty()) {
        mainRecords = GetYqlMainParts(
            queryId,
            client,
            tablePath,
            timestamp,
            afterMainRevision,
            progressParts);
    }

    std::optional<ui32> mainRevision;
    for (const auto& part : mainRecords) {
        mainRevision = std::max(mainRevision.value_or(0), *part.Revision);
    }

    auto writePart = [] (TFluentMap fluent, const NRecords::TQueryProgressPartial& part) {
        auto value = TYsonString(Decompress(*part.PartValue));
        fluent.Item(part.Key.PartName).Value(value);
    };
    std::optional<bool> progressTruncatedValue = progressTruncatedRequested && progressTruncated ? std::make_optional(true) : std::nullopt;
    return BuildYsonStringFluently()
        .BeginMap()
            .OptionalItem(progressPartName, progress)
            .OptionalItem("progress_revision", (progressRequested || progressTruncatedRequested) ? progressRevision : std::nullopt)
            .OptionalItem("main_revision", mainRevision)
            .OptionalItem(progressTruncatedPartName, progressTruncatedValue)
            .DoFor(mainRecords, writePart)
        .EndMap();
}

template TYsonString GetYqlProgressFromParts<true>(
    TQueryId queryId,
    const IClientPtr& client,
    const NYPath::TYPath& root,
    TTimestamp timestamp,
    const IMapNodePtr& progressOptions);

template TYsonString GetYqlProgressFromParts<false>(
    TQueryId queryId,
    const IClientPtr& client,
    const NYPath::TYPath& root,
    TTimestamp timestamp,
    const IMapNodePtr& progressOptions);

////////////////////////////////////////////////////////////////////////////////

IQueryEnginePtr CreateYqlEngine(
    const IClientPtr& stateClient,
    const TYPath& stateRoot,
    std::optional<std::string> adminsGroup)
{
    return New<TYqlEngine>(stateClient, stateRoot, std::move(adminsGroup));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryTracker
