#include <yql/tools/yqlworker/executor/common/credentials.h>
#include <yql/tools/yqlworker/executor/common/runnable_tasks.h>
#include <yql/tools/yqlworker/executor/common/tasks/provider_load/interface.h>
#include <yql/tools/yqlworker/executor/common/sql_library_meta_task.h>
#include <yql/tools/yqlworker/executor/common/task_progress_merger.h>
#include <yql/tools/yqlworker/executor/common/udf_library_meta_task.h>
#include <yql/tools/yqlworker/executor/common/udf_snapshot_handler_task.h>
#include <yql/tools/yqlworker/executor/task_executor.h>

#include <yt/yql/providers/yt/gateway/native/yql_yt_native.h>
#include <yt/yql/providers/yt/gateway/profile/yql_yt_profiling.h>
#include <yt/yql/providers/yt/gateway/mixed/yql_yt_mixed.h>
#include <yt/yql/providers/yt/provider/yql_yt_provider.h>
#include <yt/yql/providers/yt/comp_nodes/dq/dq_yt_factory.h>
#include <yt/yql/providers/yt/lib/log/yt_logger.h>
#include <yt/yql/providers/yt/lib/yt_download/yt_download.h>
#include <yt/yql/providers/yt/lib/yt_url_lister/yt_url_lister.h>
#include <yt/yql/providers/yt/lib/config_clusters/config_clusters.h>
#include <yt/yql/providers/yt/lib/access_provider/full/yt_access_provider.h>
#include <yt/yql/providers/yt/lib/tvm_client/full/tvm_client.h>

#include <yt/yql/library/token_resolver/yt_token_resolver.h>

#include <yql/essentials/public/issue/yql_issue_message.h>
#include <yql/essentials/public/issue/yql_issue_utils.h>
#include <yql/essentials/public/langver/yql_langver.h>
#include <yql/essentials/providers/pg/provider/yql_pg_provider.h>
#include <yql/essentials/providers/common/provider/yql_provider_names.h>
#include <yql/essentials/providers/common/provider/yql_provider.h>
#include <yql/essentials/providers/common/udf_resolve/yql_simple_udf_resolver.h>
#include <yql/essentials/providers/common/udf_resolve/yql_outproc_udf_resolver.h>
#include <yql/essentials/providers/common/comp_nodes/yql_factory.h>
#include <yql/essentials/providers/common/activation/yql_activation.h>
#include <yql/essentials/providers/common/gateways_utils/gateways_utils.h>
#include <yql/essentials/minikql/invoke_builtins/mkql_builtins.h>
#include <yql/essentials/minikql/comp_nodes/mkql_factories.h>
#include <yql/essentials/core/yql_library_compiler.h>
#include <yql/essentials/core/facade/yql_facade.h>
#include <yql/essentials/core/langver/feature.gen.h>
#include <yql/essentials/core/layers/remote_layer_provider.h>
#include <yql/essentials/core/services/yql_plan.h>
#include <yql/essentials/core/services/mounts/yql_mounts.h>
#include <yql/essentials/core/file_storage/file_storage.h>
#include <yql/essentials/core/file_storage/http_download/http_download.h>
#include <yql/essentials/core/url_lister/url_lister_manager.h>
#include <yql/essentials/core/url_preprocessing/url_preprocessing.h>
#include <yql/essentials/core/qplayer/storage/file/yql_qstorage_file.h>
#include <yql/essentials/utils/backtrace/backtrace.h>
#include <yql/essentials/utils/log/log.h>
#include <yql/essentials/utils/log/context.h>
#include <yql/essentials/utils/log/profile.h>
#include <yql/essentials/utils/debug_info.h>
#include <yql/essentials/utils/signals/utils.h>
#include <yql/essentials/utils/yql_panic.h>
#include <yql/essentials/sql/v1/translation/sql.h>
#include <yql/essentials/sql/v1/lexer/antlr4/lexer.h>
#include <yql/essentials/sql/v1/lexer/antlr4_ansi/lexer.h>
#include <yql/essentials/sql/v1/proto_parser/antlr4/proto_parser.h>
#include <yql/essentials/sql/v1/proto_parser/antlr4_ansi/proto_parser.h>
#include <yql/essentials/parser/pg_wrapper/interface/comp_factory.h>
#include <yql/essentials/protos/pg_ext.pb.h>
#include <yql/essentials/core/pg_ext/yql_pg_ext.h>
#include <yql/essentials/parser/pg_wrapper/interface/parser.h>

#include <contrib/ydb/library/actors/core/log_iface.h>
#include <contrib/ydb/library/yql/dq/comp_nodes/yql_common_dq_factory.h>
#include <contrib/ydb/library/yql/dq/opt/dq_opt_join_cbo_factory.h>
#include <contrib/ydb/library/yql/providers/dq/common/yql_dq_settings.h>
#include <contrib/ydb/library/yql/providers/dq/helper/yql_dq_helper_impl.h>
#include <contrib/ydb/library/yql/providers/dq/provider/exec/yql_dq_exectransformer.h>
#include <contrib/ydb/library/yql/providers/dq/provider/yql_dq_gateway.h>
#include <contrib/ydb/library/yql/providers/dq/provider/yql_dq_provider.h>
#include <contrib/ydb/library/yql/utils/actor_system/manager.h>

#include <yt/yql/providers/dq/clique/yql_dq_clique_routing_gateway.h>
#include <yt/yql/providers/dq/common/yql_dq_clique.h>
#include <yt/yql/providers/dq/control/yql_dq_control.h>
#include <yt/yql/providers/dq/gateway/yql_dq_gateway_factory.h>
#include <yt/yql/providers/dq/global_worker_manager/coordination_helper.h>
#include <yt/yql/providers/dq/global_worker_manager/dq_gateway_with_uploader.h>
#include <yt/yql/providers/dq/service/interconnect_helpers.h>

#include <yt/cpp/mapreduce/interface/config.h>

#include <library/cpp/resource/resource.h>
#include <library/cpp/yson/node/node_io.h>
#include <library/cpp/protobuf/util/pb_io.h>

#include <util/datetime/cputimer.h>
#include <util/generic/algorithm.h>
#include <util/generic/strbuf.h>
#include <util/generic/maybe.h>
#include <util/generic/utility.h>
#include <util/generic/hash.h>
#include <util/generic/hash_set.h>
#include <util/random/random.h>
#include <util/string/strip.h>
#include <util/string/join.h>
#include <util/system/env.h>
#include <util/system/fs.h>
#include <util/system/hostname.h>
#include <util/system/rusage.h>
#include <util/stream/file.h>
#include <util/stream/format.h>
#include <util/folder/dirut.h>
#include <util/system/getpid.h>

#include <atomic>
#include <thread>

using namespace NKikimr;
using namespace NMiniKQL;

namespace NYql {

static const ui32 MAX_ISSUE_LEVELS = 15;
static const ui32 ISSUE_TAIL_LEVELS = 3;

const TString GeneralInfoComponent = "GeneralInfo";
const TString UsernameLabel = "Username";
const TString UserIsRobotLabel = "UserIsRobot";
const TString ExcludeFromQPlayerLabel = "ExcludeFromQPlayer";
const TString EndTimeLabel = "EndTime";
const TString IsSuccessLabel = "IsSuccess";

void AddErrorMessageToTaskResult(NProto::TTaskResult* taskResult, const TString& str)
{
    taskResult->AddIssues()->CopyFrom(IssueToMessage(TIssue(TPosition(), str)));
    taskResult->AddErrors()->SetMessage(str);
}

void AddFatalMessageToTaskResult(NProto::TTaskResult* taskResult, const TString& str)
{
    TIssue issue(TPosition(), str);
    SetIssueCode(EYqlIssueCode::TIssuesIds_EIssueCode_UNEXPECTED, issue);
    taskResult->AddIssues()->CopyFrom(IssueToMessage(issue));
    taskResult->AddErrors()->SetMessage(str);
}

namespace {

const size_t MAX_RESULT_LIMIT = 15 << 20; // 15Mb. MongoDB has 16Mb limit for one document
constexpr TStringBuf AST_TOO_LARGE =
    "(\n"
    "# Ast too large\n"
    "    (return world)\n"
    ")";

template <typename F>
void FitResultStringToLimit(TMaybe<TString>& s, F f) {
    if (!s) {
        return;
    }

    if (s->size() > MAX_RESULT_LIMIT) {
        s = f();
    }
}

TString ComposeTooBigPlan() {
    TString result;
    TStringOutput out(result);
    NYson::TYsonWriter w(&out);

    w.OnBeginMap();
    w.OnKeyedItem("Basic");
    {
        w.OnBeginMap();
        w.OnKeyedItem("nodes");
        {
            w.OnBeginList();
            {
                w.OnBeginMap();
                w.OnKeyedItem("id");
                w.OnUint64Scalar(1);

                w.OnKeyedItem("level");
                w.OnUint64Scalar(1);

                w.OnKeyedItem("name");
                w.OnStringScalar("TooBigPlan!");

                w.OnKeyedItem("type");
                w.OnStringScalar("op");

                w.OnEndMap();
            }
            w.OnEndList();
        }

        w.OnKeyedItem("links");
        {
            w.OnBeginList();
            w.OnEndList();
        }
        w.OnEndMap();
    }
    w.OnEndMap();
    return result;
}

void FitAstAndPlanToLimits(TMaybe<TString>& ast, TMaybe<TString>& plan) {
    auto astTooBig = []() { return TString(AST_TOO_LARGE); };

    FitResultStringToLimit(ast, astTooBig);
    FitResultStringToLimit(plan, ComposeTooBigPlan);

    if (!ast || !plan) {
        return;
    }

    if (ast->size() + plan->size() > MAX_RESULT_LIMIT) {
        // let's forget AST and preserve plan in this case
        // user can observe progress
        ast = astTooBig();
    }
}

using TClustersMap = THashMap<TString, TString>;

IFunctionRegistry::TPtr CreateFuncRegistry(const TVector<TString>& udfsPaths) {
    return udfsPaths.empty() ? CreateFunctionRegistry(CreateBuiltinRegistry()) : CreateFunctionRegistry(&NYql::NBacktrace::KikimrBackTrace, CreateBuiltinRegistry(), false, udfsPaths);
}

bool IsRobot(const NProto::TTaskData& taskData) {
    return taskData.HasUserIsRobot()
        ? taskData.GetUserIsRobot()
        : taskData.HasUsername() && (taskData.GetUsername().StartsWith("robot-") || taskData.GetUsername().StartsWith("zomb-"));
}

NYson::EYsonFormat ToYsonFormat(NYql::NProto::EDataFormat format) {
    using NYql::NProto::EDataFormat;

    switch (format) {
    case EDataFormat::YSON_BINARY: return NYson::EYsonFormat::Binary;
    case EDataFormat::YSON_TEXT: return NYson::EYsonFormat::Text;
    case EDataFormat::YSON_PRETTY: return NYson::EYsonFormat::Pretty;
    default:
        ythrow yexception() << "unknown data format " << static_cast<int>(format);
    }
}

template <typename TIterator>
TUserDataTable ToUserDataTable(const TCredentials& creds, TIterator begin, TIterator end) {
    TUserDataTable table;
    for (TIterator it = begin; it != end; ++it) {
        const NProto::TTaskFile& taskFile = *it;

        TUserDataBlock& block = table[TUserDataKey::File(
                    NYql::GetDefaultFilePrefix() + taskFile.GetName())];
        block.Data = taskFile.GetContent();

        switch (taskFile.GetType()) {
        case NProto::TTaskFile::CONTENT:
            block.Type = NYql::EUserDataType::RAW_INLINE_DATA;
            break;

        case NProto::TTaskFile::URL:
            block.Type = NYql::EUserDataType::URL;
            if (auto tokeName = taskFile.GetTokenName()) {
                auto token = creds.FindCredential(tokeName);
                if (!token) {
                    ythrow yexception() << "unknown token name: " << taskFile.GetTokenName();
                }
                block.UrlToken = token->Content;
            }
            break;

        default:
            ythrow yexception() << "unsupported file type: "
                    << NProto::TTaskFile::EType_Name(taskFile.GetType());
        }
    }
    return table;
}

TTaskResultChanges::TMask FillErrors(const TIssues& issues, NProto::TTaskResult* taskResult, NProto::TTaskResult* taskResultDelta) {
    if (issues.Empty()) {
        return 0;
    }

    for (const TIssue& topIssue : issues) {
        const auto truncatedIssue = TruncateIssueLevels(topIssue, TTruncateIssueOpts().SetMaxLevels(MAX_ISSUE_LEVELS).SetKeepTailLevels(ISSUE_TAIL_LEVELS));
        const auto message = IssueToMessage(truncatedIssue);
        taskResult->AddIssues()->CopyFrom(message);
        taskResultDelta->AddIssues()->CopyFrom(message);
        WalkThroughIssues(truncatedIssue, false, [&taskResult, &taskResultDelta](const TIssue& issue, ui16 /*level*/) {
            {
                NProto::TTaskError* taskError = taskResult->AddErrors();
                taskError->SetRow(issue.Position.Row);
                taskError->SetColumn(issue.Position.Column);
                taskError->SetMessage(issue.GetMessage());
            }
            {
                NProto::TTaskError* taskError = taskResultDelta->AddErrors();
                taskError->SetRow(issue.Position.Row);
                taskError->SetColumn(issue.Position.Column);
                taskError->SetMessage(issue.GetMessage());
            }
        });
    }
    return TTaskResultChanges::ERRORS;
}

//////////////////////////////////////////////////////////////////////////////
// Exceptions
//////////////////////////////////////////////////////////////////////////////
class TTaskProcessingException: public yexception {
};

class TTaskAborted: public yexception {
};

//////////////////////////////////////////////////////////////////////////////
// TTaskPipelineConfigurator
//////////////////////////////////////////////////////////////////////////////
class TTaskPipelineConfigurator final: public IPipelineConfigurator
{
public:
    TTaskPipelineConfigurator(
            NProto::TTaskResult& taskResult,
            TProgram& program,
            ITaskWatcher* watcher)
        : TaskResult_(taskResult)
        , Program_(program)
        , Watcher_(watcher)
    {
    }

    void AfterCreate(TTransformationPipeline* pipeline) const override {
        Y_UNUSED(pipeline);
    }

    void AfterTypeAnnotation(TTransformationPipeline* pipeline) const override {
        if (YQL_CLOG_ACTIVE(TRACE, Core)) {
            pipeline->Add(CreateFunctorTransformer(
                [this](TExprNode::TPtr input, TExprNode::TPtr& output, TExprContext& /*ctx*/) {
                    output = input;
                    if (auto ast = Program_.GetQueryAst()) {
                        YQL_CLOG(TRACE, Core) << *ast;
                    }
                    return IGraphTransformer::TStatus::Ok;
                }), "TraceAst");
        }
    }

    void AfterOptimize(TTransformationPipeline* pipeline) const override {
        pipeline->Add(CreateFunctorTransformer(
            [this](TExprNode::TPtr input, TExprNode::TPtr& output, TExprContext& ctx) {
                Y_UNUSED(ctx);
                output = input;

                auto ast = Program_.GetQueryAst();
                auto plan = Program_.GetQueryPlan();

                FitAstAndPlanToLimits(ast, plan);

                TTaskResultChanges::TMask changesMask = 0;
                TTaskResultPtr taskResultChanges = std::make_shared<NProto::TTaskResult>();
                // set status to provide current task status to Inspector, but
                // do not add bit to changesMask, cause' status is not changed
                taskResultChanges->SetStatus(TaskResult_.GetStatus());

                if (ast) {
                // TODO (jamel): remove it (temporarily always fill AST)
//                if (!TaskResult_.HasAst() || *ast != TaskResult_.GetAst()) {
                    TaskResult_.SetAst(*ast);
                    taskResultChanges->SetAst(*ast);
                    changesMask |= TTaskResultChanges::AST;
//                }
                }

                if (plan) {
                  // TODO (jamel): remove it (temporarily always fill PLAN)
//                if (!TaskResult_.HasPlan() || *plan != TaskResult_.GetPlan()) {
                    TaskResult_.SetPlan(*plan);
                    taskResultChanges->SetPlan(*plan);
                    changesMask |= TTaskResultChanges::PLAN;
//                }
                }

                changesMask |= FillErrors(Program_.CompletedIssues(), &TaskResult_, taskResultChanges.get());

                if (changesMask != 0) {
                    TaskResult_.SetRevision(TaskResult_.GetRevision() + 1);
                    taskResultChanges->SetRevision(TaskResult_.GetRevision());
                    changesMask |= TTaskResultChanges::REVISION;
                    Watcher_->OnResultChange(changesMask, taskResultChanges);
                }

                return IGraphTransformer::TStatus::Ok;
            }), "AstAndPlanOutput");
    }

private:
    NProto::TTaskResult& TaskResult_;
    TProgram& Program_;
    ITaskWatcher* Watcher_;
};

using NSQLTranslation::ISqlFeaturePolicy;

class TRandomAllowPolicy: public ISqlFeaturePolicy {
public:
    TRandomAllowPolicy(ui32 defaultPercent, THashMap<ui32, ui32>&& percentByHour)
        : DefaultPercent(defaultPercent)
        , PercentByHour(std::move(percentByHour))
    {
    }

    bool Allow() const override {
        if (Cached) {
            return *Cached;
        }

        auto percent = DefaultPercent;
        if (PercentByHour) {
            auto percentByHour = PercentByHour.FindPtr(GetCurrentHour());
            if (percentByHour) {
                percent = *percentByHour;
            }
        }


        Cached = RandomNumber<ui8>(100) < percent;
        return *Cached;
    }

private:
    ui32 GetCurrentHour() const {
        auto now = TInstant::Now();
        struct tm local = {};
        now.LocalTime(&local);
        return ui32(local.tm_hour);
    }

    const ui32 DefaultPercent;
    const THashMap<ui32, ui32> PercentByHour;
    mutable TMaybe<bool> Cached;
};

class TProgramWithHidden: private TNonCopyable
{
private:
    struct THiddenResult {
        bool Failed = false;
        ui64 HiddenMs = 0;

        THiddenResult() = default;

        explicit THiddenResult(bool failed, ui64 ms) : Failed(failed), HiddenMs(ms) {};

        static THiddenResult FailedResult() {
            auto result = THiddenResult();
            result.Failed = true;
            return result;
        }
    };

public:
    using TStatus = TProgram::TStatus;

    TProgramWithHidden(
        TProgramFactoryPtr programFactory,
        NLayers::IRemoteLayerProviderPtr baseLayerProvider,
        TTaskDataPtr taskData,
        TMaybe<TOperationProgressWriter> primaryProgressWriter,
        EHiddenMode hiddenMode,
        TMaybe<NSQLTranslation::TTranslationSettings> translationSettings,
        NSQLTranslation::TExtendedSqlFlags activatedSqlFlags,
        TString qPlayerDir,
        TQWriterSettings qWriterSettings,
        bool enableQPlayer,
        bool enableQPlayerForRobot,
        EQPlayerCaptureMode qPlayerCaptureMode,
        bool alwaysSaveQStorage
    )
        : HiddenSessionId(taskData->GetId() + "_hidden") // don't change this suffix (NYql::NNative::TExecContextBase::Hidden depends on it)
        , HiddenMode(hiddenMode)
        , TaskData(std::move(taskData))
        , Programs()
        , ActivatedSqlFlags_(std::move(activatedSqlFlags))
        , HiddenThread()
        , EnableQPlayer_(enableQPlayer)
        , EnableQPlayerForRobot_(enableQPlayerForRobot)
        , QPlayerCaptureMode_(qPlayerCaptureMode)
        , QPlayerDir_(qPlayerDir)
        , AlwaysSaveQStorage_(alwaysSaveQStorage)
    {
        Y_ABORT_UNLESS(hiddenMode != EHiddenMode::Auto);
        YQL_LOG(INFO) << "HiddenMode: " << HiddenMode;

        YQL_CLOG(INFO, Core) << "EnableQPlayer: " << enableQPlayer << " QPlayerDir: " << qPlayerDir;
        if (EnableQPlayer_) {
            try {
                auto dir = QPlayerDir_ / "tmp" / TaskData->GetId();
                MakeDirIfNotExist(dir);
                auto pid = GetPID();
                TFileOutput pidFile(dir / "data.pid");
                pidFile << pid;
                pidFile.Flush();
                QStorage_ = MakeFileQStorage(dir, TFileQStorageSettings{.BufferUntilCommit = false, .AlwaysFlushIndex = true});
                QContext_ = TQContext(QStorage_->MakeWriter("data", qWriterSettings), QPlayerCaptureMode_);
                ExtAddRemoteLayersProvider(programFactory, baseLayerProvider, &QContext_);
            } catch (...) {
                YQL_CLOG(ERROR, Core) << "QPlayer exception: " << CurrentExceptionMessage() << "\n";
                EnableQPlayer_ = false;
            }
        } else {
            ExtAddRemoteLayersProvider(programFactory, baseLayerProvider);
        }

        for (ui8 i = 0; i < (HiddenMode != EHiddenMode::Disable ? 2 : 1); i++) {
            bool isHidden = i == 1;
            static const TString filename("-memory-");
            auto program = programFactory->Create(
                filename,
                TaskData->GetProgram(),
                isHidden ? HiddenSessionId : TaskData->GetId(),
                isHidden ? HiddenMode : EHiddenMode::Disable,
                isHidden || !EnableQPlayer_ ? TQContext{} : QContext_);
            program->ConfigureYsonResultFormat(
                        ToYsonFormat(TaskData->GetResultFormat()));
            if (!GetEnv("YQL_DETERMINISTIC_MODE")) {
                program->EnableResultPosition();
            }

            if (TaskData->HasDisableNativeUdfSupport()) {
                YQL_LOG(INFO) << "DisableNativeUdfSupport: " << TaskData->GetDisableNativeUdfSupport();
                program->SetDisableNativeUdfSupport(TaskData->GetDisableNativeUdfSupport());
            }

            if (TaskData->HasStrictConfigValidation()) {
                YQL_LOG(INFO) << "StrictConfigValidation: " << TaskData->GetStrictConfigValidation();
                program->SetStrictConfigValidation(TaskData->GetStrictConfigValidation());
            }

            if (TaskData->HasUsername()) {
                program->SetAuthenticatedUser(TaskData->GetUsername());
            }
            if (TaskData->GetPersistedId()) {
                program->SetOperationId(TaskData->GetId());
                program->SetSharedOperationId(TaskData->GetSharedId());
            }
            if (TaskData->HasQueryName() && !TaskData->GetQueryName().empty()) {
                program->SetQueryName(TaskData->GetQueryName());
            }
            if (TaskData->HasTitle() && !TaskData->GetTitle().empty()) {
                program->SetOperationTitle(TaskData->GetTitle());
            }
            if (TaskData->HasUrl() && !TaskData->GetUrl().empty()) {
                program->SetOperationUrl(TaskData->GetUrl());
            }
            if (TaskData->HasAttributes() && !TaskData->GetAttributes().empty()) {
                program->SetOperationAttrsYson(TaskData->GetAttributes());
            }
            if (TaskData->HasParameters() && !TaskData->GetParameters().empty()) {
                program->SetParametersYson(TaskData->GetParameters());
            }
            if (TaskData->HasProject() && TaskData->GetProject().HasAbcServiceSlug()) {
                program->SetProjectSlug(TaskData->GetProject().GetAbcServiceSlug());
            }
            program->SetEnableLineage();
            with_lock(ProgramsMutex) {
                Programs.push_back(program);
            }
        }
        if (primaryProgressWriter) {
            GetPrimary()->SetProgressWriter(*primaryProgressWriter);
        } else {
            YQL_LOG(INFO) << "will not watch task progress";
        }
        if (HiddenMode != EHiddenMode::Disable) {
            GetPrimary()->SetAbortHidden([hidden = GetHidden(), hiddenAborted = &HiddenAborted, hiddenSessionId = HiddenSessionId](){
                YQL_LOG_CTX_SCOPE(hiddenSessionId);
                if (*hiddenAborted) {
                    YQL_LOG(DEBUG) <<  "attempt to abort hidden twice";
                    return;
                }
                try {
                    hidden->Abort().GetValueSync();
                } catch (...) {
                    YQL_LOG(ERROR) << CurrentExceptionMessage();
                }

                *hiddenAborted = true;
                YQL_LOG(DEBUG) << "hidden aborted";
            });
            auto hiddenPromise = NThreading::NewPromise<THiddenResult>();
            HiddenResult = hiddenPromise.GetFuture();
            HiddenThread = std::thread(&TProgramWithHidden::RunHidden, this, std::move(hiddenPromise), std::move(translationSettings));
        }
    }

    void CommitQPlayer(TStatus status) {
        if (!EnableQPlayer_) {
            return;
        }

        GetPrimary()->CommitFullCapture();

        YQL_ENSURE(QContext_.CanWrite());
        const bool isRobot = IsRobot(*TaskData);
        if (TaskData->HasUsername()) {
            QContext_.GetWriter()->Put({GeneralInfoComponent, UsernameLabel}, TaskData->GetUsername()).GetValueSync();
        }
        QContext_.GetWriter()->Put({GeneralInfoComponent, UserIsRobotLabel}, isRobot ? "1" : "0").GetValueSync();
        bool excludeFromQPlayer = status == TProgram::TStatus::Ok && isRobot && !EnableQPlayerForRobot_;
        QContext_.GetWriter()->Put({GeneralInfoComponent, ExcludeFromQPlayerLabel}, excludeFromQPlayer ? "1" : "0").GetValueSync();
        if (excludeFromQPlayer && !AlwaysSaveQStorage_) {
            QContext_.GetWriter()->Close();
            try {
                NFs::RemoveRecursive(QPlayerDir_ / "tmp" / TaskData->GetId());
            } catch (...) {
                YQL_CLOG(ERROR, Core) << "Remove directory failed: " << CurrentExceptionMessage();
            }
            return;
        }
        if (status != TProgram::TStatus::Ok && status != TProgram::TStatus::Error) {
            return;
        }
        try {
            bool success = status == TProgram::TStatus::Ok;
            QContext_.GetWriter()->Put({GeneralInfoComponent, IsSuccessLabel}, success ? "1" : "0").GetValueSync();
            QContext_.GetWriter()->Put({GeneralInfoComponent, EndTimeLabel}, TInstant::Now().ToStringLocalUpToSeconds()).GetValueSync();
            QContext_.GetWriter()->Commit().GetValueSync();
            TString id = TaskData->GetId();
            NFs::Rename(QPlayerDir_ / "tmp" / id,
                                    QPlayerDir_ / (success ? "success" : "fail") / id);
            QueryHasReplay_ = !excludeFromQPlayer;
        } catch (...) {
            YQL_CLOG(ERROR, Core) << "QPlayer commit failed: " << CurrentExceptionMessage();
        }
    }

    TStatus Run(const TString& username, const IPipelineConfigurator& primaryPipelineConfigurator) {
        auto startedAt = ::MilliSeconds();
        const auto status = GetPrimary()->RunWithConfig(username, primaryPipelineConfigurator);
        PrimaryMs = ::MilliSeconds() - startedAt;
        if (HiddenMode == EHiddenMode::Debug) {
            WaitHiddenFinished();
        }
        return status;
    }

    void Abort() {
        YQL_LOG_CTX_SCOPE(HiddenSessionId);
        try {
            if (auto primary = GetPrimary(); primary) {
                primary->Abort().GetValueSync();
            }
        } catch (...) {
            YQL_LOG(ERROR) << CurrentExceptionMessage();
        }
        if (HiddenMode != EHiddenMode::Disable) {
            try {
                if (auto hidden = GetHidden(); hidden) {
                    hidden->Abort().GetValueSync();
                }
            } catch (...) {
                YQL_LOG(ERROR) << CurrentExceptionMessage();
            }
            YQL_LOG(DEBUG) << "hidden aborted";
        }
    }

    TString GetQPlayerStats() {
        TStringStream out;
        NYson::TYsonWriter writer(&out);
        writer.OnBeginMap();

        writer.OnKeyedItem("QueryHasReplay");
            writer.OnBeginMap();
                writer.OnKeyedItem("count");
                writer.OnUint64Scalar(QueryHasReplay_);
            writer.OnEndMap();

        if (EnableQPlayer_ && QPlayerCaptureMode_ == EQPlayerCaptureMode::Full) {
             writer.OnKeyedItem("QueryFullCaptureStarted");
                writer.OnBeginMap();
                    writer.OnKeyedItem("count");
                    writer.OnUint64Scalar(true);
                writer.OnEndMap();
        }
        if (GetPrimary()->IsFullCaptureReady()) {
            writer.OnKeyedItem("QueryHasFullCapture");
                writer.OnBeginMap();
                    writer.OnKeyedItem("count");
                    writer.OnUint64Scalar(QueryHasReplay_);
                writer.OnEndMap();
        }

        writer.OnEndMap();
        return out.Str();
    }

    TMaybe<TString> GetStatistics(bool totalOnly) {
        auto qplayerEntry = GetQPlayerStats();
        if (HiddenMode == EHiddenMode::Disable) {
            return GetPrimary()->GetStatistics(totalOnly, {{"qplayer", qplayerEntry}});
        }
        auto hiddenResult = THiddenResult();
        if (HiddenResult.HasValue()) {
            hiddenResult = HiddenResult.GetValue();
        } else {
            HiddenAborted = true;
        }
        TStringStream out;
        NYson::TYsonWriter writer(&out);
        writer.OnBeginMap();
        writer.OnKeyedItem("failed");
            writer.OnBeginMap();
                writer.OnKeyedItem("count");
                writer.OnUint64Scalar(hiddenResult.Failed && !HiddenAborted);  // aborting query always leads to errors like "transaction does not exist", so we ignore this
            writer.OnEndMap();
        writer.OnKeyedItem("aborted");
            writer.OnBeginMap();
                writer.OnKeyedItem("count");
                writer.OnUint64Scalar(HiddenAborted);
            writer.OnEndMap();
        writer.OnKeyedItem("hidden_ms");
            writer.OnBeginMap();
                writer.OnKeyedItem("count");
                writer.OnUint64Scalar(hiddenResult.HiddenMs);
            writer.OnEndMap();
        writer.OnKeyedItem("primary_ms");
            writer.OnBeginMap();
                writer.OnKeyedItem("count");
                writer.OnUint64Scalar(PrimaryMs);
            writer.OnEndMap();
        writer.OnEndMap();

        TStringStream activatedSqlFlags;
        {
            TOperationStatistics statistics =
                GetSqlFlagsActivationStatistics();

            NYson::TYsonWriter writer(&activatedSqlFlags);
            THashMap<ui32, TOperationStatistics> tmp;
            tmp.emplace(Max<ui32>(), statistics);
            NCommon::WriteStatistics(writer, totalOnly, tmp);
        }

        return GetPrimary()->GetStatistics(totalOnly, {
            {"sql", activatedSqlFlags.Str()},
            {"hidden", out.Str()},
            {"qplayer", qplayerEntry},
        });
    }

    TOperationStatistics GetSqlFlagsActivationStatistics() {
        TOperationStatistics statistics;
        for (const auto& [flag, _] : ActivatedSqlFlags_) {
            TString name = TStringBuilder() << "Activation:" << flag;
            statistics.Entries.emplace_back(std::move(name), 0, 0, 0, 0, 1);
        }
        return statistics;
    }

    TProgramPtr GetPrimary() {
        with_lock(ProgramsMutex) {
            return Programs.size() >= 1 ? Programs[0] : nullptr;
        }
    }

    ~TProgramWithHidden() {
        WaitHiddenFinished();
    }

    void WaitHiddenFinished() {
        try {
            YQL_LOG_CTX_SCOPE(HiddenSessionId);
            if (HiddenThread.joinable()) {
                HiddenThread.join();
                YQL_LOG(DEBUG) << "hidden thread finished";
            }
        } catch (...) {
            YQL_LOG(ERROR) << "Exception on hidden thread join: " << CurrentExceptionMessage();
        }
    }

private:
    TProgramPtr GetHidden() {
        with_lock (ProgramsMutex) {
            return Programs.size() >= 2 ? Programs[1] : nullptr;
        }
    }

    void RunHidden(NThreading::TPromise<THiddenResult>&& result, TMaybe<NSQLTranslation::TTranslationSettings>&& translationSettings) {
        YQL_LOG_CTX_SCOPE(HiddenSessionId);
        try {
            {
                // parse
                switch (TaskData->GetSyntax()) {
                case NProto::ESyntax::YQL: {
                    if (!GetHidden()->ParseYql()) {
                        throw TTaskProcessingException() << "ParseYql";
                    }
                    break;
                }
                case NProto::ESyntax::PG:
                case NProto::ESyntax::SQL:
                case NProto::ESyntax::SQLv1:
                case NProto::ESyntax::SPARK_SQL: {
                    if (!GetHidden()->ParseSql(*translationSettings)) {
                        throw TTaskProcessingException() << "ParseSql";
                    }
                    break;
                }
                default:
                    throw TTaskProcessingException()
                            << "Unknown program syntax: "
                            << NProto::ESyntax_Name(TaskData->GetSyntax());
                }
            }

            {
                // compile
                if (!GetHidden()->Compile(TaskData->GetUsername())) {
                    throw TTaskProcessingException() << "Compile";
                }
            }

            {
                // run
                class TDummyTaskWatcher : public ITaskWatcher {
                public:
                    void OnStart() override {}
                    void OnFinish(NProto::ETaskStatus) override {}
                    void OnResultChange(TTaskResultChanges::TMask, TTaskResultPtr, int, NBus::TOnMessageContext*) override {}
                    bool WillWatch(ui32) const override { return false; }
                };
                NProto::TTaskResult hiddenResult;
                TDummyTaskWatcher hiddenWatcher;
                TTaskPipelineConfigurator pipelineConfigurator(hiddenResult, *GetHidden(), &hiddenWatcher);
                auto startedAt = ::MilliSeconds();
                YQL_LOG(INFO) << "Hidden started";
                const auto status = GetHidden()->RunWithConfig(TaskData->GetUsername(), pipelineConfigurator);
                result.SetValue(THiddenResult(status != IGraphTransformer::TStatus::Ok, ::MilliSeconds() - startedAt));
                YQL_LOG(INFO) << "Hidden finished with status: " << status;
                YQL_LOG(INFO) << "Hidden issues: " << GetHidden()->Issues().ToOneLineString();
            }
        } catch (...) {
            YQL_LOG(ERROR) << "Hidden finished with error: " << CurrentExceptionMessage();
            result.SetValue(THiddenResult::FailedResult());
        }
    }

private:
    const TString HiddenSessionId;
    EHiddenMode HiddenMode;
    ui64 PrimaryMs = 0;
    const TTaskDataPtr TaskData;
    TMutex ProgramsMutex;
    TVector<TProgramPtr> Programs;
    NThreading::TFuture<THiddenResult> HiddenResult;
    bool HiddenAborted = false;
    const NSQLTranslation::TExtendedSqlFlags ActivatedSqlFlags_;
    std::thread HiddenThread;
    IQStoragePtr QStorage_;
    TQContext QContext_;
    bool EnableQPlayer_;
    bool EnableQPlayerForRobot_;
    EQPlayerCaptureMode QPlayerCaptureMode_;
    TFsPath QPlayerDir_;
    bool QueryHasReplay_ = false;
    bool AlwaysSaveQStorage_;
};

//////////////////////////////////////////////////////////////////////////////
// TYqlRunnableTaskBase
//////////////////////////////////////////////////////////////////////////////
struct TYqlRunnableTask final: public IYqlRunnableTask {
    TProgramFactoryPtr ProgramFactory;
    const TClustersMap& Clusters;
    const TClustersMap& ClusterPathPrefixes;
    const TMaybe<TString> DefaultTranslationCluster;
    std::optional<TProgramWithHidden> Program;
    NProto::ETaskAction Action;
    TTaskDataPtr TaskData;
    TTaskResultPtr TaskResult;
    ITaskWatcherPtr TaskWatcher;
    IMetricsRegistryPtr GlobalMetrics;
    IMetricsRegistryPtr UserMetrics;
    mutable TTaskProgressMerger TaskProgressMerger;
    NThreading::TPromise<void> ActiveProcessesPromise;
    const TSimpleTimer Timer;
    TGatewaysConfig GatewaysConfig;
    const TGatewaySQLFlags SqlFlags;
    std::unordered_map<TString, std::pair<TCredential, std::vector<TString>>> DefaultCredentials;
    TQWriterSettings QWriterSettings;
    TString QPlayerDir;
    TMaybe<size_t> AstMemoryLimit;

    TYqlRunnableTask(
            TProgramFactoryPtr programFactory,
            const TClustersMap& clusters,
            const TClustersMap& clusterPathPrefixes,
            const TMaybe<TString>& defaultTranslationCluster,
            NProto::ETaskAction action,
            TTaskDataPtr taskData,
            ITaskWatcherPtr taskWatcher,
            IMetricsRegistryPtr globalMetrics,
            IMetricsRegistryPtr userMetrics,
            const TGatewaysConfig& gatewaysConfig,
            TGatewaySQLFlags sqlFlags,
            std::unordered_map<TString, std::pair<TCredential, std::vector<TString>>> defaultCredentials,
            TString qPlayerDir,
            TQWriterSettings qWriterSettings,
            TMaybe<size_t> astMemoryLimit)
        : ProgramFactory(programFactory)
        , Clusters(clusters)
        , ClusterPathPrefixes(clusterPathPrefixes)
        , DefaultTranslationCluster(defaultTranslationCluster)
        , Action(action)
        , TaskData(std::move(taskData))
        , TaskResult(new NProto::TTaskResult)
        , TaskWatcher(std::move(taskWatcher))
        , GlobalMetrics(std::move(globalMetrics))
        , UserMetrics(std::move(userMetrics))
        , ActiveProcessesPromise(NThreading::NewPromise())
        , GatewaysConfig(gatewaysConfig)
        , SqlFlags(std::move(sqlFlags))
        , DefaultCredentials(std::move(defaultCredentials))
        , QWriterSettings(qWriterSettings)
        , QPlayerDir(qPlayerDir)
        , AstMemoryLimit(astMemoryLimit)
    {
        TaskWatcher->OnStart();
        SetTaskStatus(NProto::ETaskStatus::PENDING);
    }

    ~TYqlRunnableTask() {
        if (Program) {
            // Do this before destroying TaskProgressMerger, because it can be used via operation progress
            Program->Abort();
        }
    }

    const NProto::TTaskResult& GetResult() const override {
        return *TaskResult;
    }

    ITaskWatcherPtr GetWatcher() const override {
        return TaskWatcher;
    }

    bool DoRun() override {
        const TString& taskId = TaskData->GetId();
        SetCurrentOperationId(taskId.data());
        YQL_LOG_CTX_ROOT_SESSION_SCOPE(taskId);
        YQL_PROFILE_SCOPE(TRACE, "WholeOperation");

        NProto::ETaskStatus finishStatus = NProto::ETaskStatus::UNKNOWN_STATUS;
        try {
            YQL_LOG(INFO) << "Start processing task: " << TaskData->GetId();
            DoInit();
            OnProgramInitialized();
            EnsureNotAborted("init");

            SetTaskStatus(NProto::ETaskStatus::RUNNING);
            DoParse();

            if (Action != NProto::ETaskAction::PARSE) {
                UserMetrics->Flush();
                EnsureNotAborted("parse");
                DoCompile(Action == NProto::ETaskAction::EXTRACT_PARAMS_META);

                if (Action != NProto::ETaskAction::COMPILE) {
                    UserMetrics->Flush();
                    EnsureNotAborted("compile");

                    switch (Action) {
                    case NProto::ETaskAction::VALIDATE:
                        DoValidate();
                        break;

                    case NProto::ETaskAction::EXTRACT_PARAMS_META:
                        DoExtractParametersMetadata();
                        break;

                    case NProto::ETaskAction::DISCOVER:
                        DoDiscover();
                        break;

                    default:
                        DoOptimizeOrRun();
                    }
                }
            }

            EnsureNotAborted("complete");

            if (DoWaitForActiveProcesses()) {
                throw TTaskAborted() << "aborted after complete";
            }

            Program->CommitQPlayer(TProgram::TStatus::Ok);
            if (Program->GetPrimary()->HasActiveProcesses()) {
                TaskResult->SetKeepRunning(true);
                finishStatus = UpdateRunning();
            } else {
                finishStatus = FinishedWithComplete();
            }
        } catch (const TTaskAborted& e) {
            YQL_LOG(INFO) << "task " << TaskData->GetId()
                          << ' ' << e.AsStrBuf();
            finishStatus = FinishWithAbort();
        } catch (const TTaskProcessingException& e) {
            YQL_LOG(INFO) << "task " << TaskData->GetId()
                          << " finished with error: " << e.AsStrBuf();
            if (Program) {
                Program->CommitQPlayer(TProgram::TStatus::Error);
            }
            finishStatus = IsAborted() ? FinishWithAbort() : FinishedWithError(e.AsStrBuf());
        } catch (...) {
            TString errMsg = CurrentExceptionMessage();
            YQL_LOG(ERROR) << "Error while processing task "
                           << TaskData->GetId() << ": " << errMsg;

            if (IsAborted()) {
                finishStatus = FinishWithAbort();
            } else {
                finishStatus = NProto::ETaskStatus::ERROR;
                TaskResult->SetStatus(finishStatus);
                TaskResult->SetRevision(TaskResult->GetRevision() + 1);
                AddFatalMessageToTaskResult(TaskResult.get(), errMsg);

                TTaskResultPtr taskResultChanges = std::make_shared<NProto::TTaskResult>();
                taskResultChanges->SetStatus(TaskResult->GetStatus());
                taskResultChanges->SetRevision(TaskResult->GetRevision());
                AddFatalMessageToTaskResult(taskResultChanges.get(), errMsg);

                auto changesMask = TTaskResultChanges::STATUS_ERROR |
                        TTaskResultChanges::REVISION |
                        TTaskResultChanges::ERRORS;

                changesMask |= TemporarilyFillAstAndPlan(taskResultChanges.get());
                changesMask |= FillStatistics(taskResultChanges.get());

                TaskWatcher->OnResultChange(changesMask, taskResultChanges);
            }
        }

        WriteCPUStatisticsToMetrics();
        WriteTransformStatsToMetrics();
        UserMetrics->IncCounter("completion_status",
                NProto::ETaskStatus_Name(finishStatus));
        UserMetrics->Flush();

        TaskWatcher->OnFinish(finishStatus);
        return true;
    }

    void DoAbort() override {
        if (ProgramAbortState_.exchange(EProgramAbortState::Requested) == EProgramAbortState::Ready) {
            AbortProgram();
        }
    }

private:
    enum class EProgramAbortState {
        Initializing,
        Ready,
        Requested,
    };

    std::atomic<EProgramAbortState> ProgramAbortState_{EProgramAbortState::Initializing};

    void OnProgramInitialized() {
        // One atomic handoff prevents cancellation from being lost or applied during construction.
        auto expected = EProgramAbortState::Initializing;
        if (!ProgramAbortState_.compare_exchange_strong(expected, EProgramAbortState::Ready)) {
            YQL_ENSURE(expected == EProgramAbortState::Requested, "Unexpected program abort state");
            AbortProgram();
        }
    }

    void AbortProgram() {
        Program->Abort();
        if (Program->GetPrimary()->HasActiveProcesses()) {
            ActiveProcessesPromise.SetValue();
        }
    }

    void WriteCPUStatisticsToMetrics() {
        const auto labelName = "duration_ms";
        UserMetrics->SetCounter(labelName, "elapsed", Timer.Get().MilliSeconds());

        TRusage ru;
        ru.Fill();
        UserMetrics->SetCounter(labelName, "user", ru.Utime.MilliSeconds());
        UserMetrics->SetCounter(labelName, "system", ru.Stime.MilliSeconds());
    }

    void WriteTransformStatsToMetrics() {
        if (!Program) {
            return;
        }
        auto stats = Program->GetPrimary()->GetRawDiagnostics();
        DoWriteTransformStatsToMetrics("TransformationPipeline", Program->GetPrimary()->GetRawDiagnostics());
        GlobalMetrics->Flush();
    }

    void DoWriteTransformStatsToMetrics(const TString& label, const IGraphTransformer::TStatistics& stats) {
        if (stats.TransformDuration.MicroSeconds() > 0) {
            GlobalMetrics->SetCounter("TransformDurationUs", label, stats.TransformDuration.MicroSeconds());
        }
        if (stats.WaitDuration.MicroSeconds() > 0) {
            GlobalMetrics->SetCounter("TransformWaitDurationUs", label, stats.WaitDuration.MicroSeconds());
        }
        if (stats.NewExprNodes > 0) {
            GlobalMetrics->SetCounter("TransformNewExprNodes", label, stats.NewExprNodes);
        }
        if (stats.NewTypeNodes > 0) {
            GlobalMetrics->SetCounter("TransformNewTypeNodes", label, stats.NewTypeNodes);
        }
        if (stats.NewConstraintNodes > 0) {
            GlobalMetrics->SetCounter("TransformNewConstraintNodes", label, stats.NewConstraintNodes);
        }
        if (stats.Repeats > 0) {
            GlobalMetrics->SetCounter("TransformRepeats", label, stats.Repeats);
        }
        if (stats.Restarts > 0) {
            GlobalMetrics->SetCounter("TransformRestarts", label, stats.Restarts);
        }

        for (auto& stage : stats.Stages) {
            DoWriteTransformStatsToMetrics(stage.first, stage.second);
        }
    }

    void EnsureNotAborted(TStringBuf message) {
        if (Y_UNLIKELY(IsAborted())) {
            throw TTaskAborted() << "aborted after " << message;
        }
    }

    void SetTaskStatus(NProto::ETaskStatus status) {
        TaskResult->SetStatus(status);
        TaskResult->SetRevision(TaskResult->GetRevision() + 1);

        TTaskResultPtr taskResultChanges = std::make_shared<NProto::TTaskResult>();
        taskResultChanges->SetStatus(TaskResult->GetStatus());
        taskResultChanges->SetRevision(TaskResult->GetRevision());

        auto changesMask = TTaskResultChanges::FromStatus(status) |
                TTaskResultChanges::REVISION;

        changesMask |= TemporarilyFillAstAndPlan(taskResultChanges.get());

        TaskWatcher->OnResultChange(changesMask, taskResultChanges);
    }

    void DoInit() {
        UserMetrics->IncCounter("action", NProto::ETaskAction_Name(Action));
        UserMetrics->IncCounter("syntax", NProto::ESyntax_Name(TaskData->GetSyntax()));
        TLangVersion langVersion = MinLangVersion;
        if (TaskData->HasLangVer()) {
            YQL_LOG(INFO) << "LangVer: " << TaskData->GetLangVer();
            if (!ParseLangVersion(TaskData->GetLangVer(), langVersion)) {
                throw TTaskProcessingException() << "Can't parse LangVer";
            }
        }
        ProgramFactory->SetLanguageVersion(langVersion);

        if (TaskData->HasMaxLangVer()) {
            YQL_LOG(INFO) << "MaxLangVer: " << TaskData->GetMaxLangVer();
            TLangVersion ver;
            if (!ParseLangVersion(TaskData->GetMaxLangVer(), ver)) {
                throw TTaskProcessingException() << "Can't parse MaxLangVer";
            }

            ProgramFactory->SetMaxLanguageVersion(ver);
        } else {
            ProgramFactory->SetMaxLanguageVersion(GetMaxLangVersion());
        }

        if (TaskData->HasIsSystemRequest() && TaskData->GetIsSystemRequest()) {
            ProgramFactory->SetVolatileResults();
        }

        if (TaskData->GroupAllowedSize()) {
            YQL_LOG(INFO) << "GroupAllowed: " << JoinRange(",", TaskData->GetGroupAllowed().begin(), TaskData->GetGroupAllowed().end());
        }

        const bool isRobot = IsRobot(*TaskData);
        if (isRobot && IsAvailableLangVersion(NFeature::DisableDqForRobotsByDefault.MinLangVer, langVersion) && GatewaysConfig.HasDq()) {
            auto* dqConfig = GatewaysConfig.MutableDq();
            dqConfig->SetDefaultAutoPercentage(0);
            dqConfig->ClearDefaultAutoByHour();
        }
        ProgramFactory->SetGatewaysConfig(&GatewaysConfig);

        TCredentials::TPtr creds = MakeIntrusive<TCredentials>();
        if (TaskData->HasAuthData()) {
            ParseCredentials(TaskData->GetAuthData(), *creds);
        }

        if (TaskData->HasUserAuthData()) {
            const auto& userAuth = TaskData->GetUserAuthData();
            TUserCredentials userCredentials;
            userCredentials.OauthToken = userAuth.GetOauthToken();
            userCredentials.BlackboxSessionIdCookie = userAuth.GetBlackboxSessionIdCookie();
            userCredentials.QueryIdentityToken = userAuth.GetQueryIdentityToken();
            creds->SetUserCredentials(userCredentials);
        }
        creds->SetGroups(std::unordered_set<TString>(TaskData->GetGroupAllowed().begin(), TaskData->GetGroupAllowed().end()));
        creds->SetIsRobot(isRobot);

        ProgramFactory->SetCredentials(creds);

        const bool restrictedAccess = TaskData->HasStaffAffiliation() && TaskData->GetStaffAffiliation() != NProto::EStaffAffiliation::YANDEX;
        for (const auto& defCred: DefaultCredentials) {
            if (!creds->FindCredential(defCred.first)) {
                if (!restrictedAccess || AnyOf(defCred.second.second, [&creds](const auto& grp) { return creds->GetGroups().contains(grp); })) {
                    creds->AddCredential(defCred.first, defCred.second.first);
                    YQL_LOG(DEBUG) << "Adding default credential " << defCred.first;
                } else {
                    YQL_LOG(DEBUG) << "Skipping restricted default credential " << defCred.first;
                }
            } else {
                YQL_LOG(DEBUG) << "Skipping alreday confgured default credential " << defCred.first;
            }
        }

        if (TaskData->FilesSize() > 0) {
            const auto& files = TaskData->GetFiles();
            ProgramFactory->AddUserDataTable(
                        ToUserDataTable(*creds, files.begin(), files.end()));
        }

        auto progressWriter = (TaskWatcher->WillWatch(TTaskResultChanges::PROGRESS)) ?
            [this](const TOperationProgress& p) {
                if (p.State == TOperationProgress::EState::Failed && IsAborted()) {
                    // replace Failed -> Aborted state to show more realistic
                    // progress state in plan node
                    TOperationProgress copyOfP(p);
                    copyOfP.State = TOperationProgress::EState::Aborted;
                    TaskProgressMerger.MergeWith(copyOfP);
                } else {
                    TaskProgressMerger.MergeWith(p);
                }

                if (TaskProgressMerger.HasChangesSinceLastFlush()) {
                    TTaskResultPtr taskResultChanges = std::make_shared<NProto::TTaskResult>();
                    taskResultChanges->SetStatus(TaskResult->GetStatus());
                    taskResultChanges->SetRevision(TaskResult->GetRevision());
                    TaskProgressMerger.FlushTo(taskResultChanges->MutableProgress());
                    TaskWatcher->OnResultChange(
                                TTaskResultChanges::PROGRESS,
                                taskResultChanges);
                }
            }
            : TMaybe<TOperationProgressWriter>();
        TMaybe<NSQLTranslation::TTranslationSettings> settings;
        if (TaskData->GetSyntax() == NProto::SQL || TaskData->GetSyntax() == NProto::SQLv1 || TaskData->GetSyntax() == NProto::PG) {
            settings = GetSqlTranslationSettings();
        }
        bool enableQPlayer = false;
        bool enableQPlayerForRobot = false;
        auto qPlayerCaptureMode = EQPlayerCaptureMode::None;
        const std::unordered_set<std::string_view> groups(TaskData->GetGroupAllowed().begin(), TaskData->GetGroupAllowed().end());
        bool qPlayerSkip = IsHealthCheckTask(TaskData->GetId()) || QPlayerDir.empty() || (TaskData->HasIsSystemRequest() && TaskData->GetIsSystemRequest());

        if (GatewaysConfig.HasYqlCore() && GatewaysConfig.GetYqlCore().HasQPlayerFullCaptureActivation()) {
            auto f = GatewaysConfig.MutableYqlCore()->MutableQPlayerFullCaptureActivation();

            bool isOkType = Action == NProto::ETaskAction::RUN;
            bool skip = qPlayerSkip || !isOkType;
            YQL_LOG(INFO) << "QPlayer full capture: has activation; skip: " << skip;

            if (!skip && NConfig::Allow(*f, TaskData->GetUsername(), isRobot, groups)) {
                f->SetPercentage(100u);
                enableQPlayer = true;
                enableQPlayerForRobot = true;
                qPlayerCaptureMode = EQPlayerCaptureMode::Full;
                YQL_LOG(INFO) << "QPlayer full capture enabled";
            } else {
                f->SetPercentage(0u);
                YQL_LOG(INFO) << "QPlayer full capture disabled";
            }
        }

        if (!enableQPlayer && GatewaysConfig.HasYqlCore() && GatewaysConfig.GetYqlCore().HasQPlayerActivation()) {
            auto f = GatewaysConfig.MutableYqlCore()->MutableQPlayerActivation();

            bool isOkType = Action == NProto::ETaskAction::VALIDATE || Action == NProto::ETaskAction::RUN || Action == NProto::ETaskAction::OPTIMIZE || Action == NProto::ETaskAction::LINEAGE;
            bool skip = qPlayerSkip || !isOkType;
            YQL_LOG(INFO) << "QPlayer: has activation; skip: " << skip;
            if (!skip && NConfig::Allow(*f, TaskData->GetUsername(), isRobot, groups)) {
                f->SetPercentage(100u);
                enableQPlayer = true;
                enableQPlayerForRobot = GatewaysConfig.GetYqlCore().HasQPlayerRobotPercentage() && RandomNumber<ui8>(100) < GatewaysConfig.GetYqlCore().GetQPlayerRobotPercentage();
                qPlayerCaptureMode = EQPlayerCaptureMode::MetaOnly;
                YQL_LOG(INFO) << "QPlayer enabled";
            } else {
                f->SetPercentage(0u);
                YQL_LOG(INFO) << "QPlayer disabled";
            }
        }

        bool alwaysCommitQPlayer = GatewaysConfig.HasYqlCore() && GatewaysConfig.GetYqlCore().HasAlwaysCommitQPlayer() ?
                                                                                GatewaysConfig.GetYqlCore().GetAlwaysCommitQPlayer() : false;

        auto provider = ExtCreateRemoteLayerProvider(GatewaysConfig, creds);
        Program.emplace(
            ProgramFactory,
            provider,
            TaskData,
            progressWriter,
            GetHiddenMode(),
            settings,
            SqlFlags.ToMap(/*map=*/{}, /*areOnlyActivated=*/true),
            QPlayerDir,
            QWriterSettings,
            enableQPlayer,
            enableQPlayerForRobot,
            qPlayerCaptureMode,
            alwaysCommitQPlayer
        );
    }

    EHiddenMode GetHiddenMode() {
        if (Action != NProto::ETaskAction::RUN || !GatewaysConfig.HasDq()) {
            return EHiddenMode::Disable;
        }
        auto hiddenMode = EHiddenMode::Auto;
        if (TaskData->HasAttributes()) {
            NYT::TNode userAttrs = NYT::NodeFromYsonString(TaskData->GetAttributes());
            for (const auto& item: userAttrs.AsMap()) {
                const TString& key = item.first;
                const NYT::TNode& value = item.second;
                if (key == "hidden_mode") {
                    if (value.IsString()) {
                        TryFromString(value.AsString(), hiddenMode);
                    }
                    break;
                }
            }
        }
        if (hiddenMode != EHiddenMode::Auto) {
            YQL_LOG(INFO) << "HiddenMode from attrs: " << hiddenMode;
            return hiddenMode;
        }

        if (GatewaysConfig.GetDq().HasHiddenActivation()) {
            const bool isRobot = IsRobot(*TaskData);
            const std::unordered_set<std::string_view> groups(TaskData->GetGroupAllowed().begin(), TaskData->GetGroupAllowed().end());
            const ui32 hiddenPercentage = NConfig::GetPercentage(GatewaysConfig.GetDq().GetHiddenActivation(), TaskData->GetUsername(), isRobot, groups);
            ui32 finalHiddenPercentage = hiddenPercentage;
            for (auto& attr: *GatewaysConfig.MutableDq()->MutableDefaultSettings()) {
                if (attr.HasActivation()
                    && std::find(attr.GetActivation().GetTags().begin(), attr.GetActivation().GetTags().end(), "with_hidden") != attr.GetActivation().GetTags().end()) {

                    if (NConfig::Allow(attr.GetActivation(), TaskData->GetUsername(), isRobot, groups)) {
                        const auto attrPercentage = NConfig::GetPercentage(attr.GetActivation(), TaskData->GetUsername(), isRobot, groups);
                        YQL_LOG(INFO) << "Feature '" << attr.GetName() << "' with hidden activation, percentage=" << attrPercentage;
                        YQL_ENSURE(attrPercentage > 0);
                        finalHiddenPercentage = Max(finalHiddenPercentage, Min(100u, hiddenPercentage * 100u / attrPercentage));
                        attr.MutableActivation()->SetPercentage(100u);
                    } else {
                        attr.MutableActivation()->SetPercentage(0u);
                    }
                }
            }
            YQL_LOG(INFO) << "Recalculated hidden percentage=" << finalHiddenPercentage;
            const auto random = RandomNumber<ui8>(100);
            if (random < finalHiddenPercentage) {
                return EHiddenMode::Force;
            }
        }

        return EHiddenMode::Disable;
    }

    template<typename THas, typename TUserSpecificExclude, typename TDefault, typename TDefaultByHour, typename TUserSpecificInclude>
    ISqlFeaturePolicy::TPtr MakeFeaturePolicy(THas has, TUserSpecificExclude excludeUsers, TDefault _default, TDefaultByHour defaultByHour, TUserSpecificInclude includeUsers) const {
        if (!has()) {
            return ISqlFeaturePolicy::MakeAlwaysDisallow();
        }

        if (TaskData->HasUsername()) {
            auto user = TaskData->GetUsername();
            if (FindPtr(excludeUsers(), user)) {
                return ISqlFeaturePolicy::MakeAlwaysDisallow();
            }

            if (FindPtr(includeUsers(), user)) {
                YQL_CLOG(DEBUG, Core) << "Enable future for user";
                return ISqlFeaturePolicy::MakeAlwaysAllow();
            }
        }

        ui32 defaultPercent = _default();
        THashMap<ui32, ui32> percentByHour;
        for (auto& item : defaultByHour()) {
            auto hour = item.GetHour();
            if (hour > 23) {
                continue;
            }
            percentByHour[hour] = item.GetPercentage();
        }

        return new TRandomAllowPolicy(defaultPercent, std::move(percentByHour));
    }

    ISqlFeaturePolicy::TPtr MakeV0WarnAsErrorPolicy() const {
        return MakeFeaturePolicy(
            [&](){ return GatewaysConfig.HasSqlCore(); },
            [&](){ return GatewaysConfig.GetSqlCore().GetNoV0SyntaxErrorForUsers(); },
            [&](){ return GatewaysConfig.GetSqlCore().GetV0SyntaxWarnAsErrorPercentage(); },
            [&](){ return GatewaysConfig.GetSqlCore().GetV0SyntaxWarnAsErrorByHour(); },
            [&](){ return ::google::protobuf::RepeatedPtrField<TProtoStringType>{}; }
            );
    }

    ISqlFeaturePolicy::TPtr MakeDqFeaturePolicy() const {
        return MakeFeaturePolicy(
            [&](){ return GatewaysConfig.HasDq(); },
            [&](){ return GatewaysConfig.GetDq().GetNoDefaultAutoForUsers(); },
            [&](){ return GatewaysConfig.GetDq().GetDefaultAutoPercentage(); },
            [&](){ return GatewaysConfig.GetDq().GetDefaultAutoByHour(); },
            [&](){ return ::google::protobuf::RepeatedPtrField<TProtoStringType>{}; }
            );
    }

    void DoParse() {
        switch (TaskData->GetSyntax()) {
        case NProto::ESyntax::YQL: {
            if (!Program->GetPrimary()->ParseYql()) {
                throw TTaskProcessingException() << "ParseYql";
            }
            break;
        }
        case NProto::ESyntax::PG:
        case NProto::ESyntax::SQL:
        case NProto::ESyntax::SQLv1:
        case NProto::ESyntax::SPARK_SQL: {
            if (!Program->GetPrimary()->ParseSql(GetSqlTranslationSettings())) {
                throw TTaskProcessingException() << "ParseSql";
            }

            GlobalMetrics->Flush();
            break;
        }
        default:
            throw TTaskProcessingException()
                    << "Unknown program syntax: "
                    << NProto::ESyntax_Name(TaskData->GetSyntax());
        }
    }

    void DoExtractParametersMetadata() {
        if (!Program->GetPrimary()->ExtractQueryParametersMetadata()) {
            throw TTaskProcessingException() << "ExtractQueryParametersMetadata";
        }
    }

    void DoCompile(bool skipLibraries) {
        if (!Program->GetPrimary()->Compile(TaskData->GetUsername(), skipLibraries)) {
            throw TTaskProcessingException() << "Compile";
        }
    }

    void DoValidate() {
        auto status = Program->GetPrimary()->Validate(TaskData->GetUsername());
        if (status == TProgram::TStatus::Error) {
            throw TTaskProcessingException() << "Validate";
        }
    }

    void DoDiscover() {
        auto status = Program->GetPrimary()->Discover(TaskData->GetUsername());
        if (status == TProgram::TStatus::Error) {
            throw TTaskProcessingException() << "Discover";
        }
    }

    void DoOptimizeOrRun() {
        TTaskPipelineConfigurator pipelineConfig(
                    *TaskResult, *Program->GetPrimary(), TaskWatcher.Get());

        TProgram::TStatus status = TProgram::TStatus::Error;
        TStringBuf actionStr;

        const TString& username = TaskData->GetUsername();
        if (Action == NProto::ETaskAction::OPTIMIZE) {
            actionStr = "Optimize";
            status = Program->GetPrimary()->OptimizeWithConfig(username, pipelineConfig);
        } else if (Action == NProto::ETaskAction::LINEAGE) {
            actionStr = "Lineage";
            status = Program->GetPrimary()->LineageWithConfig(username, pipelineConfig);
        } else {
            actionStr = "Run";
            status = Program->Run(username, pipelineConfig);
        }

        if (status == TProgram::TStatus::Error) {
            if (YQL_CLOG_ACTIVE(TRACE, Core)) {
                try {
                    if (auto ast = Program->GetPrimary()->GetQueryAst(AstMemoryLimit)) {
                        YQL_CLOG(TRACE, Core) << *ast;
                    }
                } catch(...) {}
            }
            throw TTaskProcessingException() << actionStr;
        }
    }

    bool DoWaitForActiveProcesses() {
        if (Program->GetPrimary()->NeedWaitForActiveProcesses()) {
            UpdateRunning();
            ActiveProcessesPromise.GetFuture().Wait();
            return true;
        }
        return false;
    }

    NProto::ETaskStatus UpdateRunning() {
        auto status = NProto::ETaskStatus::RUNNING;
        TaskResult->SetStatus(status);
        TaskResult->SetRevision(TaskResult->GetRevision() + 1);

        TTaskResultPtr taskResultChanges = std::make_shared<NProto::TTaskResult>();
        taskResultChanges->SetStatus(TaskResult->GetStatus());
        taskResultChanges->SetRevision(TaskResult->GetRevision());
        if (TaskResult->GetKeepRunning()) {
            taskResultChanges->SetKeepRunning(TaskResult->GetKeepRunning());
        }

        auto changesMask = TTaskResultChanges::STATUS_RUNNING |
                TTaskResultChanges::REVISION;

        changesMask |= FillAst(taskResultChanges.get());
        changesMask |= FillErrors(taskResultChanges.get());
        changesMask |= FillResults(taskResultChanges.get());
        changesMask |= TemporarilyFillAstAndPlan(taskResultChanges.get());
        changesMask |= FillStatistics(taskResultChanges.get());

        TaskWatcher->OnResultChange(changesMask, taskResultChanges);
        return status;
    }

    NProto::ETaskStatus FinishedWithComplete() {
        NYql::DropYtDebugLog();

        auto status = NProto::ETaskStatus::COMPLETED;
        TaskResult->SetStatus(status);
        TaskResult->SetRevision(TaskResult->GetRevision() + 1);

        TTaskResultPtr taskResultChanges = std::make_shared<NProto::TTaskResult>();
        taskResultChanges->SetStatus(TaskResult->GetStatus());
        taskResultChanges->SetRevision(TaskResult->GetRevision());

        auto changesMask = TTaskResultChanges::STATUS_COMPLETED |
                TTaskResultChanges::REVISION;

        changesMask |= FillAst(taskResultChanges.get());
        changesMask |= FillErrorsWithFinal(taskResultChanges.get());
        changesMask |= FillResults(taskResultChanges.get());
        changesMask |= TemporarilyFillAstAndPlan(taskResultChanges.get());
        changesMask |= FillStatistics(taskResultChanges.get());

        TaskWatcher->OnResultChange(changesMask, taskResultChanges);
        return status;
    }

    NProto::ETaskStatus FinishedWithError(TStringBuf errorType) {
        NYql::FlushYtDebugLog();

        auto status = NProto::ETaskStatus::ERROR;
        TaskResult->SetStatus(status);
        TaskResult->SetRevision(TaskResult->GetRevision() + 1);
        UserMetrics->IncCounter("errors", TString(errorType));

        TTaskResultPtr taskResultChanges = std::make_shared<NProto::TTaskResult>();
        taskResultChanges->SetStatus(TaskResult->GetStatus());
        taskResultChanges->SetRevision(TaskResult->GetRevision());

        auto changesMask = TTaskResultChanges::STATUS_ERROR |
                TTaskResultChanges::REVISION;

        changesMask |= FillAst(taskResultChanges.get());
        changesMask |= FillErrorsWithFinal(taskResultChanges.get());
        changesMask |= TemporarilyFillAstAndPlan(taskResultChanges.get());
        changesMask |= FillStatistics(taskResultChanges.get());

        TaskWatcher->OnResultChange(changesMask, taskResultChanges);
        return status;
    }

    NProto::ETaskStatus FinishWithAbort() {
        NYql::FlushYtDebugLog();
        auto status = NProto::ETaskStatus::ABORTED;
        SetTaskStatus(status);
        return status;
    }

    TTaskResultChanges::TMask FillAst(NProto::TTaskResult* taskResultDelta) {
        TMaybe<TString> ast;
        if (Program) {
            try {
                ast = Program->GetPrimary()->GetQueryAst(AstMemoryLimit);
            } catch (...) {
                YQL_LOG(ERROR) << "Can't get query AST: " << CurrentExceptionMessage();
            }
        }

        if (!ast) {
            return 0;
        }

        if (ast->size() + TaskResult->GetPlan().size() > MAX_RESULT_LIMIT) {
            ast = TString(AST_TOO_LARGE);
        }

        // TODO (jamel): temporarily always fill AST
//        if (!TaskResult->HasAst() || *ast != TaskResult->GetAst()) {
            TaskResult->SetAst(*ast);
            taskResultDelta->SetAst(*ast);
            return TTaskResultChanges::AST;
//        }

//        return 0;
    }

    TTaskResultChanges::TMask FillErrors(NProto::TTaskResult* taskResultDelta) {
        return NYql::FillErrors(Program->GetPrimary()->Issues(), TaskResult.get(), taskResultDelta);
    }

    TTaskResultChanges::TMask FillErrorsWithFinal(NProto::TTaskResult* taskResultDelta) {
        if (!Program) {
            return 0;
        }
        Program->GetPrimary()->FinalizeIssues();
        return FillErrors(taskResultDelta);
    }

    TTaskResultChanges::TMask FillResults(NProto::TTaskResult* taskResultDelta) {
        bool atLeastOneAdded = false;

        if (NProto::ETaskAction::DISCOVER == Action) {
            auto discoveryData = Program->GetPrimary()->GetDiscoveredData();
            if (discoveryData) {
                atLeastOneAdded = true;
                TaskResult->AddResults(*discoveryData);
                taskResultDelta->AddResults(*discoveryData);
            }
        } else {
            if (Program->GetPrimary()->HasResults()) {
                for (const auto& result: Program->GetPrimary()->Results()) {
                    atLeastOneAdded = true;
                    TaskResult->AddResults(result);
                    taskResultDelta->AddResults(result);
                }
            }

            auto diagnostics = Program->GetPrimary()->GetDiagnostics();
            if (diagnostics) {
                atLeastOneAdded = true;
                TaskResult->AddResults(*diagnostics);
                taskResultDelta->AddResults(*diagnostics);
            }

            auto tasksInfo = Program->GetPrimary()->GetTasksInfo();
            if (tasksInfo) {
                atLeastOneAdded = true;
                TaskResult->AddResults(*tasksInfo);
                taskResultDelta->AddResults(*tasksInfo);
            }

            auto extractedParamsMetadata = Program->GetPrimary()->GetExtractedQueryParametersMetadataYson();
            if (extractedParamsMetadata) {
                atLeastOneAdded = true;
                TaskResult->AddResults(extractedParamsMetadata);
                taskResultDelta->AddResults(extractedParamsMetadata);
            }

            auto lineage = Program->GetPrimary()->GetLineage();
            if (lineage) {
                auto lineageTooLarge = []() {
                    TStringStream s;
                    NYson::TYsonWriter writer(&s, NYson::EYsonFormat::Binary);
                    writer.OnBeginMap();
                    writer.OnKeyedItem("Error");
                    writer.OnStringScalar("Lineage is too large");
                    writer.OnEndMap();
                    return s.Str();
                };
                FitResultStringToLimit(lineage, lineageTooLarge);
                atLeastOneAdded = true;
                TaskResult->AddResults(*lineage);
                taskResultDelta->AddResults(*lineage);
            }
        }

        if (atLeastOneAdded) {
            return TTaskResultChanges::RESULTS;
        }

        return 0;
    }

    // TODO (jamel): remove it when implementing reliable Inspector client
    TTaskResultChanges::TMask TemporarilyFillAstAndPlan(
            NProto::TTaskResult* taskResultDelta)
    {
        TTaskResultChanges::TMask changesMask = 0;
        if (TaskResult->HasAst()) {
            taskResultDelta->SetAst(TaskResult->GetAst());
            changesMask |= TTaskResultChanges::AST;
        }
        if (TaskResult->HasPlan()) {
            taskResultDelta->SetPlan(TaskResult->GetPlan());
            changesMask |= TTaskResultChanges::PLAN;
        }
        return changesMask;
    }

    TTaskResultChanges::TMask FillStatistics(NProto::TTaskResult* taskResultDelta) {
        TTaskResultChanges::TMask changesMask = 0;
        if (Program) {
            auto stat = Program->GetStatistics(true);
            if (stat) {
                TaskResult->SetStatistics(*stat);
                taskResultDelta->SetStatistics(*stat);
                changesMask |= TTaskResultChanges::STATISTICS;
            }
        }
        return changesMask;
    }

    NSQLTranslation::TTranslationSettings GetSqlTranslationSettings() const {
        NSQLTranslation::TTranslationSettings settings;
        settings.ClusterMapping = Clusters;
        settings.ClusterPathPrefixes = ClusterPathPrefixes;
        if (DefaultTranslationCluster) {
            settings.DefaultCluster = *DefaultTranslationCluster;
        }
        settings.V0Behavior = NSQLTranslation::EV0Behavior::Report;
        settings.V0WarnAsError = MakeV0WarnAsErrorPolicy();
        settings.DqDefaultAuto = MakeDqFeaturePolicy();
        settings.StrictConfigValidation = TaskData->GetStrictConfigValidation();
        try {
            NSQLTranslation::ParseTranslationSettings(SqlFlags.ToMap(), settings);
        } catch (const yexception& e) {
            AddErrorMessageToTaskResult(TaskResult.get(), TString(e.AsStrBuf()));
            throw TTaskProcessingException() << "ParseTranslationSettings";
        }

        if (TaskData->GetSyntax() == NProto::ESyntax::SQL) {
            settings.SyntaxVersion = 0;
        } else if (TaskData->GetSyntax() == NProto::ESyntax::SQLv1) {
            settings.SyntaxVersion = 1;
        }

        if (TaskData->GetSyntax() == NProto::ESyntax::PG) {
            settings.PgParser = true;
        }

        if (TaskData->GetSyntax() == NProto::ESyntax::SPARK_SQL) {
            settings.Syntax = "spark";
        }

        if (NProto::ETaskAction::DISCOVER == Action) {
            settings.Mode = NSQLTranslation::ESqlMode::DISCOVERY;
        }
        return settings;
    }
};

} // namespace


void TTaskResultChanges::Copy(
        NProto::TTaskResult* to, const NProto::TTaskResult& from,
        TMask changesMask)
{
    if ((changesMask & AST) && from.HasAst()) {
       to->SetAst(from.GetAst());
    }
    if ((changesMask & PLAN) && from.HasPlan()) {
        to->SetPlan(from.GetPlan());
    }
    if ((changesMask & RESULTS) && from.ResultsSize() > 0) {
        to->MutableResults()->MergeFrom(from.GetResults());
    }
    if ((changesMask & PROGRESS) && from.HasProgress()) {
        to->MutableProgress()->MergeFrom(from.GetProgress());
    }
    // always copy errors and revision number if any
    if (from.ErrorsSize() > 0) {
        to->MutableErrors()->MergeFrom(from.GetErrors());
    }
    if (from.IssuesSize() > 0) {
        to->MutableIssues()->MergeFrom(from.GetIssues());
    }
    if (from.HasRevision()) {
        to->SetRevision(from.GetRevision());
    }
    if (IsAnyStatus(changesMask) && from.HasStatus()) {
        to->SetStatus(from.GetStatus());
    }
    if ((changesMask & STATISTICS) && from.HasStatistics()) {
        to->SetStatistics(from.GetStatistics());
    }
}

//////////////////////////////////////////////////////////////////////////////
// TYqlRunnableTaskFactory::TImpl
//////////////////////////////////////////////////////////////////////////////
struct TYqlRunnableTaskFactory::TImpl {
    TGatewaysConfig GatewaysConfig;
    TStaticGatewaysConfig StaticGatewaysConfig;
    NProto::TTaskExecutorConfig ExecutorConfig;
    TMaybe<NProto::TDqConfig> DqConfig;
    std::function<void()> InterruptionPointFunc;
    TIntrusivePtr<IFunctionRegistry> FuncRegistry;
    TUdfIndexPackageSet::TPtr UdfIndexPackageSet;
    TUdfIndex::TPtr UdfIndex;
    std::unique_ptr<TActorSystemManager> DqActorSystemManager;
    ICoordinationHelper::TPtr DqCoordinator;
    TVector<TIntrusivePtr<TThrRefBase>> Gateways;
    TVector<TDataProviderInitializer> DataProvidersInit;
    TUrlPreprocessing::TPtr UrlPreprocessing;
    TConfigClusters::TPtr YtClusters;
    TFileStoragePtr FileStorage;
    TUserDataTable UserData;
    NProto::EStaffAffiliation Affiliation = NProto::EStaffAffiliation::YANDEX;
    TClustersMap Clusters;
    TClustersMap ClusterPathPrefixes;
    TMaybe<TString> DefaultTranslationCluster;
    IMetricsRegistryPtr Metrics;
    TMetricsDecorator MetricsDecorator;

    TExprContext LibrariesContext;
    IModuleResolver::TPtr ModuleResolver;
    IUrlListerManagerPtr UrlListerManager;
    THolder<TExprContext::TFreezeGuard> FreezeGuardHolder;
    IUdfResolver::TPtr UdfResolver;
    TString UdfBridgePath;
    THashMap<TString, TFileLinkPtr> FrozenFiles;
    IDqControlFactoryPtr DqControlFactory;
    std::unordered_map<TString, std::pair<TCredential, std::vector<TString>>> DefaultCredentials;
    TQWriterSettings QWriterSettings;
    TString QPlayerDir;
    TMaybe<size_t> AstMemoryLimit;
    TMaybe<TString> UdfResolverAnalyticsLogfile;
    NSQLTranslation::TTranslatorsRegistry TranslatorsRegistry;

    TGatewaySQLFlags SqlFlags;

    TImpl(
            const TGatewaysConfig& gatewaysConfig,
            const TStaticGatewaysConfig& staticGatewaysConfig,
            const TFileStorageConfig& fsConfig,
            const NProto::TTaskExecutorConfig& executorConfig,
            const TMaybe<NProto::TDqConfig>& dqConfig,
            IMetricsRegistryPtr metrics,
            const std::function<void()>& interruptionPointFunc)
        : GatewaysConfig(gatewaysConfig) // copied config
        , StaticGatewaysConfig(staticGatewaysConfig)
        , ExecutorConfig(executorConfig)
        , DqConfig(dqConfig)
        , InterruptionPointFunc(interruptionPointFunc)
        , Metrics(metrics)
        , MetricsDecorator(
                [](const IMetricsRegistryPtr& old, const TString& username) {
                    return old->Personalized(username);
                })
    {
        YQL_LOG(INFO) << "Start RunnableTaskFactory init ...";
        SyncWithStaticGateways(StaticGatewaysConfig, GatewaysConfig);
        // file storage for YAMR/YT and udf resolver
        UrlPreprocessing = new TUrlPreprocessing();
        YtClusters = MakeIntrusive<TConfigClusters>(GatewaysConfig.GetYt());

        if (ExecutorConfig.HasConvertAstMemoryLimit()) {
            AstMemoryLimit = ExecutorConfig.GetConvertAstMemoryLimit();
        }

        TVector<NYql::NFS::IDownloaderPtr> downloaders = {MakeYtDownloader(fsConfig, YtClusters)};
        ExtDownloaders(downloaders, fsConfig);
        FileStorage = WithAsync(CreateFileStorage(fsConfig, downloaders));

        TMap<TString, TString> udfsWithMd5;
        TVector<TString> udfs;

        if (executorConfig.HasQPlayerConfig()) {
            QWriterSettings.ItemsLimit = executorConfig.GetQPlayerConfig().GetOperationItemsLimit();
            QWriterSettings.BytesLimit = executorConfig.GetQPlayerConfig().GetOperationBytesLimit();
            QPlayerDir = executorConfig.GetQPlayerConfig().GetDirectory();
        }
        if (executorConfig.HasUdfResolverAnalyticsLogfile()) {
            UdfResolverAnalyticsLogfile = executorConfig.GetUdfResolverAnalyticsLogfile();
        }

        NPg::SetSqlLanguageParser(NSQLTranslationPG::CreateSqlLanguageParser());
        NPg::LoadSystemFunctions(*NSQLTranslationPG::CreateSystemFunctionsParser());
        if (executorConfig.HasPgExtConfigPath()) {
            YQL_LOG(INFO) << "Register PG extensions";
            YQL_ENSURE(NFs::Exists(executorConfig.GetPgExtConfigPath()),
                       "File " << executorConfig.GetPgExtConfigPath()
                       << " does not exist");
            NProto::TPgExtensions config;
            TFileInput in(executorConfig.GetPgExtConfigPath());
            ParseFromTextFormat(in, config, EParseFromTextFormatOption::AllowUnknownField);
            TVector<NPg::TExtensionDesc> extensions;
            PgExtensionsFromProto(config, extensions);
            YQL_LOG(INFO) << "Freeze " << extensions.size() << " .so files ...";
            for (auto& e : extensions) {
                auto link = FreezeFileEx(e.LibraryPath);
                e.LibraryPath = link->GetPath();
                e.LibraryMD5 = link->GetMd5();
            }

            NPg::RegisterExtensions(extensions, false,
                *NSQLTranslationPG::CreateExtensionSqlParser(),
                NKikimr::NMiniKQL::CreateExtensionLoader().get());
            YQL_LOG(INFO) << "Register PG extensions complete";
        }

        NPg::GetSqlLanguageParser()->Freeze();
        LibrariesContext.NextUniqueId = NPg::GetSqlLanguageParser()->GetContext().NextUniqueId;

        if (executorConfig.HasLoadUdfsDir() || executorConfig.LoadUdfFilesSize() > 0U) {
            TVector<TString> udfsPaths;
            udfsPaths.reserve(executorConfig.LoadUdfFilesSize());
            for (const auto& udfPath: executorConfig.GetLoadUdfFiles()) {
                udfsPaths.push_back(udfPath);
            }

            if (executorConfig.HasLoadUdfsDir()) {
                FindUdfsInDir(executorConfig.GetLoadUdfsDir(), &udfsPaths);
            }

            YQL_LOG(INFO) << "Freeze " << udfsPaths.size() << " UDFs ...";
            udfs.reserve(udfsPaths.size());
            for (auto& path : udfsPaths) {
                auto link = FreezeFileEx(path);
                udfsWithMd5.emplace(link->GetPath(), link->GetMd5());
                udfs.push_back(link->GetPath());
            }
            YQL_LOG(INFO) << "Freeze complete";
        }

        TVector<std::pair<TString, TString>> mrJobSystemLibsMd5;
        if (executorConfig.MrJobSystemLibsSize() > 0U) {
            TVector<TString> systemLibPaths;
            systemLibPaths.reserve(executorConfig.MrJobSystemLibsSize());
            for (const auto& systemLibPath: executorConfig.GetMrJobSystemLibs()) {
                systemLibPaths.push_back(systemLibPath);
            }

            YQL_LOG(INFO) << "Freeze " << systemLibPaths.size() << " System Libs ...";
            mrJobSystemLibsMd5.reserve(systemLibPaths.size());
            for (auto& path : systemLibPaths) {
                auto link = FreezeFileEx(path);
                mrJobSystemLibsMd5.push_back({link->GetPath(), link->GetMd5()});
            }
            YQL_LOG(INFO) << "Freeze complete";
        }

        // be careful this pointer can be changed (see code bellow)
        YQL_LOG(INFO) << "CreateFuncRegistry over " << udfs.size() << " UDFs ...";
        FuncRegistry = CreateFuncRegistry(udfs);
        YQL_LOG(INFO) << "CreateFuncRegistry complete";
        TString frozenUdfResolverPath;

        TVector<IUrlListerPtr> urlListers = {MakeYtUrlLister(YtClusters)};
        ExtUrlListers(urlListers);
        UrlListerManager = MakeUrlListerManager(std::move(urlListers));

        if (executorConfig.HasUdfResolverPath()) {
            YQL_ENSURE(NFs::Exists(executorConfig.GetUdfResolverPath()),
                       "File " << executorConfig.GetUdfResolverPath()
                       << " does not exist");

            frozenUdfResolverPath = FreezeFile(executorConfig.GetUdfResolverPath());
            const bool scanUdfs = executorConfig.GetScanUdfs();
            THoldingFileStorage holdingFileStorage(FileStorage);
            if (scanUdfs) {
                TString frozenUdfDependencyStubPath = executorConfig.GetUdfDependencyStubPath();
                if (frozenUdfDependencyStubPath) {
                    frozenUdfDependencyStubPath = FreezeFile(frozenUdfDependencyStubPath);
                }

                UdfResolver = NCommon::CreateOutProcUdfResolver(
                    FuncRegistry.Get(), FileStorage,
                    frozenUdfResolverPath,
                    executorConfig.GetUdfResolverUser(),
                    executorConfig.GetUdfResolverGroup(),
                    executorConfig.GetUdfResolverFilterSyscalls(),
                    frozenUdfDependencyStubPath,
                    udfsWithMd5);

                YQL_LOG(INFO) << "Build UdfIndex for " << udfsWithMd5.size() << " UDFs ...";
                UdfIndex = new TUdfIndex();
                UdfIndexPackageSet = new TUdfIndexPackageSet();
                const bool isTrusted = true;
                LoadRichMetadataToUdfIndex(*UdfResolver, udfsWithMd5, {}, isTrusted, TUdfIndex::EOverrideMode::RaiseError, *UdfIndex, holdingFileStorage);
                YQL_LOG(INFO) << "UdfIndex complete";
            }

            if (executorConfig.HasSystemUdfsDir()) {
                if (scanUdfs) {
                    YQL_LOG(INFO) << "Scan system UDFs ...";
                    LoadSystemUdfsIntoUdfIndex(executorConfig.GetSystemUdfsDir(), holdingFileStorage);
                    YQL_LOG(INFO) << "System UDFs scanned";
                } else {
                    YQL_LOG(INFO) << "Load system UDFs into function registry ...";
                    LoadSystemUdfsIntoFunctionRegistry(frozenUdfResolverPath, executorConfig.GetSystemUdfsDir());
                    YQL_LOG(INFO) << "System UDFs are loaded into function registry";
                }
            }

            if (!scanUdfs) {
                TString frozenUdfDependencyStubPath = executorConfig.GetUdfDependencyStubPath();
                if (frozenUdfDependencyStubPath) {
                    frozenUdfDependencyStubPath = FreezeFile(frozenUdfDependencyStubPath);
                }

                UdfResolver = NCommon::CreateOutProcUdfResolver(
                    FuncRegistry.Get(), FileStorage,
                    frozenUdfResolverPath,
                    executorConfig.GetUdfResolverUser(),
                    executorConfig.GetUdfResolverGroup(),
                    executorConfig.GetUdfResolverFilterSyscalls(),
                    frozenUdfDependencyStubPath);
            }

        } else {
            UdfResolver = NCommon::CreateSimpleUdfResolver(
                        FuncRegistry.Get(), FileStorage);
        }

        if (executorConfig.HasUdfBridgePath()) {
            YQL_ENSURE(NFs::Exists(executorConfig.GetUdfBridgePath()),
                       "File " << executorConfig.GetUdfBridgePath()
                       << " does not exist");
            UdfBridgePath = FreezeFile(executorConfig.GetUdfBridgePath());
        }

        if (StaticGatewaysConfig.HasYt()) {
            auto* ytConfig = StaticGatewaysConfig.MutableYt();
            for (auto& entry : *ytConfig->MutableMrJobBinaries()) {
                auto& binary = *entry.MutableBinary();
                YQL_ENSURE(NFs::Exists(binary.GetFile()), "File " << binary.GetFile() << " does not exist");
                auto mrJobLink = FreezeFileEx(binary.GetFile());
                binary.SetFile(mrJobLink->GetPath());
                binary.SetMd5(mrJobLink->GetMd5());
            }
            if (ytConfig->HasMrJobBin()) {
                auto originMrJobBin = ytConfig->GetMrJobBin();
                YQL_ENSURE(NFs::Exists(originMrJobBin),
                           "File " << originMrJobBin << " does not exist");
                auto mrJobLink = FreezeFileEx(originMrJobBin);
                ytConfig->SetMrJobBin(mrJobLink->GetPath());
                ytConfig->SetMrJobBinMd5(mrJobLink->GetMd5());

                for (auto systemLib : mrJobSystemLibsMd5) {
                    auto* systemLibWithMd5 = ytConfig->MutableMrJobSystemLibsWithMd5()->Add();
                    systemLibWithMd5->SetFile(systemLib.first);
                    systemLibWithMd5->SetMd5(systemLib.second);
                }
            }

        }

        if (ExecutorConfig.HasFmr() && ExecutorConfig.GetFmr().HasFmrJobPath()) {
            auto originFmrJobPath = ExecutorConfig.GetFmr().GetFmrJobPath();
            YQL_ENSURE(NFs::Exists(originFmrJobPath),
                       "File " << originFmrJobPath << " does not exist");
            auto fmrJobLink = FreezeFileEx(originFmrJobPath);
            ExecutorConfig.MutableFmr()->SetFmrJobPath(fmrJobLink->GetPath());
            ExecutorConfig.MutableFmr()->SetFmrJobPathMd5(fmrJobLink->GetMd5());
        }

        const bool hasCommunalDqBackend = DqConfig && AnyOf(
            DqConfig->GetYtBackends(),
            IsCommunalYtBackend);
        if (hasCommunalDqBackend) {
            // The local DQ gateway and control factory use backend 0 for the vanilla job.
            const auto& defaultYtBackend = DqConfig->GetYtBackends(0);
            YQL_ENSURE(
                IsCommunalYtBackend(defaultYtBackend),
                "The first YT backend must be communal when communal backends are configured");
            YQL_ENSURE(
                !defaultYtBackend.GetVanillaJobLite().empty(),
                "VanillaJobLite must be specified for the first communal YT backend");
        }

        if (DqConfig && DqConfig->GetYtBackends().size() > 0) {
            auto vanillaLitePath = DqConfig->GetYtBackends()[0].GetVanillaJobLite();
            if (NFs::Exists(vanillaLitePath)) {
                auto link = FreezeFileEx(vanillaLitePath);
                if (!FrozenFiles.contains(vanillaLitePath + ".stripped")) {
                    FrozenFiles[vanillaLitePath + ".stripped"] = FileStorage->PutFileStripped(link->GetPath(), link->GetMd5());
                }

                YQL_LOG(INFO) << "Vanilla job path " << link->GetPath();
                YQL_LOG(INFO) << "Vanilla job md5 " << link->GetMd5();

                (*DqConfig->MutableYtBackends())[0].SetVanillaJobLite(link->GetPath());
                (*DqConfig->MutableYtBackends())[0].SetVanillaJobLiteMd5(link->GetMd5());
            } else {
                YQL_LOG(WARN) << "Vanilla job does not exist";
            }
        }

        if (hasCommunalDqBackend && !(DqConfig->HasControl() && DqConfig->GetControl().GetDisabled())) {
            DqControlFactory = CreateDqControlFactory(*DqConfig, udfsWithMd5, FileStorage);
        }

        if (StaticGatewaysConfig.HasRtmr()) {
            auto* rtmrConfig = StaticGatewaysConfig.MutableRtmr();
            if (rtmrConfig->HasYqlRtmrDynLib()) {
                auto originLib = rtmrConfig->GetYqlRtmrDynLib();
                YQL_ENSURE(NFs::Exists(originLib),
                    "File " << originLib << " does not exist");
                rtmrConfig->SetYqlRtmrDynLib(FreezeFile(originLib));
            }

            auto* artifacts = rtmrConfig->MutableArtifacts();
            TVector<TString> files(artifacts->begin(), artifacts->end());

            artifacts->Clear();
            for (const auto& file: files) {
                YQL_ENSURE(NFs::Exists(file),
                    "File " << file << " does not exist");
                artifacts->Add(FreezeFile(file));
            }
        }

        for (const auto& t : ExecutorConfig.GetDefaultCredentials()) {
            if (t.HasAlias() && t.HasPath()) {
                TString tokenValue;
                try {
                    tokenValue = StripString(TFileInput(t.GetPath()).ReadLine());
                } catch (...) {
                    throw yexception() << "Cannot read file " << t.GetPath() << ". Reason: " << CurrentExceptionMessage();
                }
                auto cred = TCredential(t.GetCategory(), t.GetSubcategory(), tokenValue);
                auto allowedGroups = std::vector<TString>(t.GetAllowedGroups().begin(), t.GetAllowedGroups().end());
                DefaultCredentials.emplace(t.GetAlias(), std::pair(std::move(cred), std::move(allowedGroups)));
            }
        }
    }

    ~TImpl() {}

    void SetUserAffiliation(NProto::EStaffAffiliation affiliation) {
        Affiliation = affiliation;
        YQL_LOG(INFO) << "User affiliation: " << NProto::EStaffAffiliation_Name(Affiliation);
    }

    void SetGatewaysConfig(const TGatewaysConfig& gatewaysConfig) {
        GatewaysConfig = gatewaysConfig;
        SyncWithStaticGateways(StaticGatewaysConfig, GatewaysConfig);
        UrlPreprocessing->Configure(Affiliation != NProto::EStaffAffiliation::YANDEX, GatewaysConfig);
    }

    void SetFunctionRegistryData(const NProto::TFunctionRegistryData& data) {
        if (!UdfIndex) {
            return;
        }

        ConvertRegistryDataToResources(data);
    }

    NActors::TActorSystem* GetDqActorSystem() {
        if (!DqActorSystemManager) {
            auto actorSystemManager = std::make_unique<TActorSystemManager>(
                Metrics,
                NActors::NLog::PRI_INFO);
            actorSystemManager->Start();
            DqActorSystemManager = std::move(actorSystemManager);
        }
        return DqActorSystemManager->GetActorSystem();
    }

    ICoordinationHelper::TPtr CreateDqCoordinator() const {
        YQL_ENSURE(DqConfig);
        if (!AnyOf(DqConfig->GetYtBackends(), IsCommunalYtBackend)) {
            return CreateUploadCoordinationHelper(HostName());
        }
        auto coordinatorConfig = DqConfig->GetYtCoordinator();
        if (!coordinatorConfig.HasProxyAddress()) {
            *coordinatorConfig.MutableProxyAddress() = coordinatorConfig.GetClusterName();
        }
        NProto::TDqConfig::TScheduler scheduler;
        auto coordinator = CreateCoordiantionHelper(
            coordinatorConfig,
            scheduler,
            "yqlworker_executor",
            0,
            "localhost",
            "127.0.0.1");
        coordinator->GetNodeId(
            TMaybe<ui32>(static_cast<ui32>(NDqs::ENodeIdLimits::MinServiceNodeId)),
            Nothing(),
            static_cast<ui32>(NDqs::ENodeIdLimits::MinServiceNodeId),
            static_cast<ui32>(NDqs::ENodeIdLimits::MaxServiceNodeId),
            {});
        return coordinator;
    }

    ICoordinationHelper::TPtr GetDqCoordinator() {
        if (!DqCoordinator) {
            DqCoordinator = CreateDqCoordinator();
        }
        return DqCoordinator;
    }

    void InitGateways(TTaskDataPtr taskData, const NProto::TFunctionRegistryData* data) {
        const bool disableAnonymousAccess = (Affiliation != NProto::EStaffAffiliation::YANDEX);

        DataProvidersInit.push_back(GetPgDataProviderInitializer());
        GetClusterMappingFromGateways(GatewaysConfig, Clusters);

        if (taskData && taskData->HasDefaultTranslationCluster()) {
            DefaultTranslationCluster = taskData->GetDefaultTranslationCluster();
        }

        // (1) YT gateway
        if (GatewaysConfig.HasYt()) {
            if (disableAnonymousAccess) {
                // external users and robots cannot use our internal tokens
                EraseSystemYtTokens(*GatewaysConfig.MutableYt());
            }

            // do not use token from .yt/token or env
            NYT::TConfig::Get()->Token = {};

            TYtNativeServices services;
            services.FunctionRegistry = FuncRegistry.Get();
            services.FileStorage = FileStorage;
            services.Config = std::make_shared<TYtGatewayConfig>(GatewaysConfig.GetYt());
            services.StaticConfig = std::make_shared<TYtStaticGatewayConfig>(StaticGatewaysConfig.GetYt());
            services.DisableAnonymousClusterAccess = disableAnonymousAccess;
            services.Metrics = Metrics;
            services.SecretMasker = ExtCreateSecretMasker();
            services.TvmClient = CreateTvmClient(ExecutorConfig.GetYtTvm());
            services.YtAccessProvider = CreateYtAccessProvider(services.TvmClient, ExecutorConfig.GetYtAccessProvider());
            services.YtTokenResolver = CreateYtTokenResolver(ExecutorConfig.GetYtTokenResolver());

            bool isMixedGateway = ExecutorConfig.GetGatewayType() == NProto::TGatewayType::MIXED;

            IYtGateway::TPtr ytGateway = isMixedGateway
                ? CreateYtMixedGateway(services)
                : CreateYtNativeGateway(services);

            if (YQL_CLOG_ACTIVE(TRACE, Perf)) {
                ytGateway = CreateYtProfilingGateway(ytGateway);
            }
            TString fmrJobBinaryPath = ExecutorConfig.HasFmr() ? ExecutorConfig.GetFmr().GetFmrJobPath() : TString{};
            TString fmrJobBinaryMd5 = ExecutorConfig.HasFmr() ? ExecutorConfig.GetFmr().GetFmrJobPathMd5() : TString{};
            auto gateway = CreateFmrGatewayFromYtIfNeeded(ytGateway, GatewaysConfig, StaticGatewaysConfig, taskData, FuncRegistry.Get(),
                FileStorage, isMixedGateway, fmrJobBinaryPath, fmrJobBinaryMd5);
            YQL_ENSURE(gateway);
            Gateways.push_back(gateway);
            DataProvidersInit.push_back(GetYtNativeDataProviderInitializer(gateway, NDq::MakeCBOOptimizerFactory(), MakeDqHelper()));
        }

        const NYdb::TDriverConfig stub;
        const NYdb::TDriver driver(stub);

        if (DqConfig) {
            auto dqGateway = CreateDqGateway(*DqConfig);

            TVector<NProto::TDqConfig_TYtBackend> cliqueBackends;
            if (DqConfig->GetEnableClique() && !DqConfig->GetYtBackends().empty()) {
                cliqueBackends = NormalizeYtBackendCredentials(*DqConfig);
            }

            if (!cliqueBackends.empty()) {
                const auto resolveYtCluster = MakeYtBackendResolver(cliqueBackends);
                dqGateway = CreateDqCliqueRoutingGateway(
                    dqGateway,
                    resolveYtCluster);

                const auto& ytBackend = cliqueBackends[0];
                TResourceManagerOptions uploadOpts;
                uploadOpts.YtBackend = ytBackend;
                uploadOpts.UploadPrefix = GetDqCliqueUploadPrefix(ytBackend);

                auto resolveUploadCluster = [cliqueBackends] (
                    TResourceManagerOptions* options,
                    const TDqSettings::TPtr& settings)
                {
                    if (!settings || !settings->Clique.Get()) {
                        return false;
                    }
                    ConfigureDqCliqueUploadOptions(cliqueBackends, *settings->Clique.Get(), options);
                    return true;
                };

                dqGateway = CreateDqGatewayWithUploader(
                    std::move(dqGateway),
                    GetDqActorSystem(),
                    std::move(uploadOpts),
                    GetDqCoordinator(),
                    std::move(resolveUploadCluster));
            }

            Gateways.push_back(dqGateway);

            TVector<TComputationNodeFactory> dqFactories = {GetCommonDqFactory(), GetDqYtFactory()};
            ExtDqFactories(dqFactories, driver);
            dqFactories.push_back(NKikimr::NMiniKQL::GetYqlFactory());
            dqFactories.push_back(GetPgFactory());
            auto dqCompFactory = NKikimr::NMiniKQL::GetCompositeWithBuiltinFactory(std::move(dqFactories));

            TDqCliqueValidator cliqueValidator;
            if (!cliqueBackends.empty()) {
                cliqueValidator = [cliqueBackends] (const TString& value) {
                    ValidateDqCliqueYtBackend(cliqueBackends, value);
                };
            }

            DataProvidersInit.push_back(GetDqDataProviderInitializer(
                &CreateDqExecTransformer,
                dqGateway,
                dqCompFactory,
                Metrics,
                FileStorage,
                disableAnonymousAccess,
                std::move(cliqueValidator)));
        }

        auto credentialsFactory = CreateStructuredTokenCredentialsFactory();

        // (2) all others (KIKIMR / RTMR / STAT)
        ExtProviderSpecific(
            GatewaysConfig, StaticGatewaysConfig, FuncRegistry.Get(), Gateways, DataProvidersInit,
            Metrics, Clusters, ClusterPathPrefixes, disableAnonymousAccess, FileStorage,
            driver, credentialsFactory
        );

        SqlFlags = SampleSQLFlags(GatewaysConfig, taskData);
        ExtAddTranslators(ExecutorConfig.GetSparkParserPort(), TranslatorsRegistry);

        InitModuleResolver(data);
    }

    void InitModuleResolver(const NProto::TFunctionRegistryData* data) {
        LoadYqlDefaultMounts(UserData);
        ExtUserMounts(UserData);

        if (ExecutorConfig.HasMountConfig()) {
            FillUserDataTableFromFileSystem(ExecutorConfig.GetMountConfig(), UserData);
        }

        auto sqlFlags = SqlFlags.ToMap();
        NSQLTranslation::TTranslationSettings settings;
        NSQLTranslation::ParseTranslationSettings(sqlFlags, settings);

        NSQLTranslationV1::TLexers lexers;
        lexers.Antlr4 = NSQLTranslationV1::MakeAntlr4LexerFactory();
        lexers.Antlr4Ansi = NSQLTranslationV1::MakeAntlr4AnsiLexerFactory();
        NSQLTranslationV1::TParsers parsers;
        parsers.Antlr4 = NSQLTranslationV1::MakeAntlr4ParserFactory(
            /*isAmbiguityError=*/true,
            /*isAmbiguityDebugging=*/false,
            /*maxParseTreeDepth=*/settings.MaxParseTreeDepth);
        parsers.Antlr4Ansi = NSQLTranslationV1::MakeAntlr4AnsiParserFactory(
            /*isAmbiguityError=*/true,
            /*isAmbiguityDebugging=*/false,
            /*maxParseTreeDepth=*/settings.MaxParseTreeDepth);

        NSQLTranslation::TTranslators translators(
            nullptr,
            NSQLTranslationV1::MakeTranslator(lexers, parsers),
            NSQLTranslationPG::MakeTranslator()
        );

        TModulesTable modules;
        if (CompileLibraries(translators, UserData, LibrariesContext, modules)) {
            ModuleResolver = std::make_shared<TModuleResolver>(translators, std::move(modules),
                LibrariesContext.NextUniqueId, Clusters, sqlFlags);
        } else {
            ythrow yexception() << "Libraries compilation error: "
                << LibrariesContext.IssueManager.GetIssues().ToString();
        }

        // it is safe to Link mount files
        if (!ModuleResolver->Link(LibrariesContext)) {
            ythrow yexception() << "Failed to link libraries, error: " << LibrariesContext.IssueManager.GetIssues().ToString();
        }

        if (data) {
            // cannot Link here since we have at most 2 versions for single lib
            for (auto& pkg : data->GetPackages()) {
                ModuleResolver->RegisterPackage(pkg.GetName());
                for (auto& p : pkg.GetLibraries()) {
                    if (!ModuleResolver->AddFromMemory(p.GetName(), p.GetContent(), LibrariesContext, 1, p.GetVersion())) {
                        ythrow yexception() << "Library " << p.GetName() << ", version " << p.GetVersion() << " compilation error: " << LibrariesContext.IssueManager.GetIssues().ToString();
                    }
                }
            }
        }

        FreezeGuardHolder = MakeHolder<TExprContext::TFreezeGuard>(LibrariesContext);
    }

    void SetMetricsDecorator(TMetricsDecorator decorator) {
        MetricsDecorator = std::move(decorator);
    }

    TProgramFactoryPtr NewProgramFactory(const TString& runner) {
        // TODO (jamel): make TProgramFactory thread-safe
        auto progFactory = MakeIntrusive<TProgramFactory>(
                    !!GetEnv("YQL_DETERMINISTIC_MODE"),
                    FuncRegistry.Get(), LibrariesContext.NextUniqueId,
                    DataProvidersInit, runner);

        progFactory->SetIssueReportTarget(" to YQL (st/YQLSUPPORT)");
        progFactory->SetUdfIndex(UdfIndex, UdfIndexPackageSet);
        progFactory->SetGatewaysConfig(&GatewaysConfig);
        progFactory->AddUserDataTable(UserData);
        progFactory->SetModules(ModuleResolver);
        progFactory->SetUrlListerManager(UrlListerManager);
        progFactory->SetUdfResolver(UdfResolver);
        progFactory->SetFileStorage(FileStorage);
        progFactory->SetUrlPreprocessing(UrlPreprocessing);
        progFactory->SetTranslatorsRegistry(TranslatorsRegistry);
        if (!UdfBridgePath.empty()) {
            progFactory->SetUdfBridgeBinaryPath(UdfBridgePath);
        }
        progFactory->EnableAutoUseYqlLibs();
        if (UdfResolverAnalyticsLogfile) {
            progFactory->SetUdfResolverLogfile(*UdfResolverAnalyticsLogfile);
        }
        return progFactory;
    }

    IYqlRunnableTaskPtr Create(
            NProto::ETaskAction action,
            TTaskDataPtr data,
            ITaskWatcherPtr watcher)
    {
        auto userMetrics = MetricsDecorator(Metrics, data->GetUsername());
        auto runner = data->HasRunner() ? data->GetRunner() : "yql-service";

        switch (data->GetSyntax()) {
            case NProto::ESyntax::UDF_REGISTRY_SNAPSHOT:
                return CreateUdfSnapshotHandlerTask(std::move(data), std::move(watcher), std::move(userMetrics),  DqControlFactory, FileStorage);
            case NProto::ESyntax::UDF_META:
                return CreateUdfLibraryMetaTask(std::move(data), std::move(watcher), std::move(userMetrics), UdfResolver, FileStorage, UrlPreprocessing);
            case NProto::ESyntax::LIBRARY_META:
                return CreateSqlLibraryMetaTask(std::move(data), std::move(watcher), std::move(userMetrics), ModuleResolver);
            default:
                return MakeIntrusive<TYqlRunnableTask>(
                    NewProgramFactory(runner),
                    Clusters,
                    ClusterPathPrefixes,
                    DefaultTranslationCluster,
                    action,
                    std::move(data),
                    std::move(watcher),
                    Metrics,
                    std::move(userMetrics),
                    GatewaysConfig,
                    SqlFlags,
                    DefaultCredentials,
                    QPlayerDir,
                    QWriterSettings,
                    AstMemoryLimit
                );
        }
    }

private:
    void EraseSystemYtTokens(TYtGatewayConfig& ytConfig) {
        for (size_t i = 0; i < ytConfig.ClusterMappingSize(); ++i) {
            ytConfig.MutableClusterMapping(i)->ClearYTToken();
        }
    }

    void LoadSystemUdfsIntoFunctionRegistry(const TString& udfResolverPath, const TString& systemUdfsDir) {
        TUdfModulePathsMap systemModules;
        NCommon::LoadSystemModulePaths(udfResolverPath, systemUdfsDir, &systemModules);

        TUdfModulePathsMap filteredModules;
        for (auto& module : systemModules) {
            if (!FuncRegistry->IsLoadedUdfModule(module.first)) {
                filteredModules.insert({ module.first, FreezeFile(module.second) });
            }
        }

        auto mutableFuncRegistry = FuncRegistry->Clone();
        mutableFuncRegistry->SetSystemModulePaths(filteredModules);
        FuncRegistry = mutableFuncRegistry;
    }

    void LoadSystemUdfsIntoUdfIndex(const TString& systemUdfsDir, THoldingFileStorage& storage) {
        TVector<TString> systemUdfPaths;
        FindUdfsInDir(systemUdfsDir, &systemUdfPaths);

        TMap<TString, TString> frozenSystemUdfWithMd5;
        for (auto& p : systemUdfPaths) {
            if (HasFrozenFile(p)) {
                continue;
            }

            auto link = FreezeFileEx(p);
            frozenSystemUdfWithMd5.emplace(link->GetPath(), link->GetMd5());
        }

        Y_ENSURE(UdfResolver);
        Y_ENSURE(UdfIndex);
        const bool isTrusted = false;
        LoadRichMetadataToUdfIndex(*UdfResolver, frozenSystemUdfWithMd5, {}, isTrusted, TUdfIndex::EOverrideMode::PreserveExisting, *UdfIndex, storage);
    }

    // Puts file in file storage and returns path to a hardlinked copy.
    // This is need to keep files in consistent state with currently running
    // FORKER process (and as well as children TASK processes) because files
    // can be updated in the middle of running operation (e.g. new package is
    // deployed).
    TString FreezeFile(const TString& path) {
        return FreezeFileEx(path)->GetPath();
    }

    TFileLinkPtr FreezeFileEx(const TString& path) {
        if (InterruptionPointFunc) {
            InterruptionPointFunc();
        }

        TFileLinkPtr& fileLink = FrozenFiles[path];
        if (!fileLink) {
            fileLink = FileStorage->PutFile(path, TFsPath(path).GetName()); // Keep original name

            if (NFs::Exists(path + ".debug")) {
                FrozenFiles[path + ".debug"] = FileStorage->PutFile(path + ".debug", fileLink->GetPath().GetName() + ".debug");
            }
        }

        return fileLink;
    }

    bool HasFrozenFile(const TString& path) const {
        return FrozenFiles.contains(path);
    }

    void ConvertRegistryDataToResources(const NProto::TFunctionRegistryData& data) {
        for (auto& p : data.GetPackages()) {
            UdfIndexPackageSet->RegisterPackage(p.GetName());

            for (auto& r : p.GetResources()) {
                auto info = MakeIntrusive<TResourceInfo>();
                info->IsTrusted = false;
                info->Link = TDownloadLink::Url(r.GetUrl());

                for (auto& m : r.GetModules()) {
                    info->Modules.insert(m.GetName());

                    for (auto& f : m.GetFunctions()) {
                        TFunctionInfo newFunction;
                        newFunction.Name = f.GetName();
                        newFunction.IsTypeAwareness = f.GetIsTypeAwareness();
                        newFunction.ArgCount = f.GetArgCount();
                        newFunction.OptionalArgCount = f.GetOptionalArgCount();
                        if (f.HasCallableType()) {
                            newFunction.CallableType = f.GetCallableType();
                        }

                        if (f.HasRunConfigType()) {
                            newFunction.RunConfigType = f.GetRunConfigType();
                        }

                        info->Functions.emplace(newFunction.Name, newFunction);
                    }
                }

                UdfIndexPackageSet->RegisterResource(p.GetName(), r.GetVersion(), info);
            }
        }
    }

    static TGatewaySQLFlags SampleSQLFlags(const TGatewaysConfig& config, const TTaskDataPtr& task) {
        TMaybe<TString> username;
        if (task && task->HasUsername()) {
            username = task->GetUsername();
        }

        std::unordered_set<std::string_view> groups;
        bool isRobot = false;
        if (task) {
            const auto& allowed = task->GetGroupAllowed();
            groups.insert(begin(allowed), end(allowed));
            isRobot = IsRobot(*task);
        }

        return TGatewaySQLFlags::From(config, [&](const TActivationPercentage& activation) {
            // Username is actally absent only in testing environment
            return !username || NConfig::Allow(activation, *username, isRobot, groups);
        });
    }
};

//////////////////////////////////////////////////////////////////////////////
// TYqlRunnableTaskFactory
//////////////////////////////////////////////////////////////////////////////
TYqlRunnableTaskFactory::TYqlRunnableTaskFactory(
        const TGatewaysConfig& gatewaysConfig,
        const TStaticGatewaysConfig& staticGatewaysConfig,
        const TFileStorageConfig& fsConfig,
        const NProto::TTaskExecutorConfig& executroConfig,
        const TMaybe<NProto::TDqConfig>& dqConfig,
        IMetricsRegistryPtr metrics,
        std::function<void()> interruptionPointFunc)
    : Impl_(new TImpl(gatewaysConfig, staticGatewaysConfig, fsConfig, executroConfig, dqConfig, metrics, interruptionPointFunc))
{
}

TYqlRunnableTaskFactory::~TYqlRunnableTaskFactory()
{
}

void TYqlRunnableTaskFactory::InitGateways(TTaskDataPtr taskData, const NProto::TFunctionRegistryData* data) {
    Impl_->InitGateways(taskData, data);
}

void TYqlRunnableTaskFactory::SetUserAffiliation(NProto::EStaffAffiliation affiliation) {
    Impl_->SetUserAffiliation(affiliation);
}

void TYqlRunnableTaskFactory::SetGatewaysConfig(const TGatewaysConfig& gatewaysConfig) {
    Impl_->SetGatewaysConfig(gatewaysConfig);
}

void TYqlRunnableTaskFactory::SetFunctionRegistryData(const NProto::TFunctionRegistryData& data) {
    Impl_->SetFunctionRegistryData(data);
}

void TYqlRunnableTaskFactory::SetMetricsDecorator(TMetricsDecorator decorator)
{
    Impl_->SetMetricsDecorator(std::move(decorator));
}

IYqlRunnableTaskPtr TYqlRunnableTaskFactory::Create(
        NProto::ETaskAction action, TTaskDataPtr data, ITaskWatcherPtr watcher)
{
    return Impl_->Create(action, data, watcher);
}

} // namespace NYql

template <>
void Out<NYql::TTaskResultChanges>(IOutputStream& out, const NYql::TTaskResultChanges& changes) {
    out << Bin(changes.Mask) << "(";
    bool first = true;
    for (ui32 flag = NYql::TTaskResultChanges::AST; flag <= NYql::TTaskResultChanges::STATISTICS; flag <<= 1) {
        if (flag & changes.Mask) {
            out << (first ? "" : "|") << NYql::NProto::ETaskResultChange_Name(flag);
            first = false;
        }
    }
    out << ")";
}
