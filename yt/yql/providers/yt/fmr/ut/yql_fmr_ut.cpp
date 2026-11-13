#include "execution_mode.h"

#include <yql/essentials/minikql/invoke_builtins/mkql_builtins.h>
#include <yt/yql/providers/yt/fmr/coordinator/yt_coordinator_service/file/yql_yt_file_coordinator_service.h>
#include <yt/yql/providers/yt/fmr/fmr_tool_lib/yql_yt_fmr_initializer.h>
#include <yt/yql/providers/yt/fmr/job_preparer/impl/yql_yt_job_preparer_impl.h>
#include <yt/yql/providers/yt/fmr/test_tools/table_data_service/yql_yt_table_data_service_helpers.h>
#include <yt/yql/providers/yt/provider/yql_yt_provider.h>
#include <yt/yql/providers/yt/provider/yql_yt_forwarding_gateway.h>
#include <yt/yql/providers/yt/gateway/file/yql_yt_file.h>
#include <yt/yql/providers/yt/gateway/file/yql_yt_file_services.h>
#include <yql/essentials/core/cbo/simple/cbo_simple.h>
#include <yql/essentials/core/facade/yql_facade.h>
#include <yql/essentials/providers/common/provider/yql_provider_names.h>
#include <yql/essentials/core/file_storage/file_storage.h>
#include <yql/essentials/core/services/mounts/yql_mounts.h>

#include <library/cpp/yson/node/node_io.h>

#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/testing/unittest/tests_data.h>

#include <yt/yql/providers/yt/fmr/test_utils/yql_yt_file_test_utils.h>

#include <util/stream/file.h>
#include <util/folder/tempdir.h>
#include <util/system/user.h>
#include <util/system/tempfile.h>
#include <util/system/thread.h>

using namespace NYql;
using namespace NYql::NFmr;
using namespace NYql::NFmr::NTests;

namespace {

    EFmrErrorReason GetErrorReason(EInjectedFailure failure) {
        return failure == EInjectedFailure::FallbackOperation
                   ? EFmrErrorReason::FallbackOperation
                   : EFmrErrorReason::RestartOperation;
    }

    class TFaultInjectingCoordinator final: public IFmrCoordinator {
    public:
        TFaultInjectingCoordinator(IFmrCoordinator::TPtr delegate, EExecutionMode mode, TMaybe<EOperationType> operationType, EFmrErrorReason errorReason)
            : Delegate_(std::move(delegate))
            , Mode_(mode)
            , OperationType_(operationType)
            , ErrorReason_(errorReason)
        {
        }

        TFuture<TStartOperationResponse> StartOperation(const TStartOperationRequest& request) override {
            const bool canInjectFailure = CanInjectFailure(request.OperationType);
            return Delegate_->StartOperation(request).Apply([this, canInjectFailure](const auto& future) {
                auto result = future.GetValue();
                if (canInjectFailure && result.Status != EOperationStatus::Failed) {
                    with_lock (Mutex_) {
                        ++OperationCount_;
                        if ((Mode_ == EExecutionMode::FailFirstOperation && OperationCount_ == 1) || (Mode_ == EExecutionMode::FailSecondOperation && OperationCount_ == 2)) {
                            FaultOperations_.insert(result.OperationId);
                        }
                    }
                }
                return result;
            });
        }

        TFuture<TGetOperationResponse> GetOperation(const TGetOperationRequest& request) override {
            return Delegate_->GetOperation(request).Apply([this, operationId = request.OperationId](const auto& future) {
                auto result = future.GetValue();
                with_lock (Mutex_) {
                    if (result.Status == EOperationStatus::Completed && FaultOperations_.contains(operationId)) {
                        // The gateway observes this from its tracking thread, after real asynchronous work.
                        result.Status = EOperationStatus::Failed;
                        result.ErrorMessages = {{
                            .Component = EFmrComponent::Coordinator,
                            .Reason = ErrorReason_,
                            .ErrorMessage = "Injected operation failure",
                        }};
                        InjectedOperations_.insert(operationId);
                    }
                }
                return result;
            });
        }

        size_t GetInjectedOperationCount() {
            with_lock (Mutex_) {
                return InjectedOperations_.size();
            }
        }

        size_t GetExpectedInjectionCount() {
            with_lock (Mutex_) {
                if (Mode_ == EExecutionMode::FailFirstOperation) {
                    return OperationCount_ >= 1;
                }
                if (Mode_ == EExecutionMode::FailSecondOperation) {
                    return OperationCount_ >= 2;
                }
                return 0;
            }
        }

        TFuture<TDeleteOperationResponse> DeleteOperation(const TDeleteOperationRequest& request) override {
            return Delegate_->DeleteOperation(request);
        }

        TFuture<TDropTablesResponse> DropTables(const TDropTablesRequest& request) override {
            return Delegate_->DropTables(request);
        }

        TFuture<THeartbeatResponse> SendHeartbeatResponse(const THeartbeatRequest& request) override {
            return Delegate_->SendHeartbeatResponse(request);
        }

        TFuture<TGetFmrTableInfoResponse> GetFmrTableInfo(const TGetFmrTableInfoRequest& request) override {
            return Delegate_->GetFmrTableInfo(request);
        }

        TFuture<void> ClearSession(const TClearSessionRequest& request) override {
            return Delegate_->ClearSession(request);
        }

        TFuture<TOpenSessionResponse> OpenSession(const TOpenSessionRequest& request) override {
            return Delegate_->OpenSession(request);
        }

        TFuture<TPingSessionResponse> PingSession(const TPingSessionRequest& request) override {
            return Delegate_->PingSession(request);
        }

        TFuture<TListSessionsResponse> ListSessions(const TListSessionsRequest& request) override {
            return Delegate_->ListSessions(request);
        }

        TFuture<TPrepareOperationResponse> PrepareOperation(const TPrepareOperationRequest& request) override {
            return Delegate_->PrepareOperation(request);
        }

        TFuture<TWaitForOperationsResponse> WaitForOperations(const TWaitForOperationsRequest& request) override {
            return Delegate_->WaitForOperations(request);
        }

        TFuture<TWaitForTasksResponse> WaitForTasks(const TWaitForTasksRequest& request) override {
            return Delegate_->WaitForTasks(request);
        }

    private:
        bool CanInjectFailure(EOperationType operationType) const {
            if (OperationType_) {
                return operationType == *OperationType_;
            }
            switch (operationType) {
                case EOperationType::Map:
                case EOperationType::Merge:
                case EOperationType::Reduce:
                case EOperationType::MapReduce:
                case EOperationType::Fill:
                case EOperationType::Sort:
                case EOperationType::SortedMerge:
                case EOperationType::Pull:
                    return true;
                default:
                    return false;
            }
        }

        const IFmrCoordinator::TPtr Delegate_;
        const EExecutionMode Mode_;
        const TMaybe<EOperationType> OperationType_;
        const EFmrErrorReason ErrorReason_;
        TMutex Mutex_;
        size_t OperationCount_ = 0;
        THashSet<TString> FaultOperations_;
        THashSet<TString> InjectedOperations_;
    };

    class TThreadCheckingGateway final: public TYtForwardingGatewayBase {
    public:
        explicit TThreadCheckingGateway(IYtGateway::TPtr slave)
            : TYtForwardingGatewayBase(std::move(slave))
        {
        }

        TFuture<TRunResult> Run(const TExprNode::TPtr& node, TExprContext& ctx, TRunOptions&& options) override {
            CheckThread();
            FallbackCallCount_ += options.SkipFmr();
            return Slave_->Run(node, ctx, std::move(options));
        }

        ui32 GetFallbackCallCount() const {
            return FallbackCallCount_;
        }

        TFuture<TPublishResult> Publish(const TExprNode::TPtr& node, TExprContext& ctx, TPublishOptions&& options) override {
            CheckThread();
            FallbackCallCount_ += options.SkipFmr();
            return Slave_->Publish(node, ctx, std::move(options));
        }

        TFuture<TResOrPullResult> ResOrPull(const TExprNode::TPtr& node, TExprContext& ctx, TResOrPullOptions&& options) override {
            CheckThread();
            FallbackCallCount_ += options.SkipFmr();
            return Slave_->ResOrPull(node, ctx, std::move(options));
        }

        TFuture<TCalcResult> Calc(const TExprNode::TListType& nodes, TExprContext& ctx, TCalcOptions&& options) override {
            CheckThread();
            return Slave_->Calc(nodes, ctx, std::move(options));
        }

    private:
        void CheckThread() const {
            UNIT_ASSERT_C(TThread::CurrentThreadId() == OwnerThread_,
                          "Expression nodes passed to gateway on thread " << TThread::CurrentThreadId()
                                                                          << ", expected query thread " << OwnerThread_);
        }

        const TThread::TId OwnerThread_ = TThread::CurrentThreadId();
        ui32 FallbackCallCount_ = 0;
    };

} // namespace

constexpr TStringBuf InputData =
    "{\"key\"=\"075\";\"subkey\"=\".\";\"value\"=\"abc\"};\n"
    "{\"key\"=\"800\";\"subkey\"=\".\";\"value\"=\"ddd\"};\n"
    "{\"key\"=\"020\";\"subkey\"=\".\";\"value\"=\"q\"};\n"
    "{\"key\"=\"150\";\"subkey\"=\".\";\"value\"=\"qzz\"};\n"sv;

template <typename F>
TString WithTables(const F&& f, TStringBuf inputData = InputData) {
    static const TStringBuf KSV_ATTRS =
        "{\"_yql_row_spec\" = {\"Type\" = [\"StructType\";["
        "[\"key\";[\"DataType\";\"String\"]];"
        "[\"subkey\";[\"DataType\";\"String\"]];"
        "[\"value\";[\"DataType\";\"String\"]]"
        "]]}}";

    TTempFileHandle inputFile, outputFile;
    TTempFileHandle inputFileAttrs(inputFile.Name() + ".attr");

    inputFile.Write(inputData.data(), inputData.size());
    inputFile.FlushData();
    inputFileAttrs.Write(KSV_ATTRS.data(), KSV_ATTRS.size());
    inputFileAttrs.FlushData();

    THashMap<TString, TString> tables;
    tables["yt.plato.Input"] = inputFile.Name();
    tables["yt.plato.Output"] = outputFile.Name();
    f(tables);

    const TString content = NYql::NFmr::ReadFileWithSplittedSupport(outputFile.Name());
    TStringInput contentStream(content);
    NYT::TNode ysonSqlResult = NYT::NodeFromYsonStream(&contentStream, NYT::NYson::EYsonType::ListFragment);
    if (NFs::Exists(outputFile.Name() + ".attr")) {
        const auto attributes = NYT::NodeFromYsonString(TFileInput(outputFile.Name() + ".attr").ReadAll());
        if (attributes.HasKey("row_count")) {
            UNIT_ASSERT_VALUES_EQUAL(attributes["row_count"].AsInt64(), ysonSqlResult.AsList().size());
        }
    }
    return NYT::NodeToCanonicalYsonString(ysonSqlResult);
}

struct TRunSettings {
    bool IsSql = true;
    bool IsPg = false;
    THashMap<TString, TString> Tables;
    EExecutionMode ExecutionMode = EExecutionMode::Fmr;
    EFmrErrorReason ErrorReason = EFmrErrorReason::FallbackOperation;
    TStringBuf ExpectedError;
    TMaybe<EOperationType> FallbackOperationType;
    TMaybe<ui32> ExpectedInjectedOperations;
    TString* QueryResults = nullptr;
};

bool RunProgram(const TString& query, const TRunSettings& runSettings) {
    auto functionRegistry = NKikimr::NMiniKQL::CreateFunctionRegistry(NKikimr::NMiniKQL::CreateBuiltinRegistry());
    auto yqlNativeServices = NFile::TYtFileServices::Make(functionRegistry.Get(), runSettings.Tables, {}, "");
    auto checkingGateway = MakeIntrusive<TThreadCheckingGateway>(CreateYtFileGateway(yqlNativeServices));
    IYtGateway::TPtr ytGateway = checkingGateway;

    auto fmrServices = MakeIntrusive<NFmr::TFmrServices>();
    fmrServices->FunctionRegistry = functionRegistry.Get();
    fmrServices->JobLauncher = MakeIntrusive<NFmr::TFmrUserJobLauncher>(NFmr::TFmrUserJobLauncherOptions{.RunInSeparateProcess = false});
    fmrServices->DisableLocalFmrWorker = false;
    fmrServices->YtJobService = NFmr::MakeFileYtJobService();
    fmrServices->YtCoordinatorService = NFmr::MakeFileYtCoordinatorService();
    fmrServices->NeedToTransformTmpTablePaths = false;

    TTempFileHandle discoveryFile;
    TPortManager pm;
    const ui16 port = pm.GetPort();
    SetupTableDataServiceDiscovery(discoveryFile, port);
    auto tableDataServiceServer = MakeTableDataServiceServer(port);
    fmrServices->TableDataServiceDiscoveryFilePath = discoveryFile.Name();

    TFileStorageConfig fsConfig;
    fsConfig.SetThreads(3);
    auto fileStorage = WithAsync(CreateFileStorage(fsConfig, {}));

    fmrServices->FileStorage = fileStorage;
    auto jobPreparer = NFmr::MakeFmrJobPreparer(fileStorage, NFmr::MakeFileTableDataServiceDiscovery({.Path = discoveryFile.Name()}));
    fmrServices->JobPreparer = jobPreparer;

    auto coordinator = MakeIntrusive<TFaultInjectingCoordinator>(
        MakeFmrCoordinator(GetDefaultCoordinatorSettings(), fmrServices->YtCoordinatorService), runSettings.ExecutionMode,
        runSettings.FallbackOperationType, runSettings.ErrorReason);
    auto workerSettings = GetDefaultWorkerSettings();
    workerSettings.WorkerId = 0;
    workerSettings.JobFactorySettings.Function = [fmrServices](TTask::TPtr task, std::shared_ptr<std::atomic<bool>> cancelFlag) {
        auto discovery = MakeFileTableDataServiceDiscovery({.Path = fmrServices->TableDataServiceDiscoveryFilePath});
        return RunJob(task, discovery, Nothing(), fmrServices->YtJobService, fmrServices->JobLauncher, cancelFlag);
    };
    auto worker = MakeFmrWorker(coordinator, MakeFmrJobFactory(workerSettings.JobFactorySettings), jobPreparer, workerSettings);
    worker->Start();
    auto fmrGateway = CreateYtFmrGateway(ytGateway, coordinator, fmrServices);

    TVector<TDataProviderInitializer> dataProvidersInit;
    dataProvidersInit.push_back(GetYtNativeDataProviderInitializer(fmrGateway, MakeSimpleCBOOptimizerFactory(), {}));

    TExprContext modulesCtx;
    IModuleResolver::TPtr moduleResolver;
    if (!GetYqlDefaultModuleResolver(modulesCtx, moduleResolver)) {
        Cerr << "Errors loading default YQL libraries:" << Endl;
        modulesCtx.IssueManager.GetIssues().PrintTo(Cerr);
        return false;
    }
    TExprContext::TFreezeGuard freezeGuard(modulesCtx);

    TProgramFactory factory(false, functionRegistry.Get(), 0ULL, dataProvidersInit, "ut");
    factory.SetModules(moduleResolver);
    TProgramPtr program = factory.Create("-stdin-", query, "", EHiddenMode::Disable);

    if (runSettings.IsSql || runSettings.IsPg) {
        NSQLTranslation::TTranslationSettings settings;
        settings.PgParser = runSettings.IsPg;
        settings.ClusterMapping["plato"] = TString(YtProviderName);

        if (!program->ParseSql(settings)) {
            program->PrintErrorsTo(Cerr);
            return false;
        }
    }

    if (!program->Compile(GetUsername())) {
        program->PrintErrorsTo(Cerr);
        return false;
    }

    TProgram::TStatus status = program->Run(GetUsername(), nullptr, nullptr, nullptr);
    if (status == TProgram::TStatus::Error) {
        program->PrintErrorsTo(Cerr);
    }
    if (!runSettings.ExpectedError.empty()) {
        UNIT_ASSERT_VALUES_EQUAL(status, TProgram::TStatus::Error);
        UNIT_ASSERT_STRING_CONTAINS(program->Issues().ToString(), runSettings.ExpectedError);
    }
    if (runSettings.QueryResults) {
        *runSettings.QueryResults = program->ResultsAsString();
    }
    if (runSettings.ExpectedInjectedOperations) {
        UNIT_ASSERT_VALUES_EQUAL(coordinator->GetInjectedOperationCount(), *runSettings.ExpectedInjectedOperations);
    }
    const auto expectedFallbacks = runSettings.ErrorReason == EFmrErrorReason::FallbackOperation
                                       ? coordinator->GetInjectedOperationCount()
                                       : 0;
    UNIT_ASSERT_VALUES_EQUAL(checkingGateway->GetFallbackCallCount(), expectedFallbacks);
    UNIT_ASSERT_VALUES_EQUAL(coordinator->GetInjectedOperationCount(), coordinator->GetExpectedInjectionCount());
    return status == TProgram::TStatus::Ok;
}

void CheckPublishMergeFallback(bool sorted, EExecutionMode mode, EInjectedFailure failure) {
    const TString orderBy = sorted ? " ORDER BY key" : "";
    const TString query = TStringBuilder() << R"(
        USE plato;
        PRAGMA DqEngine = 'disable';
        INSERT INTO @stage WITH TRUNCATE SELECT key || '_a' AS key FROM Input
    )" << orderBy << R"(;
        INSERT INTO @stage SELECT key || '_b' AS key FROM Input
    )" << orderBy << R"(;
        COMMIT;
        INSERT INTO Output WITH TRUNCATE SELECT * FROM @stage;
    )";
    const auto result = WithTables([&](const auto& tables) {
        UNIT_ASSERT(RunProgram(query, {
                                          .Tables = tables,
                                          .ExecutionMode = mode,
                                          .ErrorReason = GetErrorReason(failure),
                                          .FallbackOperationType = sorted ? EOperationType::SortedMerge : EOperationType::Merge,
                                          .ExpectedInjectedOperations = mode == EExecutionMode::FailFirstOperation ? MakeMaybe<ui32>(1) : Nothing(),
                                      }));
    }, InputData.Before('\n'));
    const auto rows = NYT::NodeFromYsonString(result);
    THashSet<TString> keys;
    for (const auto& row : rows.AsList()) {
        keys.insert(row["key"].AsString());
    }
    const THashSet<TString> expected = {"075_a", "075_b"};
    UNIT_ASSERT_VALUES_EQUAL(rows.AsList().size(), expected.size());
    UNIT_ASSERT_VALUES_EQUAL(keys, expected);
}

Y_UNIT_TEST_SUITE(FastMapReduceTests) {
    Y_UNIT_TEST(SnapshotFailureDoesNotPublishPath) {
        auto directory = TTempDir::NewTempDir(".");
        const TString source = directory.Path() / "input";
        auto services = NFile::TYtFileServices::Make(nullptr, {{"yt.plato.Input", source}}, {}, directory.Name());
        UNIT_ASSERT_EXCEPTION(services->SnapshotTable(source, "plato", "Input", 0), TFileError);
        UNIT_ASSERT_VALUES_EQUAL(services->GetTableSnapshotPath("plato", "Input", false), source);
        {
            TFileOutput output(source);
            output << InputData;
        }
        const auto snapshot = services->SnapshotTable(source, "plato", "Input", 0);
        UNIT_ASSERT_VALUES_EQUAL(TFileInput(snapshot).ReadAll(), InputData);
        UNIT_ASSERT_VALUES_EQUAL(services->SnapshotTable(source, "plato", "Input", 0), snapshot);
        auto otherServices = NFile::TYtFileServices::Make(nullptr, {}, {}, directory.Name());
        const auto otherSnapshot = otherServices->SnapshotTable(source, "plato", "Input", 0);
        UNIT_ASSERT(snapshot != otherSnapshot);
        UNIT_ASSERT_VALUES_EQUAL(TFileInput(otherSnapshot).ReadAll(), InputData);
    }

    Y_UNIT_TEST(FatalCoordinatorErrors) {
        const TString query = R"(
            USE plato;
            PRAGMA DqEngine = 'disable';
            INSERT INTO Output WITH TRUNCATE SELECT key || '_suffix' AS key FROM Input;
        )";
        for (const auto reason : {EFmrErrorReason::Unknown, EFmrErrorReason::RestartQuery, EFmrErrorReason::UdfTerminate, EFmrErrorReason::WorkerOOM}) {
            WithTables([&](const auto& tables) {
                UNIT_ASSERT(!RunProgram(query, {
                                                   .Tables = tables,
                                                   .ExecutionMode = EExecutionMode::FailFirstOperation,
                                                   .ErrorReason = reason,
                                                   .ExpectedError = "Injected operation failure",
                                                   .ExpectedInjectedOperations = 1,
                                               }));
            }, InputData.Before('\n'));
        }
    }

    Y_UNIT_TEST(PublishMergeFallback, EExecutionMode, EInjectedFailure) {
        CheckPublishMergeFallback(false, Arg<0>(), Arg<1>());
    }

    Y_UNIT_TEST(SortedPublishMergeFallback, EExecutionMode, EInjectedFailure) {
        CheckPublishMergeFallback(true, Arg<0>(), Arg<1>());
    }

    Y_UNIT_TEST(PullFallback, EExecutionMode, EInjectedFailure) {
        const TString query = R"(
            USE plato;
            PRAGMA DqEngine = 'disable';
            SELECT key || '_suffix' AS key FROM Input;
        )";
        TString results;
        WithTables([&](const auto& tables) {
            UNIT_ASSERT(RunProgram(query, {
                                              .Tables = tables,
                                              .ExecutionMode = Arg<0>(),
                                              .ErrorReason = GetErrorReason(Arg<1>()),
                                              .ExpectedInjectedOperations = Arg<0>() == EExecutionMode::Fmr ? 0u : 1u,
                                              .QueryResults = &results,
                                          }));
        }, InputData.Before('\n'));
        const auto blocks = NYT::NodeFromYsonString(results);
        const auto& rows = blocks[0]["Write"][0]["Data"];
        UNIT_ASSERT_VALUES_EQUAL(rows.AsList().size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(rows[0][0].AsString(), "075_suffix");
    }

    Y_UNIT_TEST(UploadFailureIsReported) {
        const TString query = R"(
            USE plato;
            PRAGMA DqEngine = 'disable';
            PRAGMA yt.FmrOperationSpec = '{"partition"={"fmr_table"={"max_parts"=0;};};}';
            INSERT INTO @stage SELECT key || '_suffix' AS key FROM Input;
            COMMIT;
            INSERT INTO Output WITH TRUNCATE SELECT * FROM @stage;
        )";
        WithTables([&](const auto& tables) {
            UNIT_ASSERT(!RunProgram(query, {.Tables = tables, .ExpectedError = "Failed to partition fmr input tables into tasks"}));
        }, InputData.Before('\n'));
    }

    Y_UNIT_TEST(PublishRunsOnQueryThread, EExecutionMode, EInjectedFailure) {
        const TString query = R"(
            USE plato;
            PRAGMA DqEngine = 'disable';
            INSERT INTO Output WITH TRUNCATE SELECT key || '_suffix' AS key FROM Input;
        )";
        const auto result = WithTables([&](const auto& tables) {
            UNIT_ASSERT(RunProgram(query, {.Tables = tables, .ExecutionMode = Arg<0>(), .ErrorReason = GetErrorReason(Arg<1>())}));
        }, InputData.Before('\n'));
        const auto rows = NYT::NodeFromYsonString(result);
        UNIT_ASSERT_VALUES_EQUAL(rows.AsList().size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(rows[0]["key"].AsString(), "075_suffix");
    }

    Y_UNIT_TEST(FallbackRunsOnQueryThread, EExecutionMode, EInjectedFailure) {
        const TString query = R"(
            USE plato;
            PRAGMA DqEngine = 'disable';
            INSERT INTO Output WITH TRUNCATE SELECT key || '_suffix' AS key FROM Input;
        )";
        const auto result = WithTables([&](const auto& tables) {
            UNIT_ASSERT(RunProgram(query, {.Tables = tables, .ExecutionMode = Arg<0>(), .ErrorReason = GetErrorReason(Arg<1>())}));
        });
        const auto rows = NYT::NodeFromYsonString(result);
        THashSet<TString> keys;
        for (const auto& row : rows.AsList()) {
            keys.insert(row["key"].AsString());
        }
        const THashSet<TString> expected = {"075_suffix", "800_suffix", "020_suffix", "150_suffix"};
        UNIT_ASSERT_VALUES_EQUAL(rows.AsList().size(), expected.size());
        UNIT_ASSERT_VALUES_EQUAL(keys, expected);
    }

    Y_UNIT_TEST(FallbackAfterUploadRunsOnQueryThread, EExecutionMode, EInjectedFailure) {
        const TString query = R"(
            USE plato;
            PRAGMA DqEngine = 'disable';
            INSERT INTO @stage SELECT key || '_suffix' AS key FROM Input;
            COMMIT;
            INSERT INTO Output WITH TRUNCATE SELECT key || '_next' AS key FROM @stage;
        )";
        const auto result = WithTables([&](const auto& tables) {
            UNIT_ASSERT(RunProgram(query, {.Tables = tables, .ExecutionMode = Arg<0>(), .ErrorReason = GetErrorReason(Arg<1>())}));
        }, InputData.Before('\n'));
        const auto rows = NYT::NodeFromYsonString(result);
        UNIT_ASSERT_VALUES_EQUAL(rows.AsList().size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(rows[0]["key"].AsString(), "075_suffix_next");
    }

    Y_UNIT_TEST(InsertTmpTable, EExecutionMode, EInjectedFailure) {
        auto query = "use plato; insert into Output with truncate select * from Input";
        TTempFileHandle outputFile;
        auto sqlQueryResult = WithTables([&](const auto& tables) {
            TRunSettings runSettings;
            runSettings.Tables = tables;
            runSettings.ExecutionMode = Arg<0>();
            runSettings.ErrorReason = GetErrorReason(Arg<1>());
            UNIT_ASSERT(RunProgram(query, runSettings));
        });
        auto expected = NYT::NodeToCanonicalYsonString(NYT::NodeFromYsonString(InputData, NYT::NYson::EYsonType::ListFragment));
        UNIT_ASSERT_NO_DIFF(sqlQueryResult, expected);
    }

    Y_UNIT_TEST(InsertTmpTableFiltered, EExecutionMode, EInjectedFailure) {
        auto query = "use plato; insert into Output with truncate select * from Input where Cast(key As Uint32) > 700";
        TTempFileHandle outputFile;
        auto sqlQueryResult = WithTables([&](const auto& tables) {
            TRunSettings runSettings;
            runSettings.Tables = tables;
            runSettings.ExecutionMode = Arg<0>();
            runSettings.ErrorReason = GetErrorReason(Arg<1>());
            UNIT_ASSERT(RunProgram(query, runSettings));
        });
        TStringBuf filteredInputData = "{\"key\"=\"800\";\"subkey\"=\".\";\"value\"=\"ddd\"};\n"sv;
        auto expected = NYT::NodeToCanonicalYsonString(NYT::NodeFromYsonString(filteredInputData, NYT::NYson::EYsonType::ListFragment));
        UNIT_ASSERT_NO_DIFF(sqlQueryResult, expected);
    }

    Y_UNIT_TEST(SelectFixedColumn, EExecutionMode, EInjectedFailure) {
        auto query = "use plato; insert into Output with truncate select key from Input where Cast(key As Uint32) > 700";
        TTempFileHandle outputFile;
        auto sqlQueryResult = WithTables([&](const auto& tables) {
            TRunSettings runSettings;
            runSettings.Tables = tables;
            runSettings.ExecutionMode = Arg<0>();
            runSettings.ErrorReason = GetErrorReason(Arg<1>());
            UNIT_ASSERT(RunProgram(query, runSettings));
        });
        TStringBuf filteredInputData = "{\"key\"=\"800\";};\n"sv;
        auto expected = NYT::NodeToCanonicalYsonString(NYT::NodeFromYsonString(filteredInputData, NYT::NYson::EYsonType::ListFragment));
        UNIT_ASSERT_NO_DIFF(sqlQueryResult, expected);
    }
    Y_UNIT_TEST(AnonymousTables, EExecutionMode, EInjectedFailure) {
        auto query = R"(
            use plato;
            PRAGMA DqEngine = 'disable';

            INSERT INTO @anon1 with truncate (
                SELECT * FROM `Input`
            );

            COMMIT;

            INSERT INTO @anon2 WITH truncate (
                SELECT * FROM @anon1
            );

            COMMIT;

            INSERT INTO `Output` WITH TRUNCATE (
                SELECT value || '_suffix' AS result FROM @anon2
            );
        )";
        TTempFileHandle outputFile;
        auto sqlQueryResult = WithTables([&](const auto& tables) {
            TRunSettings runSettings;
            runSettings.Tables = tables;
            runSettings.ExecutionMode = Arg<0>();
            runSettings.ErrorReason = GetErrorReason(Arg<1>());
            UNIT_ASSERT(RunProgram(query, runSettings));
        });
        TString expected = "[{\"result\"=\"abc_suffix\"};{\"result\"=\"ddd_suffix\"};{\"result\"=\"q_suffix\"};{\"result\"=\"qzz_suffix\"}]";
        UNIT_ASSERT_NO_DIFF(sqlQueryResult, expected);
    }
} // Y_UNIT_TEST_SUITE(FastMapReduceTests)
