#include "runnable_tasks.h"

#include <yql/essentials/core/qplayer/storage/memory/yql_qstorage_memory.h>

#include <library/cpp/yson/node/node.h>
#include <library/cpp/yson/node/node_io.h>
#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/yexception.h>
#include <util/system/thread.h>

#include <functional>


using namespace NYql;

//////////////////////////////////////////////////////////////////////////////
// TTestYqlRunnableTaskFactory
//////////////////////////////////////////////////////////////////////////////
class TTestYqlRunnableTaskFactory {
public:
    using TGatewaysConfigModifier = std::function<void(TGatewaysConfig&)>;

    TTestYqlRunnableTaskFactory()
        : TTestYqlRunnableTaskFactory([](TGatewaysConfig&) {}) {}

    explicit TTestYqlRunnableTaskFactory(TGatewaysConfigModifier modifier, TTaskDataPtr taskData = {},
            const NProto::TFunctionRegistryData* functionRegistryData = nullptr, const TQContext& qContext = {}) {
        NProto::TTaskExecutorConfig executorCfg;
        executorCfg.SetThreads(1);
        executorCfg.SetQueueMaxSize(1);

        TGatewaysConfig gatewaysCfg;
        TYtGatewayConfig* ytCfg = gatewaysCfg.MutableYt();
        ytCfg->SetGatewayThreads(1);

        TStaticGatewaysConfig staticGatewaysCfg;

        TYtClusterConfig* cluster = ytCfg->AddClusterMapping();
        cluster->SetName("plato");
        cluster->SetCluster("plato.search.yandex.net");
        cluster->SetDefault(true);

        modifier(gatewaysCfg);

        TFileStorageConfig fsCfg;
        fsCfg.SetMaxFiles(64);
        fsCfg.SetMaxSizeMb(512);

        auto metrics = CreateMetricsRegistry(GetSensorsRootGroup());
        Slave_.Reset(new TYqlRunnableTaskFactory(gatewaysCfg, staticGatewaysCfg, fsCfg, executorCfg, NProto::TDqConfig{}, metrics));
        Slave_->InitGateways(std::move(taskData), functionRegistryData, qContext);
    }

    IYqlRunnableTaskPtr Create(
            NProto::ETaskAction action, TTaskDataPtr data,
            ITaskWatcherPtr watcher)
    {
        return Slave_->Create(action, data, watcher);
    }

private:
    THolder<TYqlRunnableTaskFactory> Slave_;
};

//////////////////////////////////////////////////////////////////////////////
// TCountingWatcher
//////////////////////////////////////////////////////////////////////////////
struct TCountingWatcher final: public ITaskWatcher {
    std::function<void()> OnProgressSubscription;
    std::function<void()> OnRunning;
    ui32 StartCount = 0;
    ui32 FinishCount = 0;
    ui32 StatusChangesCount = 0;
    ui32 AstChangesCount = 0;
    ui32 PlanChangesCount = 0;
    ui32 ResultsChangesCount = 0;
    ui32 ErrorsChangesCount = 0;
    ui32 ProgressChangesCount = 0;
    ui32 RevisionChangesCount = 0;

    void Output(IOutputStream* out) const {
        (*out) << "StartCount: " << StartCount
               << ", FinishCount: " << FinishCount
               << ", StatusChangesCount: " << StatusChangesCount
               << ", AstChangesCount: " << AstChangesCount
               << ", PlanChangesCount: " << PlanChangesCount
               << ", ResultsChangesCount: " << ResultsChangesCount
               << ", ErrorsChangesCount: " << ErrorsChangesCount
               << ", ProgressChangesCount: " << ProgressChangesCount
               << ", RevisionChangesCount: " << RevisionChangesCount;
    }

private:
    void OnStart() override {
        StartCount++;
    }

    void OnFinish(NProto::ETaskStatus status) override {
        Y_UNUSED(status);
        FinishCount++;
    }

    void OnResultChange(
            TTaskResultChanges::TMask changesMask,
            TTaskResultPtr changes,
            int workerPid,
            NBus::TOnMessageContext*) override
    {
        Y_UNUSED(workerPid);

        if (TTaskResultChanges::IsAnyStatus(changesMask)) {
            StatusChangesCount++;
            if (changes->GetStatus() == NProto::ETaskStatus::RUNNING && OnRunning) {
                OnRunning();
            }
        }
        if (TTaskResultChanges::AST & changesMask) {
            AstChangesCount++;
        }
        if (NProto::ETaskResultChange::PLAN & changesMask) {
            PlanChangesCount++;
        }
        if (NProto::ETaskResultChange::RESULTS & changesMask) {
            ResultsChangesCount++;
        }
        if (NProto::ETaskResultChange::ERRORS & changesMask) {
            ErrorsChangesCount++;
        }
        if (NProto::ETaskResultChange::PROGRESS & changesMask) {
            ProgressChangesCount++;
        }
        if (NProto::ETaskResultChange::REVISION & changesMask) {
            RevisionChangesCount++;
        }
    }

    bool WillWatch(ui32 changesMask) const override {
        if (changesMask == TTaskResultChanges::PROGRESS && OnProgressSubscription) {
            OnProgressSubscription();
        }
        return true; // watch all changes
    }
};

template <>
void Out<TCountingWatcher>(IOutputStream& out, const TCountingWatcher& cw) {
    cw.Output(&out);
}

TTaskDataPtr MakeStrictConfigValidationTaskData(bool strict) {
    auto data = std::make_shared<NProto::TTaskData>();
    data->SetId("some-id");
    data->SetSyntax(NProto::ESyntax::SQLv1);
    data->SetProgram("SELECT 1;");
    data->SetResultFormat(NProto::EDataFormat::YSON_TEXT);
    data->SetStrictConfigValidation(strict);
    return data;
}

IYqlRunnableTaskPtr RunTask(
        TTestYqlRunnableTaskFactory& factory,
        NProto::ETaskAction action,
        TTaskDataPtr data) {
    auto counter = MakeIntrusive<TCountingWatcher>();
    IYqlRunnableTaskPtr runnable = factory.Create(action, std::move(data), counter);
    UNIT_ASSERT(runnable->IsIdle());
    UNIT_ASSERT(runnable->Run());
    UNIT_ASSERT(runnable->IsDone());
    return runnable;
}

IYqlRunnableTaskPtr RunStrictConfigValidationTask(
        TTestYqlRunnableTaskFactory& factory,
        NProto::ETaskAction action,
        bool strict) {
    return RunTask(factory, action, MakeStrictConfigValidationTaskData(strict));
}

void AddProjectPragma(NProto::TTaskData& data, const TString& name, const TVector<TString>& values) {
    auto* pragma = data.MutableProject()->MutableSettings()->AddPragmas();
    pragma->SetName(name);
    for (const auto& value : values) {
        pragma->AddValue(value);
    }
}

bool ContainsError(const NProto::TTaskResult& result, TStringBuf needle) {
    for (const auto& err : result.GetErrors()) {
        if (err.GetMessage().Contains(needle)) {
            return true;
        }
    }
    return false;
}

namespace {

    enum class EAbortPoint {
        DuringInitialization,
        AfterInitialization,
        DuringFailedInitialization,
    };

    void CheckTaskAbort(EAbortPoint abortPoint) {
        TTestYqlRunnableTaskFactory factory;
        auto watcher = MakeIntrusive<TCountingWatcher>();
        auto runnable = factory.Create(NProto::ETaskAction::PARSE, MakeStrictConfigValidationTaskData(false), watcher);
        const bool duringInitialization = abortPoint != EAbortPoint::AfterInitialization;
        ui32 abortRequests = 0;
        auto& callback = duringInitialization ? watcher->OnProgressSubscription : watcher->OnRunning;
        callback = [&] {
            TThread abortThread([&] {
                runnable->Abort();
                runnable->Abort();
            });
            abortThread.Start();
            abortThread.Join();
            ++abortRequests;
            if (abortPoint == EAbortPoint::DuringFailedInitialization) {
                ythrow yexception() << "Test initialization failure";
            }
        };
        UNIT_ASSERT(runnable->Run());
        UNIT_ASSERT_VALUES_EQUAL(abortRequests, 1);
        UNIT_ASSERT(runnable->IsAborted());
        UNIT_ASSERT(runnable->GetResult().GetStatus() == NProto::ETaskStatus::ABORTED);
        UNIT_ASSERT_VALUES_EQUAL(watcher->FinishCount, 1);
        UNIT_ASSERT_VALUES_EQUAL(watcher->StatusChangesCount, duringInitialization ? 2 : 3);
    }

} // namespace

Y_UNIT_TEST_SUITE(TYqlRunnableTasksTest)
{
    Y_UNIT_TEST(AbortDuringProgramInitialization) {
        CheckTaskAbort(EAbortPoint::DuringInitialization);
    }

    Y_UNIT_TEST(AbortAfterProgramInitialization) {
        CheckTaskAbort(EAbortPoint::AfterInitialization);
    }

    Y_UNIT_TEST(AbortDuringFailedProgramInitialization) {
        CheckTaskAbort(EAbortPoint::DuringFailedInitialization);
    }

    Y_UNIT_TEST(ParseGoodYql) {
        TTaskDataPtr data = std::make_shared<NProto::TTaskData>();
        data->SetId("some-id");
        data->SetSyntax(NProto::ESyntax::YQL);
        data->SetProgram(
                R"((
                    (let res_sink (DataSink 'result))
                    (let res (Uint32 '42))
                    (let world (Write! world res_sink (Key) res '('('type))))
                    (return (Commit! world res_sink))
                ))");
        data->SetResultFormat(NProto::EDataFormat::YSON_TEXT);

        TTestYqlRunnableTaskFactory factory;
        auto counter = MakeIntrusive<TCountingWatcher>();
        auto runnable = factory.Create(NProto::ETaskAction::PARSE, data, counter);
        UNIT_ASSERT(runnable->IsIdle());
        UNIT_ASSERT(runnable->Run());
        UNIT_ASSERT(runnable->IsDone());
        Cout << "ParseGoodYql: " << *counter << Endl;
        UNIT_ASSERT_VALUES_EQUAL(counter->StartCount, 1);
        UNIT_ASSERT_VALUES_EQUAL(counter->FinishCount, 1);
        UNIT_ASSERT_VALUES_EQUAL(counter->StatusChangesCount, 3);
        UNIT_ASSERT_VALUES_EQUAL(counter->AstChangesCount, 1);
        UNIT_ASSERT_VALUES_EQUAL(counter->PlanChangesCount, 0);
        UNIT_ASSERT_VALUES_EQUAL(counter->ResultsChangesCount, 0);
        UNIT_ASSERT_VALUES_EQUAL(counter->ErrorsChangesCount, 0);
        UNIT_ASSERT_VALUES_EQUAL(counter->ProgressChangesCount, 0);
        UNIT_ASSERT_VALUES_EQUAL(counter->RevisionChangesCount, 3);

        const NProto::TTaskResult& result = runnable->GetResult();
        UNIT_ASSERT(result.GetStatus() == NProto::ETaskStatus::COMPLETED);
        UNIT_ASSERT(result.HasAst());
        UNIT_ASSERT(!result.HasPlan());
        UNIT_ASSERT(result.ResultsSize() == 0);
        UNIT_ASSERT(result.GetRevision() > 0);
        UNIT_ASSERT(result.ErrorsSize() == 0);

        UNIT_ASSERT_NO_DIFF(
                result.GetAst(),
                "(\n"
                    "(let res_sink (DataSink 'result))\n"
                    "(let res (Uint32 '42))\n"
                    "(let world (Write! world res_sink (Key) res '('('type))))\n"
                    "(return (Commit! world res_sink))\n"
                ")\n");
    }

    Y_UNIT_TEST(ParseBadYql) {
        TTaskDataPtr data = std::make_shared<NProto::TTaskData>();
        data->SetId("some-id");
        data->SetSyntax(NProto::ESyntax::YQL);
        data->SetProgram(")(");
        data->SetResultFormat(NProto::EDataFormat::YSON_TEXT);

        TTestYqlRunnableTaskFactory factory;
        auto counter = MakeIntrusive<TCountingWatcher>();
        IYqlRunnableTaskPtr runnable = factory.Create(NProto::ETaskAction::PARSE, data, counter);
        UNIT_ASSERT(runnable->IsIdle());
        UNIT_ASSERT(runnable->Run());
        UNIT_ASSERT(runnable->IsDone());
        Cout << "ParseBadYql: " << *counter << Endl;
        UNIT_ASSERT_VALUES_EQUAL(counter->StartCount, 1);
        UNIT_ASSERT_VALUES_EQUAL(counter->FinishCount, 1);
        UNIT_ASSERT_VALUES_EQUAL(counter->StatusChangesCount, 3);
        UNIT_ASSERT_VALUES_EQUAL(counter->AstChangesCount, 0);
        UNIT_ASSERT_VALUES_EQUAL(counter->PlanChangesCount, 0);
        UNIT_ASSERT_VALUES_EQUAL(counter->ResultsChangesCount, 0);
        UNIT_ASSERT_VALUES_EQUAL(counter->ErrorsChangesCount, 1);
        UNIT_ASSERT_VALUES_EQUAL(counter->ProgressChangesCount, 0);
        UNIT_ASSERT_VALUES_EQUAL(counter->RevisionChangesCount, 3);

        const NProto::TTaskResult& result = runnable->GetResult();
        UNIT_ASSERT(result.GetStatus() == NProto::ETaskStatus::ERROR);
        UNIT_ASSERT(!result.HasAst());
        UNIT_ASSERT(!result.HasPlan());
        UNIT_ASSERT(result.ResultsSize() == 0);
        UNIT_ASSERT(result.GetRevision() > 0);
        UNIT_ASSERT(result.ErrorsSize() == 3);

        {
            const auto& e = result.GetErrors(0);
            UNIT_ASSERT_VALUES_EQUAL(e.GetRow(), 0);
            UNIT_ASSERT_VALUES_EQUAL(e.GetColumn(), 0);
            UNIT_ASSERT_STRINGS_EQUAL(e.GetMessage(), "Parse Yql");
        }

        {
            const auto& e = result.GetErrors(1);
            UNIT_ASSERT_VALUES_EQUAL(e.GetRow(), 1);
            UNIT_ASSERT_VALUES_EQUAL(e.GetColumn(), 1);
            UNIT_ASSERT_STRINGS_EQUAL(e.GetMessage(), "Expected (");
        }

        {
            const auto& e = result.GetErrors(2);
            UNIT_ASSERT_VALUES_EQUAL(e.GetRow(), 1);
            UNIT_ASSERT_VALUES_EQUAL(e.GetColumn(), 1);
            UNIT_ASSERT_STRINGS_EQUAL(e.GetMessage(), "Unexpected symbols after end of root list");
        }
    }

    Y_UNIT_TEST(ParseGoodSql) {
        TTaskDataPtr data = std::make_shared<NProto::TTaskData>();
        data->SetId("some-id");
        data->SetSyntax(NProto::ESyntax::SQLv1);
        data->SetProgram("SELECT 42;");
        data->SetResultFormat(NProto::EDataFormat::YSON_TEXT);

        TTestYqlRunnableTaskFactory factory;
        auto counter = MakeIntrusive<TCountingWatcher>();
        IYqlRunnableTaskPtr runnable = factory.Create(NProto::ETaskAction::PARSE, data, counter);
        UNIT_ASSERT(runnable->IsIdle());
        UNIT_ASSERT(runnable->Run());
        UNIT_ASSERT(runnable->IsDone());
        Cout << "ParseGoodSql: " << *counter << Endl;
        UNIT_ASSERT_VALUES_EQUAL(counter->StartCount, 1);
        UNIT_ASSERT_VALUES_EQUAL(counter->FinishCount, 1);
        UNIT_ASSERT_VALUES_EQUAL(counter->StatusChangesCount, 3);
        UNIT_ASSERT_VALUES_EQUAL(counter->AstChangesCount, 1);
        UNIT_ASSERT_VALUES_EQUAL(counter->PlanChangesCount, 0);
        UNIT_ASSERT_VALUES_EQUAL(counter->ResultsChangesCount, 0);
        UNIT_ASSERT_VALUES_EQUAL(counter->ErrorsChangesCount, 0);
        UNIT_ASSERT_VALUES_EQUAL(counter->ProgressChangesCount, 0);
        UNIT_ASSERT_VALUES_EQUAL(counter->RevisionChangesCount, 3);

        const NProto::TTaskResult& result = runnable->GetResult();
        UNIT_ASSERT(result.GetStatus() == NProto::ETaskStatus::COMPLETED);
        UNIT_ASSERT(result.HasAst());
        UNIT_ASSERT(!result.GetAst().empty());
        UNIT_ASSERT(!result.HasPlan());
        UNIT_ASSERT(result.ResultsSize() == 0);
        UNIT_ASSERT(result.GetRevision() > 0);
        UNIT_ASSERT(result.ErrorsSize() == 0);
    }

    Y_UNIT_TEST(Compile) {
        TTaskDataPtr data = std::make_shared<NProto::TTaskData>();
        data->SetId("some-id");
        data->SetSyntax(NProto::ESyntax::YQL);
        data->SetProgram(
                R"((
                    (let res_sink (DataSink 'result))
                    (let res (Uint32 '42))
                    (let world (Write! world res_sink (Key) res '('('type))))
                    (return (Commit! world res_sink))
                ))");
        data->SetResultFormat(NProto::EDataFormat::YSON_TEXT);

        TTestYqlRunnableTaskFactory factory;
        auto counter = MakeIntrusive<TCountingWatcher>();
        IYqlRunnableTaskPtr runnable = factory.Create(NProto::ETaskAction::COMPILE, data, counter);
        UNIT_ASSERT(runnable->IsIdle());
        UNIT_ASSERT(runnable->Run());
        UNIT_ASSERT(runnable->IsDone());
        Cout << "Compile: " << *counter << Endl;
        UNIT_ASSERT_VALUES_EQUAL(counter->StartCount, 1);
        UNIT_ASSERT_VALUES_EQUAL(counter->FinishCount, 1);
        UNIT_ASSERT_VALUES_EQUAL(counter->StatusChangesCount, 3);
        UNIT_ASSERT_VALUES_EQUAL(counter->AstChangesCount, 1);
        UNIT_ASSERT_VALUES_EQUAL(counter->PlanChangesCount, 0);
        UNIT_ASSERT_VALUES_EQUAL(counter->ResultsChangesCount, 0);
        UNIT_ASSERT_VALUES_EQUAL(counter->ErrorsChangesCount, 0);
        UNIT_ASSERT_VALUES_EQUAL(counter->ProgressChangesCount, 0);
        UNIT_ASSERT_VALUES_EQUAL(counter->RevisionChangesCount, 3);

        const NProto::TTaskResult& result = runnable->GetResult();
        UNIT_ASSERT(result.GetStatus() == NProto::ETaskStatus::COMPLETED);
        UNIT_ASSERT(result.HasAst());
        UNIT_ASSERT(!result.HasPlan());
        UNIT_ASSERT(result.ResultsSize() == 0);
        UNIT_ASSERT(result.GetRevision() > 0);
        UNIT_ASSERT(result.ErrorsSize() == 0);

        UNIT_ASSERT_NO_DIFF(
                result.GetAst(),
                "(\n"
                "(let $1 (DataSink 'result))\n"
                "(let $2 (Write! world $1 (Key) (Uint32 '42) '('('type))))\n"
                "(return (Commit! $2 $1))\n"
                ")\n");
    }

    Y_UNIT_TEST(Validate) {
        TTaskDataPtr data = std::make_shared<NProto::TTaskData>();
        data->SetId("some-id");
        data->SetSyntax(NProto::ESyntax::YQL);
        data->SetProgram(
                R"((
                    (let res_sink (DataSink 'result))
                    (let res (Uint32 '42))
                    (let world (Write! world res_sink (Key) res '('('type))))
                    (return (Commit! world res_sink))
                ))");
        data->SetResultFormat(NProto::EDataFormat::YSON_TEXT);

        TTestYqlRunnableTaskFactory factory;
        auto counter = MakeIntrusive<TCountingWatcher>();
        IYqlRunnableTaskPtr runnable = factory.Create(NProto::ETaskAction::VALIDATE, data, counter);
        UNIT_ASSERT(runnable->IsIdle());
        UNIT_ASSERT(runnable->Run());
        UNIT_ASSERT(runnable->IsDone());
        Cout << "Validate: " << *counter << Endl;
        UNIT_ASSERT_VALUES_EQUAL(counter->StartCount, 1);
        UNIT_ASSERT_VALUES_EQUAL(counter->FinishCount, 1);
        UNIT_ASSERT_VALUES_EQUAL(counter->StatusChangesCount, 3);
        UNIT_ASSERT_VALUES_EQUAL(counter->AstChangesCount, 1);
        UNIT_ASSERT_VALUES_EQUAL(counter->PlanChangesCount, 0);
        UNIT_ASSERT_VALUES_EQUAL(counter->ResultsChangesCount, 0);
        UNIT_ASSERT_VALUES_EQUAL(counter->ErrorsChangesCount, 0);
        UNIT_ASSERT_VALUES_EQUAL(counter->ProgressChangesCount, 0);
        UNIT_ASSERT_VALUES_EQUAL(counter->RevisionChangesCount, 3);

        const NProto::TTaskResult& result = runnable->GetResult();
        UNIT_ASSERT(result.GetStatus() == NProto::ETaskStatus::COMPLETED);
        UNIT_ASSERT(result.HasAst());
        UNIT_ASSERT(!result.HasPlan());
        UNIT_ASSERT(result.ResultsSize() == 0);
        UNIT_ASSERT(result.GetRevision() > 0);
        UNIT_ASSERT(result.ErrorsSize() == 0);

        UNIT_ASSERT_NO_DIFF(
                result.GetAst(),
                "(\n"
                "(let $1 (DataSink 'result))\n"
                "(let $2 (ResWrite! world $1 (Key) (Uint32 '42) '('('type))))\n"
                "(return (Commit! $2 $1))\n"
                ")\n");
    }

    Y_UNIT_TEST(Optimize) {
        TTaskDataPtr data = std::make_shared<NProto::TTaskData>();
        data->SetId("some-id");
        data->SetSyntax(NProto::ESyntax::YQL);
        data->SetProgram(
                R"((
                    (let res_sink (DataSink 'result))
                    (let res (Uint32 '42))
                    (let world (Write! world res_sink (Key) res '('('type))))
                    (return (Commit! world res_sink))
                ))");
        data->SetResultFormat(NProto::EDataFormat::YSON_TEXT);

        TTestYqlRunnableTaskFactory factory;
        auto counter = MakeIntrusive<TCountingWatcher>();
        IYqlRunnableTaskPtr runnable = factory.Create(NProto::ETaskAction::OPTIMIZE, data, counter);
        UNIT_ASSERT(runnable->IsIdle());
        UNIT_ASSERT(runnable->Run());
        UNIT_ASSERT(runnable->IsDone());
        Cout << "Optimize: " << *counter << Endl;
        UNIT_ASSERT_VALUES_EQUAL(counter->StartCount, 1);
        UNIT_ASSERT_VALUES_EQUAL(counter->FinishCount, 1);
        UNIT_ASSERT_VALUES_EQUAL(counter->StatusChangesCount, 3);
        UNIT_ASSERT_VALUES_EQUAL(counter->AstChangesCount, 2);
        UNIT_ASSERT_VALUES_EQUAL(counter->PlanChangesCount, 2);
        UNIT_ASSERT_VALUES_EQUAL(counter->ResultsChangesCount, 0);
        UNIT_ASSERT_VALUES_EQUAL(counter->ErrorsChangesCount, 0);
        UNIT_ASSERT_VALUES_EQUAL(counter->ProgressChangesCount, 0);
        UNIT_ASSERT_VALUES_EQUAL(counter->RevisionChangesCount, 4);

        const NProto::TTaskResult& result = runnable->GetResult();
        UNIT_ASSERT(result.GetStatus() == NProto::ETaskStatus::COMPLETED);
        UNIT_ASSERT(result.HasAst());
        UNIT_ASSERT(result.HasPlan());
        UNIT_ASSERT(result.ResultsSize() == 0);
        UNIT_ASSERT(result.GetRevision() > 0);
        UNIT_ASSERT(result.ErrorsSize() == 0);

        UNIT_ASSERT_NO_DIFF(
                result.GetAst(),
                "(\n"
                "(let $1 (DataSink 'result))\n"
                "(let $2 (ResFill! world $1 (Key) (Uint32 '42) '('('type)) '\"yt\"))\n"
                "(return (Commit! $2 $1))\n"
                ")\n");


        UNIT_ASSERT_NO_DIFF(
                result.GetPlan(),
                "{"
                    "\"Detailed\"={"
                        "\"Operations\"=["
                            "{\"Id\"=2u;\"Name\"=\"ResFill!\"}"
                        "];"
                        "\"OperationRoot\"=2u;"
                        "\"Providers\"=[];"
                        "\"OperationStats\"={\"ResFill!\"=1u}"
                    "};"
                    "\"Basic\"={"
                        "\"nodes\"=["
                            "{\"id\"=2u;\"level\"=1u;\"name\"=\"ResFill!\";\"type\"=\"op\"}"
                        "];"
                        "\"links\"=["
                        "]"
                    "}"
                "}");
    }

    Y_UNIT_TEST(Run) {
        TTaskDataPtr data = std::make_shared<NProto::TTaskData>();
        data->SetId("some-id");
        data->SetSyntax(NProto::ESyntax::YQL);
        data->SetProgram(
                R"((
                    (let res_sink (DataSink 'result))
                    (let res (Uint32 '42))
                    (let world (Write! world res_sink (Key) res '('('type))))
                    (return (Commit! world res_sink))
                ))");
        data->SetResultFormat(NProto::EDataFormat::YSON_TEXT);

        TTestYqlRunnableTaskFactory factory;
        auto counter = MakeIntrusive<TCountingWatcher>();
        IYqlRunnableTaskPtr runnable = factory.Create(NProto::ETaskAction::RUN, data, counter);
        UNIT_ASSERT(runnable->IsIdle());
        UNIT_ASSERT(runnable->Run());
        UNIT_ASSERT(runnable->IsDone());
        Cout << "Run: " << *counter << Endl;
        UNIT_ASSERT_VALUES_EQUAL(counter->StartCount, 1);
        UNIT_ASSERT_VALUES_EQUAL(counter->FinishCount, 1);
        UNIT_ASSERT_VALUES_EQUAL(counter->StatusChangesCount, 3);
        UNIT_ASSERT_VALUES_EQUAL(counter->AstChangesCount, 2);
        UNIT_ASSERT_VALUES_EQUAL(counter->PlanChangesCount, 2);
        UNIT_ASSERT_VALUES_EQUAL(counter->ResultsChangesCount, 1);
        UNIT_ASSERT_VALUES_EQUAL(counter->ErrorsChangesCount, 0);
        UNIT_ASSERT_VALUES_EQUAL(counter->ProgressChangesCount, 7);
        UNIT_ASSERT_VALUES_EQUAL(counter->RevisionChangesCount, 4);

        const NProto::TTaskResult& result = runnable->GetResult();
        UNIT_ASSERT(result.GetStatus() == NProto::ETaskStatus::COMPLETED);
        UNIT_ASSERT(result.HasAst());
        UNIT_ASSERT(result.HasPlan());
        UNIT_ASSERT(result.ResultsSize() == 1);
        UNIT_ASSERT(result.GetRevision() > 0);
        UNIT_ASSERT(result.ErrorsSize() == 0);

        UNIT_ASSERT_NO_DIFF(
                result.GetAst(),
                "(\n"
                "(let $1 (DataSink 'result))\n"
                "(let $2 (ResFill! world $1 (Key) (Uint32 '42) '('('type)) '\"yt\"))\n"
                "(return (Commit! $2 $1))\n"
                ")\n");

        UNIT_ASSERT_NO_DIFF(
                result.GetPlan(),
                "{"
                    "\"Detailed\"={"
                        "\"Operations\"=["
                            "{\"Id\"=2u;\"Name\"=\"ResFill!\"}"
                        "];"
                        "\"OperationRoot\"=2u;"
                        "\"Providers\"=[];"
                        "\"OperationStats\"={\"ResFill!\"=1u}"
                    "};"
                    "\"Basic\"={"
                        "\"nodes\"=["
                            "{\"id\"=2u;\"level\"=1u;\"name\"=\"ResFill!\";\"type\"=\"op\"}"
                        "];"
                        "\"links\"=["
                        "]"
                    "}"
                "}");

        TStringBuf expectedResult = R"(
        {
            "Position"={"File"="<main>";"Row"=4;"Column"=33};
            "Write"=[{
                "Type"=["DataType";"Uint32"];
                "Data"="42"
            }]
        })";

        UNIT_ASSERT_EQUAL(NYT::NodeFromYsonString(result.GetResults(0)), NYT::NodeFromYsonString(expectedResult));
    }

    Y_UNIT_TEST(ExtractParamsFromGoodSQL) {
        TTaskDataPtr data = std::make_shared<NProto::TTaskData>();
        data->SetId("some-id");
        data->SetSyntax(NProto::ESyntax::SQLv1);
        data->SetProgram(
            "declare $x as int;"
            "select 2 * $x;");
        data->SetResultFormat(NProto::EDataFormat::YSON_TEXT);

        TTestYqlRunnableTaskFactory factory;
        auto counter = MakeIntrusive<TCountingWatcher>();
        IYqlRunnableTaskPtr runnable = factory.Create(NProto::ETaskAction::EXTRACT_PARAMS_META, data, counter);
        UNIT_ASSERT(runnable->IsIdle());
        UNIT_ASSERT(runnable->Run());
        UNIT_ASSERT(runnable->IsDone());

        const NProto::TTaskResult& result = runnable->GetResult();
        UNIT_ASSERT(result.GetStatus() == NProto::ETaskStatus::COMPLETED);
        UNIT_ASSERT(result.HasAst());
        UNIT_ASSERT(!result.HasPlan());
        UNIT_ASSERT(result.ResultsSize() == 1);
        UNIT_ASSERT(result.GetRevision() > 0);
        UNIT_ASSERT(result.ErrorsSize() == 0);

        UNIT_ASSERT_NO_DIFF(
            result.GetAst(),
            R"((
(declare $x (DataType 'Int32))
(let $1 (DataSink 'result))
(let $2 (AsList (AsStruct)))
(let $3 '('('type) '('autoref) '('columns '('('auto)))))
(let $4 (Write! world $1 (Key) (Unordered (PersistableRepr (OrderedSqlProject $2 '((SqlProjectItem (TypeOf $2) '"" (lambda '($5) (*MayWarn (Int32 '"2") $x)) '('('autoName))))))) $3))
(return (CommitAll! (Commit! $4 $1)))
)
)");

        TStringBuf expectedResult = R"({"$x"=["DataType";"Int32"]})";
        UNIT_ASSERT_NO_DIFF(result.GetResults(0), expectedResult);
    }


    Y_UNIT_TEST(ExtractParamsFromGoodYQL) {
        TTaskDataPtr data = std::make_shared<NProto::TTaskData>();
        data->SetId("some-id");
        data->SetSyntax(NProto::ESyntax::YQL);
        data->SetProgram(
            R"((
(let res_sink (DataSink 'result))
(let data (Parameter '"$foo" (ParseType '"Tuple<String, Int32 ? , List<Uint32>, Dict<Int32, Bool>, Struct<a : Void, b : Double>, Variant<Int32, Bool>>")))
(let world (Write! world res_sink (Key) data '('('type))))
(let world (Commit! world res_sink))
(return world)
)
)");

        data->SetResultFormat(NProto::EDataFormat::YSON_TEXT);

        TTestYqlRunnableTaskFactory factory;
        auto counter = MakeIntrusive<TCountingWatcher>();
        IYqlRunnableTaskPtr runnable = factory.Create(NProto::ETaskAction::EXTRACT_PARAMS_META, data, counter);
        UNIT_ASSERT(runnable->IsIdle());
        UNIT_ASSERT(runnable->Run());
        UNIT_ASSERT(runnable->IsDone());

        const NProto::TTaskResult& result = runnable->GetResult();
        UNIT_ASSERT(result.GetStatus() == NProto::ETaskStatus::COMPLETED);
        UNIT_ASSERT(result.HasAst());
        UNIT_ASSERT(!result.HasPlan());
        UNIT_ASSERT(result.ResultsSize() == 1);
        UNIT_ASSERT(result.GetRevision() > 0);
        UNIT_ASSERT(result.ErrorsSize() == 0);

        TStringBuf expectedResult = R"({"$foo"=["TupleType";[["DataType";"String"];["OptionalType";["DataType";"Int32"]];["ListType";["DataType";"Uint32"]];["DictType";["DataType";"Int32"];["DataType";"Bool"]];["StructType";[["a";["VoidType"]];["b";["DataType";"Double"]]]];["VariantType";["TupleType";[["DataType";"Int32"];["DataType";"Bool"]]]]]]})";
        UNIT_ASSERT_NO_DIFF(result.GetResults(0), expectedResult);
    }

    Y_UNIT_TEST(ExtractParamsWithLibs) {
        TTaskDataPtr data = std::make_shared<NProto::TTaskData>();
        data->SetId("some-id");
        data->SetSyntax(NProto::ESyntax::SQLv1);

        // error at compile phase
        data->SetProgram(
            R"(
declare $x as int;
pragma library("lib1.sql");
import lib1 symbols $a;
select $a($x);
)");

        data->SetResultFormat(NProto::EDataFormat::YSON_TEXT);

        TTestYqlRunnableTaskFactory factory;
        auto counter = MakeIntrusive<TCountingWatcher>();
        IYqlRunnableTaskPtr runnable = factory.Create(NProto::ETaskAction::EXTRACT_PARAMS_META, data, counter);
        UNIT_ASSERT(runnable->IsIdle());
        UNIT_ASSERT(runnable->Run());
        UNIT_ASSERT(runnable->IsDone());

        const NProto::TTaskResult& result = runnable->GetResult();
        UNIT_ASSERT(result.GetStatus() == NProto::ETaskStatus::COMPLETED);
        UNIT_ASSERT(result.HasAst());
        UNIT_ASSERT(!result.HasPlan());
        UNIT_ASSERT(result.ResultsSize() == 1);
        UNIT_ASSERT(result.GetRevision() > 0);
        UNIT_ASSERT(result.ErrorsSize() == 0);

        TStringBuf expectedResult = R"({"$x"=["DataType";"Int32"]})";
        UNIT_ASSERT_NO_DIFF(result.GetResults(0), expectedResult);
    }

    Y_UNIT_TEST(ExtractParamsWithLibsAndPlaceholders) {
        TTaskDataPtr data = std::make_shared<NProto::TTaskData>();
        data->SetId("some-id");
        data->SetSyntax(NProto::ESyntax::SQLv1);

        // error at compile phase
        data->SetProgram(
            R"(
declare $x as string;
pragma library("lib1.sql","file{$x}");
)");

        data->SetResultFormat(NProto::EDataFormat::YSON_TEXT);

        TTestYqlRunnableTaskFactory factory;
        auto counter = MakeIntrusive<TCountingWatcher>();
        IYqlRunnableTaskPtr runnable = factory.Create(NProto::ETaskAction::EXTRACT_PARAMS_META, data, counter);
        UNIT_ASSERT(runnable->IsIdle());
        UNIT_ASSERT(runnable->Run());
        UNIT_ASSERT(runnable->IsDone());

        const NProto::TTaskResult& result = runnable->GetResult();
        UNIT_ASSERT(result.GetStatus() == NProto::ETaskStatus::COMPLETED);
        UNIT_ASSERT(result.HasAst());
        UNIT_ASSERT(!result.HasPlan());
        UNIT_ASSERT(result.ResultsSize() == 1);
        UNIT_ASSERT(result.GetRevision() > 0);
        UNIT_ASSERT(result.ErrorsSize() == 0);

        TStringBuf expectedResult = R"({"$x"=["DataType";"String"]})";
        UNIT_ASSERT_NO_DIFF(result.GetResults(0), expectedResult);
    }

    Y_UNIT_TEST(ParsePG) {
        TTaskDataPtr data = std::make_shared<NProto::TTaskData>();
        data->SetId("some-id");
        data->SetSyntax(NProto::ESyntax::PG);
        data->SetProgram("SELECT 1::text");
        data->SetResultFormat(NProto::EDataFormat::YSON_TEXT);

        TTestYqlRunnableTaskFactory factory;
        auto counter = MakeIntrusive<TCountingWatcher>();
        IYqlRunnableTaskPtr runnable = factory.Create(NProto::ETaskAction::PARSE, data, counter);
        UNIT_ASSERT(runnable->IsIdle());
        UNIT_ASSERT(runnable->Run());
        UNIT_ASSERT(runnable->IsDone());
        Cout << "ParseGoodSql: " << *counter << Endl;
        UNIT_ASSERT_VALUES_EQUAL(counter->StartCount, 1);
        UNIT_ASSERT_VALUES_EQUAL(counter->FinishCount, 1);
        UNIT_ASSERT_VALUES_EQUAL(counter->StatusChangesCount, 3);
        UNIT_ASSERT_VALUES_EQUAL(counter->AstChangesCount, 1);
        UNIT_ASSERT_VALUES_EQUAL(counter->PlanChangesCount, 0);
        UNIT_ASSERT_VALUES_EQUAL(counter->ResultsChangesCount, 0);
        UNIT_ASSERT_VALUES_EQUAL(counter->ErrorsChangesCount, 0);
        UNIT_ASSERT_VALUES_EQUAL(counter->ProgressChangesCount, 0);
        UNIT_ASSERT_VALUES_EQUAL(counter->RevisionChangesCount, 3);

        const NProto::TTaskResult& result = runnable->GetResult();
        UNIT_ASSERT(result.GetStatus() == NProto::ETaskStatus::COMPLETED);
        UNIT_ASSERT(result.HasAst());
        UNIT_ASSERT(!result.GetAst().empty());
        UNIT_ASSERT(!result.HasPlan());
        UNIT_ASSERT(result.ResultsSize() == 0);
        UNIT_ASSERT(result.GetRevision() > 0);
        UNIT_ASSERT(result.ErrorsSize() == 0);
    }

    Y_UNIT_TEST(PrintTaskResultChangesMask) {
        {
            TStringStream ss;
            ss << TTaskResultChanges(TTaskResultChanges::MAX_VALUE);
            UNIT_ASSERT_VALUES_EQUAL(
                ss.Str(),
                "0b01111111111111111111111111111111(AST|PLAN|RESULTS|ERRORS|PROGRESS|REVISION|STATISTICS)");
        }
        {
            TStringStream ss;
            ss << TTaskResultChanges(TTaskResultChanges::STATUS_MASK);
            UNIT_ASSERT_VALUES_EQUAL(ss.Str(), "0b00000000000000000000000000111111()");
        }
        {
            TStringStream ss;
            ss << TTaskResultChanges(TTaskResultChanges::DATA_MASK);
            UNIT_ASSERT_VALUES_EQUAL(
                ss.Str(),
                "0b00000000000000000111100000000000(AST|PLAN|RESULTS|ERRORS)");
        }
        {
            TStringStream ss;
            ss << TTaskResultChanges(TTaskResultChanges::PLAN | TTaskResultChanges::REVISION);
            UNIT_ASSERT_VALUES_EQUAL(
                ss.Str(), "0b00000000000000010001000000000000(PLAN|REVISION)");
        }
    }

    Y_UNIT_TEST(ProjectSettingsApplyToStartupLibraries) {
        auto data = MakeStrictConfigValidationTaskData(true);
        AddProjectPragma(*data, "ClassicDivision", {"false"});
        data->SetProgram("IMPORT project SYMBOLS $value; SELECT $value;");
        NProto::TFunctionRegistryData registry;
        auto* package = registry.AddPackages();
        package->SetName("test");
        package->SetDefaultVersion(0);
        auto* library = package->AddLibraries();
        library->SetName("project.sql");
        library->SetContent("$value = 1 / 2; EXPORT $value;");
        library->SetVersion(0);
        TTestYqlRunnableTaskFactory factory([](TGatewaysConfig&) {}, data, &registry);
        auto actual = RunTask(factory, NProto::ETaskAction::RUN, data);
        UNIT_ASSERT_C(actual->GetResult().GetStatus() == NProto::ETaskStatus::COMPLETED, actual->GetResult().DebugString());
        auto expectedData = MakeStrictConfigValidationTaskData(true);
        expectedData->SetId("expected-library-result");
        expectedData->SetProgram("PRAGMA ClassicDivision='false'; SELECT 1 / 2;");
        auto expected = RunTask(factory, NProto::ETaskAction::RUN, expectedData);
        UNIT_ASSERT_C(expected->GetResult().GetStatus() == NProto::ETaskStatus::COMPLETED, expected->GetResult().DebugString());
        const auto actualResult = NYT::NodeFromYsonString(actual->GetResult().GetResults(0));
        const auto expectedResult = NYT::NodeFromYsonString(expected->GetResult().GetResults(0));
        UNIT_ASSERT_EQUAL(actualResult["Write"], expectedResult["Write"]);
    }

    Y_UNIT_TEST(ReplayProjectSettingsApplyToStartupLibraries) {
        auto storage = MakeMemoryQStorage();
        auto writer = storage->MakeWriter("query", {});
        writer->Put({.Component = "Facade", .Label = "ProjectSettings"},
            "{Pragmas=[{Name=ClassicDivision;Value=[false;];};];}").GetValueSync();
        writer->Commit().GetValueSync();
        const TQContext replay(storage->MakeReader("query", {}));
        for (bool changedSettings : {false, true}) {
            auto data = MakeStrictConfigValidationTaskData(true);
            if (changedSettings) {
                AddProjectPragma(*data, "ClassicDivision", {"true"});
            }
            data->SetProgram("IMPORT project SYMBOLS $value; SELECT $value;");
            NProto::TFunctionRegistryData registry;
            auto* package = registry.AddPackages();
            package->SetName("test");
            package->SetDefaultVersion(0);
            auto* library = package->AddLibraries();
            library->SetName("project.sql");
            library->SetContent("$value = 1 / 2; EXPORT $value;");
            library->SetVersion(0);
            TTestYqlRunnableTaskFactory factory([](TGatewaysConfig&) {}, data, &registry, replay);
            auto actual = RunTask(factory, NProto::ETaskAction::RUN, data);
            UNIT_ASSERT_C(actual->GetResult().GetStatus() == NProto::ETaskStatus::COMPLETED, actual->GetResult().DebugString());
            const auto result = NYT::NodeFromYsonString(actual->GetResult().GetResults(0));
            UNIT_ASSERT_VALUES_EQUAL(result["Write"][0]["Data"][0][0].AsString(), "0.5");
        }
    }

    Y_UNIT_TEST(ProjectSettingsSqlPragmaPrecedence) {
        TTestYqlRunnableTaskFactory factory;
        for (bool queryOverrides : {false, true}) {
            auto data = MakeStrictConfigValidationTaskData(true);
            AddProjectPragma(*data, "TablePathPrefix", {"project/"});
            data->SetProgram(TString(queryOverrides ? "PRAGMA TablePathPrefix = 'query/'; " : "")
                + "SELECT * FROM plato.Input;");
            auto runnable = RunTask(factory, NProto::ETaskAction::PARSE, data);
            const auto& result = runnable->GetResult();
            UNIT_ASSERT_C(result.GetStatus() == NProto::ETaskStatus::COMPLETED, result.DebugString());
            UNIT_ASSERT_VALUES_EQUAL(result.ErrorsSize(), 0);
            UNIT_ASSERT_C(result.GetAst().Contains(queryOverrides ? "query/Input" : "project/Input"), result.GetAst());
            if (queryOverrides) {
                UNIT_ASSERT(!result.GetAst().Contains("project/Input"));
            }
        }
    }

    Y_UNIT_TEST(ProjectSettingsAcceptedByDifferentComponents) {
        TTestYqlRunnableTaskFactory factory;
        for (const auto syntax : {NProto::ESyntax::SQLv1, NProto::ESyntax::PG}) {
            auto data = MakeStrictConfigValidationTaskData(true);
            data->SetSyntax(syntax);
            AddProjectPragma(*data, "ClassicDivision", {"false"});
            AddProjectPragma(*data, "yson.Strict", {});
            AddProjectPragma(*data, "config.flags", {"LLVM_OFF"});
            AddProjectPragma(*data, "config.flags", {"ValidateUdf", "None"});
            AddProjectPragma(*data, "yt.StaticPool", {"project"});
            AddProjectPragma(*data, "plato.StaticPool", {"cluster"});
            AddProjectPragma(*data, "dq.MaxTasksPerStage", {"1"});
            auto runnable = RunTask(factory, NProto::ETaskAction::VALIDATE, data);
            const auto& result = runnable->GetResult();
            UNIT_ASSERT_C(result.GetStatus() == NProto::ETaskStatus::COMPLETED, result.DebugString());
            UNIT_ASSERT_VALUES_EQUAL(result.ErrorsSize(), 0);
        }
    }

    Y_UNIT_TEST(ProjectSettingsUnknownNamesRespectStrictValidation) {
        TTestYqlRunnableTaskFactory factory;
        for (bool strict : {false, true}) {
            for (const auto& name : {"UnknownProjectPragma", "yt.UnknownProjectPragma", "plato.UnknownProjectPragma",
                    "dq.UnknownProjectPragma", "unregistered.Option", "yt.Bad__Name", "plato.A_B_C_D_E_F"}) {
                auto data = MakeStrictConfigValidationTaskData(strict);
                AddProjectPragma(*data, name, {"value"});
                auto runnable = RunTask(factory, NProto::ETaskAction::VALIDATE, data);
                const auto& result = runnable->GetResult();
                UNIT_ASSERT_C(result.GetStatus() == (strict ? NProto::ETaskStatus::ERROR : NProto::ETaskStatus::COMPLETED), result.DebugString());
                if (strict) {
                    UNIT_ASSERT_C(ContainsError(result, name), result.DebugString());
                } else {
                    UNIT_ASSERT_VALUES_EQUAL(result.ErrorsSize(), 0);
                }
            }
        }
    }

    Y_UNIT_TEST(ProjectSettingsDisallowedPragmasRespectStrictValidation) {
        TTestYqlRunnableTaskFactory factory;
        for (bool strict : {false, true}) {
            for (const auto& name : {"yt._EnableDq", "plato._EnableDq", "dq._AnalyzeQueryPercentage", "config.NodesAllocationLimit"}) {
                auto data = MakeStrictConfigValidationTaskData(strict);
                AddProjectPragma(*data, name, {"true"});
                auto runnable = RunTask(factory, NProto::ETaskAction::VALIDATE, data);
                const auto& result = runnable->GetResult();
                UNIT_ASSERT_C(result.GetStatus() == (strict ? NProto::ETaskStatus::ERROR : NProto::ETaskStatus::COMPLETED), result.DebugString());
                if (strict) {
                    UNIT_ASSERT_C(ContainsError(result, name), result.DebugString());
                } else {
                    UNIT_ASSERT_VALUES_EQUAL(result.ErrorsSize(), 0);
                }
            }
        }
    }

    Y_UNIT_TEST(ProjectSettingsInvalidValuesAreRejected) {
        TTestYqlRunnableTaskFactory factory;
        for (const auto& name : {"GroupByLimit", "yt.ValidateClusters", "dq.MaxTasksPerStage"}) {
            auto data = MakeStrictConfigValidationTaskData(true);
            AddProjectPragma(*data, name, {"invalid"});
            auto runnable = RunTask(factory, NProto::ETaskAction::VALIDATE, data);
            const auto& result = runnable->GetResult();
            UNIT_ASSERT_C(result.GetStatus() == NProto::ETaskStatus::ERROR, result.DebugString());
            UNIT_ASSERT_C(result.ErrorsSize() > 0, result.DebugString());
        }
    }

    Y_UNIT_TEST(ProjectSettingsKeepStrictSqlFlagValidation) {
        TTestYqlRunnableTaskFactory factory([](TGatewaysConfig& cfg) {
            cfg.MutableSqlCore()->AddTranslationFlags("UnknownProjectSqlFlag");
        });
        auto data = MakeStrictConfigValidationTaskData(true);
        AddProjectPragma(*data, "TablePathPrefix", {"project/"});
        auto runnable = RunTask(factory, NProto::ETaskAction::PARSE, data);
        const auto& result = runnable->GetResult();
        UNIT_ASSERT_C(result.GetStatus() == NProto::ETaskStatus::ERROR, result.DebugString());
        UNIT_ASSERT_C(ContainsError(result, "Unknown SQL flag: UnknownProjectSqlFlag"), result.DebugString());
    }

    Y_UNIT_TEST(ProjectSettingsConfigFlagsRequireAllowedName) {
        TTestYqlRunnableTaskFactory factory;
        for (bool strict : {false, true}) {
            for (const TVector<TString>& values : {TVector<TString>{}, TVector<TString>{""},
                    TVector<TString>{"UnknownProjectFlag"}, TVector<TString>{"NodesAllocationLimit", "1"}}) {
                auto data = MakeStrictConfigValidationTaskData(strict);
                AddProjectPragma(*data, "config.flags", values);
                auto runnable = RunTask(factory, NProto::ETaskAction::VALIDATE, data);
                const auto& result = runnable->GetResult();
                UNIT_ASSERT_C(result.GetStatus() == (strict ? NProto::ETaskStatus::ERROR : NProto::ETaskStatus::COMPLETED), result.DebugString());
                if (strict) {
                    UNIT_ASSERT_C(ContainsError(result, "config.flags"), result.DebugString());
                } else {
                    UNIT_ASSERT_VALUES_EQUAL(result.ErrorsSize(), 0);
                }
            }
        }
    }

    Y_UNIT_TEST(StrictConfigValidationFailsOnUnknownSimpleFlag) {
        TTestYqlRunnableTaskFactory factory([](TGatewaysConfig& cfg) {
            cfg.MutableSqlCore()->AddTranslationFlags("UnknownTestSimpleFlag");
        });
        auto runnable = RunStrictConfigValidationTask(factory, NProto::ETaskAction::PARSE, /*strict=*/true);
        const NProto::TTaskResult& result = runnable->GetResult();
        UNIT_ASSERT(result.GetStatus() == NProto::ETaskStatus::ERROR);
        UNIT_ASSERT(result.ErrorsSize() > 0);
        UNIT_ASSERT_C(ContainsError(result, "Unknown SQL flag"), "Expected 'Unknown SQL flag' error");
    }

    Y_UNIT_TEST(StrictConfigValidationFailsOnUnknownExtendedFlag) {
        TTestYqlRunnableTaskFactory factory([](TGatewaysConfig& cfg) {
            auto* flag = cfg.MutableSqlCore()->AddExtendedTranslationFlags();
            flag->SetName("UnknownTestExtendedFlag");
            flag->AddArgs("some_value");
        });
        auto runnable = RunStrictConfigValidationTask(factory, NProto::ETaskAction::PARSE, /*strict=*/true);
        const NProto::TTaskResult& result = runnable->GetResult();
        UNIT_ASSERT(result.GetStatus() == NProto::ETaskStatus::ERROR);
        UNIT_ASSERT(result.ErrorsSize() > 0);
        UNIT_ASSERT_C(ContainsError(result, "Unknown SQL flag"), "Expected 'Unknown SQL flag' error");
    }

    Y_UNIT_TEST(StrictConfigValidationOffIgnoresUnknownFlags) {
        TTestYqlRunnableTaskFactory factory([](TGatewaysConfig& cfg) {
            cfg.MutableSqlCore()->AddTranslationFlags("UnknownTestSimpleFlag");
            auto* flag = cfg.MutableSqlCore()->AddExtendedTranslationFlags();
            flag->SetName("UnknownTestExtendedFlag");
            flag->AddArgs("some_value");
        });
        auto runnable = RunStrictConfigValidationTask(factory, NProto::ETaskAction::PARSE, /*strict=*/false);
        const NProto::TTaskResult& result = runnable->GetResult();
        UNIT_ASSERT(result.GetStatus() == NProto::ETaskStatus::COMPLETED);
        UNIT_ASSERT(result.ErrorsSize() == 0);
    }

    Y_UNIT_TEST(StrictConfigValidationFailsOnUnknownConfigFlag) {
        TTestYqlRunnableTaskFactory factory([](TGatewaysConfig& cfg) {
            auto* flag = cfg.MutableYqlCore()->AddFlags();
            flag->SetName("UnknownTestConfigFlag");
        });
        auto runnable = RunStrictConfigValidationTask(factory, NProto::ETaskAction::VALIDATE, /*strict=*/true);
        const NProto::TTaskResult& result = runnable->GetResult();
        UNIT_ASSERT(result.GetStatus() == NProto::ETaskStatus::ERROR);
        UNIT_ASSERT(result.ErrorsSize() > 0);
        UNIT_ASSERT_C(ContainsError(result, "Unsupported command: UnknownTestConfigFlag"),
                      "Expected 'Unsupported command: UnknownTestConfigFlag' error");
    }

    Y_UNIT_TEST(StrictConfigValidationAcceptsKnownConfigFlag) {
        TTestYqlRunnableTaskFactory factory([](TGatewaysConfig& cfg) {
            auto* flag = cfg.MutableYqlCore()->AddFlags();
            flag->SetName("NodesAllocationLimit");
            flag->AddArgs("12345");
        });
        auto runnable = RunStrictConfigValidationTask(factory, NProto::ETaskAction::VALIDATE, /*strict=*/true);
        const NProto::TTaskResult& result = runnable->GetResult();
        UNIT_ASSERT_C(result.GetStatus() == NProto::ETaskStatus::COMPLETED,
            "Errors: " << (result.ErrorsSize() > 0 ? result.GetErrors(0).GetMessage() : TString("<none>")));
        UNIT_ASSERT(result.ErrorsSize() == 0);
    }

    Y_UNIT_TEST(StrictConfigValidationOffIgnoresUnknownConfigFlag) {
        TTestYqlRunnableTaskFactory factory([](TGatewaysConfig& cfg) {
            auto* flag = cfg.MutableYqlCore()->AddFlags();
            flag->SetName("UnknownTestConfigFlag");
        });
        auto runnable = RunStrictConfigValidationTask(factory, NProto::ETaskAction::VALIDATE, /*strict=*/false);
        const NProto::TTaskResult& result = runnable->GetResult();
        UNIT_ASSERT(result.GetStatus() == NProto::ETaskStatus::COMPLETED);
        UNIT_ASSERT(result.ErrorsSize() == 0);
    }

    Y_UNIT_TEST(StrictConfigValidationFailsOnUnknownYtPragma) {
        TTestYqlRunnableTaskFactory factory([](TGatewaysConfig& cfg) {
            auto* setting = cfg.MutableYt()->AddDefaultSettings();
            setting->SetName("UnknownYtPragma");
            setting->SetValue("some_value");
        });
        auto runnable = RunStrictConfigValidationTask(factory, NProto::ETaskAction::VALIDATE, /*strict=*/true);
        const NProto::TTaskResult& result = runnable->GetResult();
        UNIT_ASSERT(result.GetStatus() == NProto::ETaskStatus::ERROR);
        UNIT_ASSERT(result.ErrorsSize() > 0);
        UNIT_ASSERT_C(ContainsError(result, "Unknown setting name"), "Expected 'Unknown setting name' error");
    }

    Y_UNIT_TEST(StrictConfigValidationAcceptsKnownYtPragma) {
        TTestYqlRunnableTaskFactory factory([](TGatewaysConfig& cfg) {
            auto* setting = cfg.MutableYt()->AddDefaultSettings();
            setting->SetName("ValidateClusters");
            setting->SetValue("true");
        });
        auto runnable = RunStrictConfigValidationTask(factory, NProto::ETaskAction::VALIDATE, /*strict=*/true);
        const NProto::TTaskResult& result = runnable->GetResult();
        UNIT_ASSERT_C(result.GetStatus() == NProto::ETaskStatus::COMPLETED,
            "Errors: " << (result.ErrorsSize() > 0 ? result.GetErrors(0).GetMessage() : TString("<none>")));
        UNIT_ASSERT(result.ErrorsSize() == 0);
    }

    Y_UNIT_TEST(StrictConfigValidationOffIgnoresUnknownYtPragma) {
        TTestYqlRunnableTaskFactory factory([](TGatewaysConfig& cfg) {
            auto* setting = cfg.MutableYt()->AddDefaultSettings();
            setting->SetName("UnknownYtPragma");
            setting->SetValue("some_value");
        });
        auto runnable = RunStrictConfigValidationTask(factory, NProto::ETaskAction::VALIDATE, /*strict=*/false);
        const NProto::TTaskResult& result = runnable->GetResult();
        UNIT_ASSERT(result.GetStatus() == NProto::ETaskStatus::COMPLETED);
        UNIT_ASSERT(result.ErrorsSize() == 0);
    }
}
