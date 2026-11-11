#include "task_executor.h"

#include <yql/tools/yqlworker/config/config.h>
#include <yql/tools/yqlworker/executor/subproc/bottom_half.h>
#include <yql/tools/yqlworker/executor/subproc/control_protocol.h>
#include <yql/tools/yqlworker/misc/multiprocessing.h>

#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/threading/future/future.h>


using namespace NYql;
using namespace NThreading;
using namespace NSubprocessTaskExecutor;

//////////////////////////////////////////////////////////////////////////////
// TFutureResultWatcher
//////////////////////////////////////////////////////////////////////////////
struct TFutureResultWatcher final: public ITaskWatcher {
    TFutureResultWatcher()
        : Promise_(NewPromise<TTaskResultPtr>())
        , Result_(new NProto::TTaskResult)
    {
    }

    TTaskResultPtr GetResult(TDuration timeout) {
        return Promise_.GetFuture().GetValue(timeout);
    }

private:
    void OnStart() override {
    }

    void OnFinish(NProto::ETaskStatus /*status*/) override {
        Promise_.SetValue(Result_);
    }

    void OnResultChange(
            TTaskResultChanges::TMask /*changesMask*/,
            TTaskResultPtr changes,
            int /*workerPid*/,
            NBus::TOnMessageContext*) override
    {
        Result_->MergeFrom(*changes);
    }

    bool WillWatch(ui32 /*changesMask*/) const override {
        return true;
    }

private:
    NThreading::TPromise<TTaskResultPtr> Promise_;
    TTaskResultPtr Result_;
};

//////////////////////////////////////////////////////////////////////////////
// TTaskExecutorFactory
//////////////////////////////////////////////////////////////////////////////
class TTaskExecutorFactory
{
public:
    TTaskExecutorFactory()
        : ForkerProc_([this](void* param) { return ForkerProcMain(param); })
    {
        MainCfg_.Worker.MutableExecutor()->SetThreads(1);
        MainCfg_.Worker.MutableExecutor()->SetQueueMaxSize(1);

        TYtGatewayConfig* ytCfg = MainCfg_.Gateways.MutableYt();
        ytCfg->SetGatewayThreads(1);

        TYtClusterConfig* cluster = ytCfg->AddClusterMapping();
        cluster->SetName("plato");
        cluster->SetCluster("plato.search.yandex.net");
        cluster->SetDefault(true);

        MainCfg_.FileStorage.SetMaxFiles(64);
        MainCfg_.FileStorage.SetMaxSizeMb(512);

        Metrics_ = CreateMetricsRegistry(GetSensorsRootGroup());
    }

    ITaskExecutorPtr CreateInprocess() {
        auto result = CreateInprocessTaskExecutor(MainCfg_.Worker.GetExecutor(), {}, MainCfg_.Gateways, MainCfg_.Worker.GetStaticGateways(),
                                                  MainCfg_.FileStorage, Metrics_);
        result->Startup();
        return result;
    }

    ITaskExecutorPtr CreateSubprocess(
            TIpcChannelEndpoint& ep1,
            TIpcChannelEndpoint& ep2)
    {
        ForkerProc_.Spawn(&ep1);
        auto result = CreateSubprocessTaskExecutor(MainCfg_.Worker.GetExecutor(), ep2, Metrics_, NBus::BindOnPort(0, false).second);
        result->Startup();
        return result;
    }

    int ForkerProcMain(void* param) {
        auto* coreProcCh = reinterpret_cast<TIpcChannelEndpoint*>(param);
        TForkerProcess forkerProc(MainCfg_, {}, *coreProcCh, Metrics_);
        return forkerProc.Run();
    }

private:
    TMainConfig MainCfg_;
    IMetricsRegistryPtr Metrics_;

    TProcess ForkerProc_;
};

Y_UNIT_TEST_SUITE(TTaskExecutorTest)
{
    void RunSimpleProgram(const ITaskExecutorPtr& executor) {
        TTaskDataPtr taskData = std::make_shared<NProto::TTaskData>();
        taskData->SetId("test-operation-id");
        taskData->SetProgram("SELECT 42;");
        taskData->SetSyntax(NProto::ESyntax::SQLv1);
        taskData->SetResultFormat(NProto::EDataFormat::YSON_PRETTY);

        auto watcher = MakeIntrusive<TFutureResultWatcher>();
        auto status = executor->ProcessTask(
                    NProto::ETaskAction::RUN,
                    std::move(taskData),
                    watcher);
        UNIT_ASSERT_EQUAL(status, NProto::TProcessTaskResponse::SUBMITED);

        TTaskResultPtr result = watcher->GetResult(TDuration::Seconds(5));
        UNIT_ASSERT_EQUAL(result->GetStatus(), NProto::ETaskStatus::COMPLETED);
        UNIT_ASSERT(result->HasAst());
        UNIT_ASSERT(result->HasPlan());
        UNIT_ASSERT(result->ResultsSize() > 0);
        UNIT_ASSERT(result->GetRevision() > 0);
    }

    Y_UNIT_TEST(InprocessTaskExecutor) {
        TTaskExecutorFactory factory;
        ITaskExecutorPtr executor = factory.CreateInprocess();
        RunSimpleProgram(executor);
    }

    Y_UNIT_TEST(SubprocessTaskExecutor) {
        auto ch = DuplexIpcChannel();

        ITaskExecutorPtr executor;
        // destroy forker process before executor
        {
            TTaskExecutorFactory factory;
            executor = factory.CreateSubprocess(ch.first, ch.second);
            RunSimpleProgram(executor);
        }
    }
}
