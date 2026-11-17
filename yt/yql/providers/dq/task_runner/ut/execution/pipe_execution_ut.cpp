#include <yt/yql/providers/dq/task_runner/tasks_runner_pipe.h>

#include <yt/yql/providers/dq/runtime/file_cache.h>

#include <contrib/ydb/library/yql/dq/runtime/dq_transport.h>

#include <contrib/ydb/library/yql/providers/dq/api/protos/service.pb.h>

#include <library/cpp/testing/common/env.h>

#include <library/cpp/testing/unittest/registar.h>

#include <yql/essentials/minikql/mkql_function_registry.h>
#include <yql/essentials/minikql/mkql_node_builder.h>
#include <yql/essentials/minikql/mkql_node_serialization.h>
#include <yql/essentials/minikql/mkql_program_builder.h>

#include <yql/essentials/minikql/invoke_builtins/mkql_builtins.h>

#include <util/datetime/base.h>

#include <util/folder/tempdir.h>

#include <util/generic/scope.h>
#include <util/generic/size_literals.h>

#include <util/stream/file.h>

#include <util/string/cast.h>

#include <util/system/shellcommand.h>
#include <util/system/thread.h>

#include <atomic>
#include <cerrno>
#include <csignal>
#include <sys/wait.h>

namespace NYql::NTaskRunnerProxy {
namespace {

using namespace NKikimr::NMiniKQL;

NDqProto::TDqTask BuildTask(bool fail, bool block)
{
    TScopedAlloc alloc(__LOCATION__);
    TTypeEnvironment env(alloc);
    const auto registry = CreateFunctionRegistry(CreateBuiltinRegistry());
    TProgramBuilder builder(env, *registry);
    const auto value = builder.NewDataLiteral<ui64>(42);
    auto stream = builder.Iterator(builder.NewList(value.GetStaticType(), {value}), /*dependentNodes*/ {});
    if (fail) {
        stream = builder.Ensure(
            stream,
            builder.NewDataLiteral(false),
            builder.NewDataLiteral<NUdf::EDataSlot::String>("pipe smoke failure"),
            "pipe_smoke",
            /*row*/ 1,
            /*column*/ 1);
    }
    if (block) {
        TCallableBuilder callable(env, "PipeSmokeBlock", stream.GetStaticType());
        stream = TRuntimeNode(callable.Build(), /*isImmediate*/ false);
    }
    const auto program = builder.NewStruct({
        {"Program", stream},
        {"Inputs", builder.NewTuple({})},
        {"Parameters", builder.Arg(builder.NewEmptyStruct().GetStaticType())},
    });

    NDqProto::TDqTask task;
    task.SetId(1);
    task.MutableProgram()->SetRaw(SerializeRuntimeNode(program, env));
    task.MutableProgram()->SetRuntimeVersion(NDqProto::RUNTIME_VERSION_YQL_1_0);
    auto* output = task.AddOutputs();
    output->MutableMap();
    output->AddChannels()->SetId(2);
    return task;
}

class TPipeExecutionTest
{
public:
    const TTempDir Directory;
    const TString PortoContainer = GetTestParam("porto-container", "");
    const TString PidFile = Directory.Name() + "/cache/Slot-1/child.pid";
    const TString RunningFile = Directory.Name() + "/cache/Slot-1/running";
    const IProxyFactory::TPtr Factory;
    const std::shared_ptr<TScopedAlloc> Alloc = std::make_shared<TScopedAlloc>(
        __LOCATION__,
        NKikimr::TAlignedPagePoolCounters(),
        /*initiallyAcquired*/ false);

    TPipeExecutionTest()
        : Factory(CreatePipeFactory(TPipeFactoryOptions{
            .ExecPath = BinaryPath("yt/yql/providers/dq/task_runner/ut/execution/child/pipe_smoke_child"),
            .FileCache = new TFileCache(Directory.Name() + "/cache", /*size*/ 1_MB),
            .Env = {
                {"DQ_PIPE_SMOKE_PID_FILE", PortoContainer.empty() ? PidFile : "/mnt/work/child.pid"},
                {"DQ_PIPE_SMOKE_RUNNING_FILE", PortoContainer.empty() ? RunningFile : "/mnt/work/running"},
            },
            .EnablePorto = !PortoContainer.empty(),
            .MaxProcesses = 0,
            .ContainerName = PortoContainer,
            .PortoCtlPath = GetTestParam("portoctl", "/usr/sbin/portoctl"),
        }))
    { }

    ITaskRunner::TPtr CreateRunner(bool fail = false, bool block = false)
    {
        auto task = BuildTask(fail, block);
        if (!PortoContainer.empty()) {
            Yql::DqsProto::TTaskMeta meta;
            auto* setting = meta.AddSettings();
            setting->SetName("_EnablePorto");
            setting->SetValue("true");
            task.MutableMeta()->PackFrom(meta);
        }
        return Factory->GetOld(Alloc, NDq::TDqTaskSettings(&task));
    }

    int ReadPid() const
    {
        return PortoContainer.empty()
            ? FromString<int>(TFileInput(PidFile).ReadAll())
            : 0;
    }

    void VerifyReaped(int pid) const
    {
        if (!PortoContainer.empty()) {
            TShellCommand command(GetTestParam("portoctl", "/usr/sbin/portoctl"), {
                "list", "-1", PortoContainer + "/Slot-1",
            });
            command.Run().Wait();
            UNIT_ASSERT_C(command.GetExitCode().GetOrElse(-1) == 0, command.GetError());
            UNIT_ASSERT_VALUES_EQUAL(command.GetOutput(), "");
            return;
        }
        int status = 0;
        errno = 0;
        UNIT_ASSERT_VALUES_EQUAL(waitpid(pid, &status, WNOHANG), -1);
        UNIT_ASSERT_VALUES_EQUAL(errno, ECHILD);
        errno = 0;
        UNIT_ASSERT_VALUES_EQUAL(kill(pid, 0), -1);
        UNIT_ASSERT_VALUES_EQUAL(errno, ESRCH);
    }
};

} // namespace

Y_UNIT_TEST_SUITE(PipeExecution)
{
    Y_UNIT_TEST(ExecutesTaskAndTransfersOutput)
    {
        TPipeExecutionTest test;
        auto runner = test.CreateRunner();
        Y_DEFER {
            if (runner) {
                runner->Kill();
            }
        };
        UNIT_ASSERT_VALUES_EQUAL(runner->GetProtocolVersion(), 6);
        UNIT_ASSERT(runner->Prepare().GetResult());
        UNIT_ASSERT_VALUES_EQUAL(runner->Run().GetResult(), static_cast<int>(NDq::ERunStatus::Finished));

        const auto output = runner->GetOutputChannel(2);
        NDq::TDqSerializedBatch data;
        UNIT_ASSERT(output->Pop(data).GetResult());
        UNIT_ASSERT_VALUES_EQUAL(data.RowCount(), 1);
        {
            const auto guard = runner->BindAllocator({});
            const auto registry = CreateFunctionRegistry(CreateBuiltinRegistry());
            TProgramBuilder builder(runner->GetTypeEnv(), *registry);
            const auto* type = builder.NewDataLiteral<ui64>(0).GetStaticType();
            NDq::TDqDataSerializer serializer(
                runner->GetTypeEnv(),
                runner->GetHolderFactory(),
                static_cast<NDqProto::EDataTransportVersion>(data.Proto.GetTransportVersion()),
                NDq::FromProto(data.Proto.GetValuePackerVersion()),
                MakeRuntimeSettings()->DatumValidation.Get());
            TUnboxedValueBatch rows(type);
            serializer.Deserialize(std::move(data), type, rows);
            int rowCount = 0;
            rows.ForEachRow([&] (const NUdf::TUnboxedValue& row) {
                UNIT_ASSERT_VALUES_EQUAL(row.Get<ui64>(), 42);
                ++rowCount;
            });
            UNIT_ASSERT_VALUES_EQUAL(rowCount, 1);
        }
        UNIT_ASSERT(output->IsFinished());
        const auto status = runner->GetStatus();
        UNIT_ASSERT_C(WIFEXITED(status.ExitCode), status.Stderr);
        UNIT_ASSERT_C(WEXITSTATUS(status.ExitCode) == 0, status.Stderr);
        const int pid = test.ReadPid();
        runner.Reset();
        test.VerifyReaped(pid);
    }

    Y_UNIT_TEST(ReportsChildComputationFailure)
    {
        TPipeExecutionTest test;
        const auto previousHandler = signal(SIGPIPE, SIG_IGN);
        Y_DEFER {
            signal(SIGPIPE, previousHandler);
        };
        auto runner = test.CreateRunner(/*fail*/ true);
        Y_DEFER {
            if (runner) {
                runner->Kill();
            }
        };
        UNIT_ASSERT(runner->Prepare().GetResult());
        UNIT_ASSERT_EXCEPTION(runner->Run(), std::exception);
        const auto status = runner->GetStatus();
        UNIT_ASSERT(WIFEXITED(status.ExitCode));
        UNIT_ASSERT_VALUES_EQUAL(WEXITSTATUS(status.ExitCode), 127);
        UNIT_ASSERT_STRING_CONTAINS(status.Stderr, "pipe smoke failure");
        const int pid = test.ReadPid();
        runner.Reset();
        test.VerifyReaped(pid);
    }

    Y_UNIT_TEST(KillsAndReapsRunningChild)
    {
        TPipeExecutionTest test;
        auto runner = test.CreateRunner(/*fail*/ false, /*block*/ true);
        Y_DEFER {
            if (runner) {
                runner->Kill();
            }
        };
        UNIT_ASSERT(runner->Prepare().GetResult());
        std::atomic<bool> runFailed = false;
        TThread thread([&] {
            try {
                runner->Run();
            } catch (const std::exception&) {
                runFailed = true;
            }
        });
        thread.Start();
        bool joined = false;
        Y_DEFER {
            if (!joined) {
                runner->Kill();
                thread.Join();
            }
        };
        const auto deadline = TInstant::Now() + TDuration::Seconds(10);
        while (!TFsPath(test.RunningFile).Exists() && TInstant::Now() < deadline) {
            Sleep(TDuration::MilliSeconds(10));
        }
        UNIT_ASSERT(TFsPath(test.RunningFile).Exists());
        runner->Kill();
        thread.Join();
        joined = true;
        UNIT_ASSERT(runFailed);
        const int pid = test.ReadPid();
        runner.Reset();
        test.VerifyReaped(pid);
    }

    Y_UNIT_TEST(KillsAndReapsChild)
    {
        TPipeExecutionTest test;
        auto runner = test.CreateRunner();
        Y_DEFER {
            if (runner) {
                runner->Kill();
            }
        };
        UNIT_ASSERT_VALUES_EQUAL(runner->GetProtocolVersion(), 6);
        runner->Kill();
        const int pid = test.ReadPid();
        runner.Reset();
        test.VerifyReaped(pid);
    }
}

} // namespace NYql::NTaskRunnerProxy
