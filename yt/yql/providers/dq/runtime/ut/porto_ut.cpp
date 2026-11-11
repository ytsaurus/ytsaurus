#include <yt/yql/providers/dq/runtime/file_cache.h>
#include <yt/yql/providers/dq/runtime/porto.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/datetime/base.h>

#include <util/folder/tempdir.h>

#include <util/generic/scope.h>
#include <util/generic/size_literals.h>

#include <util/stream/file.h>

#include <util/string/cast.h>
#include <util/string/strip.h>

#include <util/system/file.h>
#include <util/system/fs.h>
#include <util/system/shellcommand.h>
#include <util/system/sysstat.h>

#include <cerrno>
#include <csignal>
#include <sys/wait.h>

using namespace NYql;
using namespace NYql::NDq;

namespace {

void WriteScript(const TString& path, const TString& body)
{
    TFileOutput output(path);
    output << "#!/bin/sh\n" << body;
    output.Finish();
    UNIT_ASSERT_VALUES_EQUAL(Chmod(path.c_str(), MODE0755), 0);
}

TString RunPortoTaskRunner(NTaskRunnerProxy::TPipeFactoryOptions options, const TString& executable)
{
    options.ExecPath = executable;
    auto factory = NTaskRunnerProxy::CreatePipeFactory(options);
    const auto deadline = TInstant::Now() + TDuration::Seconds(5);
    while (!NFs::Exists(options.PortoCtlPath + ".executed")) {
        UNIT_ASSERT_C(TInstant::Now() < deadline, "Task runner did not invoke portoctl");
        Sleep(TDuration::MilliSeconds(10));
    }
    return TFileInput(options.PortoCtlPath + ".exec").ReadAll();
}

void CheckFailedPoolStart(bool initialWarmup, bool useOldApi = false, bool shutdownImmediately = false)
{
    TTempDir root;
    const auto previousDirectory = NFs::CurrentWorkingDirectory();
    Y_DEFER { NFs::SetCurrentWorkingDirectory(previousDirectory); };
    NFs::SetCurrentWorkingDirectory(root.Path().GetPath());

    NTaskRunnerProxy::TPipeFactoryOptions options;
    options.ExecPath = (root.Path() / "runner").GetPath();
    WriteScript(options.ExecPath, "exit 0\n");
    options.FileCache = new TFileCache("cache", 1_MB);
    options.EnablePorto = true;
    options.UseJobRoot = true;
    options.EnablePortoAnonLimitRaiseBeforeDestroy = false;
    options.MaxProcesses = initialWarmup ? 3 : 2;
    options.Counters = MakeIntrusive<NMonitoring::TDynamicCounters>();
    options.PortoCtlPath = (root.Path() / "portoctl").GetPath();
    WriteScript(options.PortoCtlPath,
        "case \"$1\" in\n"
        "exec) echo $$ > \"$0.${2##*/}.pid.tmp\"; "
        "mv \"$0.${2##*/}.pid.tmp\" \"$0.${2##*/}.pid\"; exec /bin/sleep 60;;\n"
        "destroy) exit 0;;\n"
        "*) exit 1;;\n"
        "esac\n");

    // Keep the regression test itself from leaking children if cleanup is broken.
    TVector<int> pids;
    Y_DEFER {
        for (const auto pid : pids) {
            int status;
            if (waitpid(pid, &status, WNOHANG) == 0) {
                kill(pid, SIGKILL);
                while (waitpid(pid, &status, 0) == -1 && errno == EINTR) { }
            }
        }
    };
    NTaskRunnerProxy::IProxyFactory::TPtr factory;
    // The first two runners start; preparation of the third fails before fork.
    NFs::MakeDirectoryRecursive("TmpDirSlot-3/bin/runner");
    if (initialWarmup) {
        UNIT_ASSERT_EXCEPTION_CONTAINS(
            NTaskRunnerProxy::CreatePipeFactory(options), TFileError, "runner");
    } else {
        factory = NTaskRunnerProxy::CreatePipeFactory(options);
    }
    const auto deadline = TInstant::Now() + TDuration::Seconds(5);
    for (int slot = 1; slot <= 2; ++slot) {
        const auto pidFile = options.PortoCtlPath + ".Slot-" + ToString(slot) + ".pid";
        // On constructor failure cleanup may kill a child before it reaches exec.
        while ((!initialWarmup || options.Counters->FindCounter("PortoContainersDestroyed")->Val() != 2)
            && !NFs::Exists(pidFile)) {
            UNIT_ASSERT_C(TInstant::Now() < deadline, "Runner did not start");
            Sleep(TDuration::MilliSeconds(10));
        }
        if (NFs::Exists(pidFile)) {
            pids.push_back(FromString<int>(Strip(TFileInput(pidFile).ReadAll())));
        }
    }
    if (!initialWarmup) {
        Yql::DqsProto::TTaskMeta meta;
        auto* setting = meta.AddSettings();
        setting->SetName("_EnablePorto");
        setting->SetValue("true");
        setting = meta.AddSettings();
        setting->SetName("_PortoMemoryLimit");
        setting->SetValue("1024"); // Evict both warm runners by changing the pool key.
        NDqProto::TDqTask task;
        task.MutableMeta()->PackFrom(meta);
        if (useOldApi) {
            UNIT_ASSERT_EXCEPTION_CONTAINS(factory->GetOld(nullptr, NDq::TDqTaskSettings(&task)), TFileError, "runner");
        } else {
            UNIT_ASSERT_EXCEPTION_CONTAINS(
                factory->Get(nullptr, NDq::TDqTaskSettings(&task), NDqProto::DQ_STATS_MODE_NONE), TFileError, "runner");
        }
        if (shutdownImmediately) {
            factory = nullptr;
        }
    }

    const auto count = [&](const TString& name) { return options.Counters->FindCounter(name)->Val(); };
    while (count("PortoContainersAlive") != 0 || count("ProcessesPendingStop") != 0) {
        UNIT_ASSERT_C(TInstant::Now() < deadline, "Started runners were not stopped after preparation failed");
        Sleep(TDuration::MilliSeconds(10));
    }
    UNIT_ASSERT_VALUES_EQUAL(count("PortoContainersStarted"), 2);
    UNIT_ASSERT_VALUES_EQUAL(count("PortoContainersDestroyed"), 2);
    UNIT_ASSERT_VALUES_EQUAL(count("ProcessesInPool"), 0);
    UNIT_ASSERT_VALUES_EQUAL(count("PortoOwnerReleasedWithoutDestroyAttempt"), 0);
    UNIT_ASSERT_VALUES_EQUAL(count("PoolEvictedOnKeyMismatch"), initialWarmup ? 0 : 2);
    for (const auto pid : pids) {
        int status;
        UNIT_ASSERT_VALUES_EQUAL(waitpid(pid, &status, WNOHANG), -1);
        UNIT_ASSERT_VALUES_EQUAL(errno, ECHILD); // Wait() must have reaped the child.
    }
}

} // namespace

Y_UNIT_TEST_SUITE(TTaskRunnerPortoTest) {
    Y_UNIT_TEST(DisabledPortoDoesNotTouchFilesystemOrStartPorto) {
        for (bool withPortoLayer : {false, true}) {
            for (bool withOperationLayer : {false, true}) {
                TTempDir root;
                const auto cacheDir = (root.Path() / "cache").GetPath();
                NProto::TDqConfig::TYtBackend config;
                config.SetEnablePorto("none");
                if (withPortoLayer) {
                    config.AddPortoLayer("//missing/layer.tar");
                }
                if (withOperationLayer) {
                    config.AddOperationLayer("//missing/outer.tar");
                }
                NTaskRunnerProxy::TPipeFactoryOptions options;
                options.PortoCtlPath = "/missing/portoctl";

                UNIT_ASSERT(PrepareTaskRunnerPorto(config, cacheDir, &options).empty());
                UNIT_ASSERT(!options.EnablePorto);
                UNIT_ASSERT(options.PortoLayer.empty());
                UNIT_ASSERT(!NFs::Exists(cacheDir));
                ConfigureTaskRunnerPorto(config, options);
            }
        }
    }

    void CheckJobRoot(bool failHardLink) {
        TTempDir root;
        const auto previousDirectory = NFs::CurrentWorkingDirectory();
        Y_DEFER { NFs::SetCurrentWorkingDirectory(previousDirectory); };
        NFs::SetCurrentWorkingDirectory(root.Path().GetPath());

        const auto cacheDir = (root.Path() / "cache").GetPath();
        const auto sourcePortoCtl = (root.Path() / "portoctl").GetPath();
        WriteScript(sourcePortoCtl,
            "printf '%s\\n' \"$*\" >> \"$0.calls\"\n"
            "if [ \"$1\" = exec ]; then\n"
            "    printf '%s\\n' \"$@\" > \"$0.exec\"\n"
            "    : > \"$0.executed\"\n"
            "fi\n");
        NProto::TDqConfig::TYtBackend config;
        config.SetEnablePorto("isolate");
        config.SetEnablePortoAnonLimitRaiseBeforeDestroy(false);
        config.AddOperationLayer("//not-in-sandbox/layer.tar");
        NTaskRunnerProxy::TPipeFactoryOptions options;
        options.FileCache = new TFileCache(cacheDir, 1_MB);
        if (failHardLink) {
            NFs::MakeDirectory(cacheDir + "/Slot-1");
            // An occupied destination reliably exercises the same fallback as EXDEV
            // when YT mounts the executable separately from the file-cache tmpfs.
            TFileOutput(cacheDir + "/Slot-1/portoctl").Finish();
        }

        const auto guardFile = PrepareTaskRunnerPorto(
            config, root.Path().GetPath(), &options, sourcePortoCtl);
        UNIT_ASSERT_VALUES_EQUAL(guardFile, cacheDir + "/portoctl");
        UNIT_ASSERT_VALUES_EQUAL(options.PortoCtlPath, guardFile);
        UNIT_ASSERT(NFs::Exists(guardFile));
        UNIT_ASSERT_VALUES_EQUAL(TFileInput(guardFile).ReadAll(), TFileInput(sourcePortoCtl).ReadAll());
        UNIT_ASSERT(options.EnablePorto);
        UNIT_ASSERT(options.UseJobRoot);
        UNIT_ASSERT(!options.EnablePortoAnonLimitRaiseBeforeDestroy);
        UNIT_ASSERT(options.PortoLayer.empty());
        UNIT_ASSERT_VALUES_EQUAL(options.ContainerName, "Outer");
        UNIT_ASSERT(!NFs::Exists((root.Path() / "layer").GetPath()));
        ConfigureTaskRunnerPorto(config, options);
        UNIT_ASSERT_VALUES_EQUAL(TFileInput(guardFile + ".calls").ReadAll(),
            "create Outer\n"
            "set Outer enable_porto isolate\n"
            "set Outer respawn true\n"
            "start Outer\n"
            "wait Outer\n");

        const auto args = RunPortoTaskRunner(options, sourcePortoCtl);
        UNIT_ASSERT_C(args.StartsWith("exec\nOuter/Slot-1\n"), args);
        UNIT_ASSERT_C(args.Contains("TmpDirSlot-1/mnt /mnt ro;" + cacheDir + "/Slot-1 mnt/work rw;"), args);
        const auto executable = failHardLink ? sourcePortoCtl : cacheDir + "/Slot-1/portoctl";
        UNIT_ASSERT_C(args.Contains("bind=TmpDirSlot-1/bin usr/local/bin ro;"
            + executable + " usr/local/bin/portoctl ro;"), args);
        UNIT_ASSERT_C(!args.Contains("TmpDirSlot-1/empty " + cacheDir + " ro;"), args);
        UNIT_ASSERT_C(args.Contains("TmpDirSlot-1/empty " + NFs::CurrentWorkingDirectory() + " ro;"), args);
        UNIT_ASSERT_C(args.Contains("root_readonly=true\n"), args);
        UNIT_ASSERT_C(args.Contains("root=/\n"), args);
        UNIT_ASSERT_C(!args.Contains("\n-L\n"), args);
    }

    Y_UNIT_TEST(JobRootDoesNotRequireLayerArchives) {
        CheckJobRoot(false);
    }

    Y_UNIT_TEST(JobRootBindsExecutableWhenHardLinkFails) {
        CheckJobRoot(true);
    }

    Y_UNIT_TEST(JobRootPreparationFailureDoesNotLeakPipes) {
        TTempDir root;
        const auto previousDirectory = NFs::CurrentWorkingDirectory();
        Y_DEFER { NFs::SetCurrentWorkingDirectory(previousDirectory); };
        NFs::SetCurrentWorkingDirectory(root.Path().GetPath());

        const auto executable = (root.Path() / "runner").GetPath();
        WriteScript(executable, "exit 0\n");
        NTaskRunnerProxy::TPipeFactoryOptions options;
        options.ExecPath = executable;
        options.FileCache = new TFileCache("cache", 1_MB);
        options.EnablePorto = true;
        options.UseJobRoot = true;
        options.MaxProcesses = 1;

        // Opening a file allocates the lowest unused FD. Leaked pipes would move it.
        const auto nextFd = TFile("/dev/null", RdOnly).GetHandle();
        for (int attempt = 0; attempt < 3; ++attempt) {
            // A directory at the file mountpoint makes TFileOutput fail reliably,
            // without filling a disk or relying on the test user's permissions.
            NFs::MakeDirectoryRecursive("TmpDirSlot-1/bin/runner");
            UNIT_ASSERT_EXCEPTION_CONTAINS(
                NTaskRunnerProxy::CreatePipeFactory(options), TFileError, "runner");
            UNIT_ASSERT_VALUES_EQUAL(TFile("/dev/null", RdOnly).GetHandle(), nextFd);
        }
    }

    Y_UNIT_TEST(JobRootFailedReplacementStopsEvictedRunners) {
        for (bool useOldApi : {false, true}) {
            CheckFailedPoolStart(/*initialWarmup*/ false, useOldApi);
        }
    }

    Y_UNIT_TEST(JobRootFailedReplacementCanShutDownImmediately) {
        CheckFailedPoolStart(/*initialWarmup*/ false, /*useOldApi*/ false, /*shutdownImmediately*/ true);
    }

    Y_UNIT_TEST(JobRootFailedWarmupStopsStartedRunners) {
        CheckFailedPoolStart(/*initialWarmup*/ true);
    }

    Y_UNIT_TEST(JobRootRejectsCacheOutsideSandbox) {
        TTempDir root;
        const auto previousDirectory = NFs::CurrentWorkingDirectory();
        Y_DEFER { NFs::SetCurrentWorkingDirectory(previousDirectory); };
        const auto sandbox = root.Path() / "sandbox";
        sandbox.MkDir();
        NFs::SetCurrentWorkingDirectory(sandbox.GetPath());

        NTaskRunnerProxy::TPipeFactoryOptions options;
        options.ExecPath = (sandbox / "runner").GetPath();
        WriteScript(options.ExecPath, "exit 0\n");
        options.FileCache = new TFileCache((root.Path() / "cache").GetPath(), 1_MB);
        options.EnablePorto = true;
        options.UseJobRoot = true;
        options.MaxProcesses = 1;

        UNIT_ASSERT_EXCEPTION_CONTAINS(NTaskRunnerProxy::CreatePipeFactory(options), yexception,
            "Job-root Porto requires the file cache to be inside the job sandbox");
    }

    Y_UNIT_TEST(UnpacksWorkerLayerAndConfiguresOuterContainer) {
        for (bool withOperationLayer : {false, true}) {
            TTempDir root;
            const auto previousDirectory = NFs::CurrentWorkingDirectory();
            Y_DEFER { NFs::SetCurrentWorkingDirectory(previousDirectory); };
            NFs::SetCurrentWorkingDirectory(root.Path().GetPath());

            const auto layer = root.Path() / "source";
            (layer / "usr/bin").MkDirs();
            (layer / "usr/sbin").MkDirs();
            WriteScript((layer / "usr/bin/portoctl").GetPath(),
                "printf '%s\\n' \"$*\" >> \"$0.calls\"\n"
                "if [ \"$1\" = exec ]; then\n"
                "    printf '%s\\n' \"$@\" > \"$0.exec\"\n"
                "    : > \"$0.executed\"\n"
                "fi\n");
            TShellCommandOptions commandOptions;
            commandOptions.SetUseShell(false);
            TShellCommand archive("tar", {"cf", "layer.tar", "-C", layer.GetPath(), "."}, commandOptions);
            archive.Run().Wait();
            UNIT_ASSERT(archive.GetExitCode().Defined());
            UNIT_ASSERT_VALUES_EQUAL(*archive.GetExitCode(), 0);

            const auto cacheDir = (root.Path() / "file_cache").GetPath();
            NProto::TDqConfig::TYtBackend config;
            config.SetEnablePorto("isolate");
            config.AddPortoLayer("//layers/layer.tar");
            if (withOperationLayer) {
                config.AddOperationLayer("//layers/outer.tar");
            }
            auto* setting = config.MutablePortoSettings()->AddSetting();
            setting->SetName("memory_limit");
            setting->SetValue("42");
            NTaskRunnerProxy::TPipeFactoryOptions options;
            options.FileCache = new TFileCache(cacheDir + "/cache", 1_MB);

            const auto guardFile = PrepareTaskRunnerPorto(config, cacheDir, &options);
            UNIT_ASSERT_VALUES_EQUAL(guardFile, options.PortoCtlPath);
            UNIT_ASSERT_VALUES_EQUAL(options.PortoLayer, cacheDir + "/layer");
            UNIT_ASSERT(!options.UseJobRoot);
            UNIT_ASSERT(NFs::Exists(options.PortoLayer + "/mnt/work"));
            UNIT_ASSERT(NFs::Exists(options.PortoLayer + "/usr/sbin/portoctl"));
            ConfigureTaskRunnerPorto(config, options);
            UNIT_ASSERT_VALUES_EQUAL(TFileInput(guardFile + ".calls").ReadAll(),
                "create Outer\n"
                "set Outer enable_porto isolate\n"
                "set Outer respawn true\n"
                "set Outer memory_limit 42\n"
                "start Outer\n"
                "wait Outer\n");
            const auto args = RunPortoTaskRunner(options, guardFile);
            UNIT_ASSERT_C(args.StartsWith("exec\n-L\n" + options.PortoLayer + "\nOuter/Slot-1\n"), args);
            UNIT_ASSERT_C(args.Contains("bind=" + cacheDir + "/cache/Slot-1 mnt/work rw;"
                + cacheDir + "/cache/Slot-1 usr/local/bin ro;TmpDirSlot-1 /tmp rw;\n"), args);
            UNIT_ASSERT_C(!args.Contains("\nroot=/\n"), args);
        }
    }

    Y_UNIT_TEST(WorkerWithoutLayersKeepsDefaultPortoCtl) {
        TTempDir root;
        const auto previousDirectory = NFs::CurrentWorkingDirectory();
        Y_DEFER { NFs::SetCurrentWorkingDirectory(previousDirectory); };
        NFs::SetCurrentWorkingDirectory(root.Path().GetPath());
        NProto::TDqConfig::TYtBackend config;
        config.SetEnablePorto("isolate");
        NTaskRunnerProxy::TPipeFactoryOptions options;
        options.PortoCtlPath = "/worker/porto";

        UNIT_ASSERT(PrepareTaskRunnerPorto(config, "cache", &options, "/missing/portoctl").empty());
        UNIT_ASSERT(options.EnablePorto);
        UNIT_ASSERT(options.PortoLayer.empty());
        UNIT_ASSERT(!options.UseJobRoot);
        UNIT_ASSERT_VALUES_EQUAL(options.PortoCtlPath, "/worker/porto");
        UNIT_ASSERT(NFs::Exists("mnt/work"));
        UNIT_ASSERT(NFs::Exists("usr/local/bin"));
    }

    Y_UNIT_TEST(WorkerWithoutLayersFindsJobRootPortoCtl) {
        TTempDir root;
        const auto previousDirectory = NFs::CurrentWorkingDirectory();
        Y_DEFER { NFs::SetCurrentWorkingDirectory(previousDirectory); };
        NFs::SetCurrentWorkingDirectory(root.Path().GetPath());
        const auto sourcePortoCtl = (root.Path() / "portoctl").GetPath();
        WriteScript(sourcePortoCtl, "exit 0\n");
        NProto::TDqConfig::TYtBackend config;
        config.SetEnablePorto("isolate");
        NTaskRunnerProxy::TPipeFactoryOptions options;
        options.FileCache = new TFileCache("cache", 1_MB);

        const auto guardFile = PrepareTaskRunnerPorto(config, "cache", &options, sourcePortoCtl);
        UNIT_ASSERT_VALUES_EQUAL(guardFile, options.FileCache->GetDir() + "/portoctl");
        UNIT_ASSERT_VALUES_EQUAL(options.PortoCtlPath, guardFile);
        UNIT_ASSERT_VALUES_EQUAL(TFileInput(guardFile).ReadAll(), TFileInput(sourcePortoCtl).ReadAll());
        UNIT_ASSERT(!options.UseJobRoot);
        UNIT_ASSERT(options.PortoLayer.empty());
    }

    Y_UNIT_TEST(PortoSetupFailureIsReportedAtStartup) {
        TTempDir root;
        const auto portoCtl = (root.Path() / "portoctl").GetPath();
        WriteScript(portoCtl, "echo simulated_failure >&2\nexit 7\n");
        NProto::TDqConfig::TYtBackend config;
        NTaskRunnerProxy::TPipeFactoryOptions options;
        options.EnablePorto = true;
        options.ContainerName = "Outer";
        options.PortoCtlPath = portoCtl;

        UNIT_ASSERT_EXCEPTION_CONTAINS(ConfigureTaskRunnerPorto(config, options), yexception, "simulated_failure");
    }
}
