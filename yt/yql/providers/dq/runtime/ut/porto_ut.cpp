#include <yt/yql/providers/dq/runtime/file_cache.h>
#include <yt/yql/providers/dq/runtime/porto.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/folder/tempdir.h>

#include <util/generic/scope.h>
#include <util/generic/size_literals.h>

#include <util/stream/file.h>

#include <util/system/fs.h>
#include <util/system/shellcommand.h>
#include <util/system/sysstat.h>

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

} // namespace

Y_UNIT_TEST_SUITE(TTaskRunnerPortoTest) {
    Y_UNIT_TEST(DisabledJobRootDoesNotTouchFilesystemOrStartPorto) {
        NProto::TDqConfig::TYtBackend config;
        config.SetEnablePorto("none");
        config.AddPortoLayer("//missing/layer.tar");
        NTaskRunnerProxy::TPipeFactoryOptions options;
        options.PortoCtlPath = "/missing/portoctl";

        UNIT_ASSERT(PrepareTaskRunnerPorto(config, "/missing/cache", &options, ETaskRunnerPortoRoot::JobRoot).empty());
        UNIT_ASSERT(!options.EnablePorto);
        ConfigureTaskRunnerPorto(config, options);
    }

    Y_UNIT_TEST(JobRootDoesNotRequireLayerArchives) {
        TTempDir root;
        const auto cacheDir = (root.Path() / "cache").GetPath();
        const auto sourcePortoCtl = (root.Path() / "portoctl").GetPath();
        WriteScript(sourcePortoCtl, "printf '%s\\n' \"$*\" >> \"$0.calls\"\n");
        NProto::TDqConfig::TYtBackend config;
        config.SetEnablePorto("isolate");
        config.SetEnablePortoAnonLimitRaiseBeforeDestroy(false);
        config.AddPortoLayer("//not-in-sandbox/layer.tar");
        NTaskRunnerProxy::TPipeFactoryOptions options;
        options.FileCache = new TFileCache(cacheDir, 1_MB);

        const auto guardFile = PrepareTaskRunnerPorto(
            config, root.Path().GetPath(), &options, ETaskRunnerPortoRoot::JobRoot, sourcePortoCtl);
        UNIT_ASSERT_VALUES_EQUAL(guardFile, cacheDir + "/portoctl");
        UNIT_ASSERT_VALUES_EQUAL(options.PortoCtlPath, guardFile);
        UNIT_ASSERT(NFs::Exists(guardFile));
        UNIT_ASSERT_VALUES_EQUAL(TFileInput(guardFile).ReadAll(), TFileInput(sourcePortoCtl).ReadAll());
        UNIT_ASSERT(options.EnablePorto);
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
    }

    Y_UNIT_TEST(UnpacksWorkerLayerAndConfiguresOuterContainer) {
        TTempDir root;
        const auto previousDirectory = NFs::CurrentWorkingDirectory();
        Y_DEFER { NFs::SetCurrentWorkingDirectory(previousDirectory); };
        NFs::SetCurrentWorkingDirectory(root.Path().GetPath());

        const auto layer = root.Path() / "source";
        (layer / "usr/bin").MkDirs();
        (layer / "usr/sbin").MkDirs();
        WriteScript((layer / "usr/bin/portoctl").GetPath(), "printf '%s\\n' \"$*\" >> \"$0.calls\"\n");
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
        auto* setting = config.MutablePortoSettings()->AddSetting();
        setting->SetName("memory_limit");
        setting->SetValue("42");
        NTaskRunnerProxy::TPipeFactoryOptions options;
        options.FileCache = new TFileCache(cacheDir + "/cache", 1_MB);

        const auto guardFile = PrepareTaskRunnerPorto(config, cacheDir, &options, ETaskRunnerPortoRoot::SandboxArchives);
        UNIT_ASSERT_VALUES_EQUAL(guardFile, options.PortoCtlPath);
        UNIT_ASSERT_VALUES_EQUAL(options.PortoLayer, cacheDir + "/layer");
        UNIT_ASSERT(NFs::Exists(options.PortoLayer + "/usr/sbin/portoctl"));
        ConfigureTaskRunnerPorto(config, options);
        UNIT_ASSERT_VALUES_EQUAL(TFileInput(guardFile + ".calls").ReadAll(),
            "create Outer\n"
            "set Outer enable_porto isolate\n"
            "set Outer respawn true\n"
            "set Outer memory_limit 42\n"
            "start Outer\n"
            "wait Outer\n");
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

        UNIT_ASSERT(PrepareTaskRunnerPorto(config, "cache", &options, ETaskRunnerPortoRoot::SandboxArchives).empty());
        UNIT_ASSERT(options.EnablePorto);
        UNIT_ASSERT(options.PortoLayer.empty());
        UNIT_ASSERT_VALUES_EQUAL(options.PortoCtlPath, "/worker/porto");
        UNIT_ASSERT(NFs::Exists("mnt/work"));
        UNIT_ASSERT(NFs::Exists("usr/local/bin"));
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
