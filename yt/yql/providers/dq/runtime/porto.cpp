#include "porto.h"

#include <yql/essentials/utils/log/log.h>

#include <yql/essentials/utils/yql_panic.h>

#include <util/generic/list.h>

#include <util/system/fs.h>
#include <util/system/shellcommand.h>
#include <util/system/sysstat.h>

namespace NYql::NDq {

namespace {

void RunCommand(const TString& executable, const TList<TString>& args)
{
    TShellCommandOptions options;
    options.SetUseShell(false);
    TShellCommand command(executable, args, options);
    command.Run().Wait();
    YQL_ENSURE(command.GetExitCode().Defined() && *command.GetExitCode() == 0,
        "Porto setup command failed: " << executable << " " << args.front()
        << ", stderr: " << command.GetError());
}

} // namespace

TString PrepareTaskRunnerPorto(
    const NProto::TDqConfig::TYtBackend& config,
    const TString& fileCacheDir,
    NTaskRunnerProxy::TPipeFactoryOptions* options,
    ETaskRunnerPortoRoot root,
    const TString& jobRootPortoCtlPath)
{
    options->EnablePorto = config.GetEnablePorto() == "isolate";
    options->EnablePortoAnonLimitRaiseBeforeDestroy = config.GetEnablePortoAnonLimitRaiseBeforeDestroy();
    options->ContainerName = "Outer";
    options->PortoLayer.clear();

    if (root == ETaskRunnerPortoRoot::JobRoot && !options->EnablePorto) {
        return {};
    }

    TString layerDir;
    if (root == ETaskRunnerPortoRoot::SandboxArchives && !config.GetPortoLayer().empty()) {
        layerDir = fileCacheDir + "/layer";
        NFs::MakeDirectoryRecursive(layerDir + "/mnt/work");
        for (const auto& layer : config.GetPortoLayer()) {
            const auto archive = layer.substr(layer.rfind('/') + 1);
            TShellCommandOptions commandOptions;
            commandOptions.SetUseShell(false);
            TShellCommand command("tar", {"xf", archive, "-C", layerDir}, commandOptions);
            command.Run().Wait();
            // Unprivileged workers cannot recreate device entries from layers.
            // Keep the files tar managed to extract, as worker_job did before.
            if (!command.GetExitCode().Defined() || *command.GetExitCode() != 0) {
                YQL_CLOG(ERROR, ProviderDq) << "'tar' exited with non-zero code, stderr: " << command.GetError();
            }
        }
        options->PortoLayer = layerDir;
    } else if (root == ETaskRunnerPortoRoot::SandboxArchives) {
        NFs::MakeDirectoryRecursive("mnt/work");
        NFs::MakeDirectoryRecursive("usr/local/bin");
        return {};
    }

    // An empty layer selects '/' in the pipe factory: reuse the vanilla job's
    // root populated by layer_paths, without requiring archives in its sandbox.
    const TString portoCtl = layerDir.empty() ? jobRootPortoCtlPath : layerDir + "/usr/bin/portoctl";
    if (!NFs::Exists(portoCtl)) {
        return {};
    }

    YQL_ENSURE(options->FileCache, "Porto setup requires a file cache");
    const TString cachedPortoCtl = options->FileCache->GetDir() + "/portoctl";
    NFs::Copy(portoCtl, cachedPortoCtl);
    if (!layerDir.empty()) {
        // Workaround PORTO-997, confined to the unpacked task-runner layer.
        NFs::HardLink(layerDir + "/usr/bin/portoctl", layerDir + "/usr/sbin/portoctl");
    }
    YQL_ENSURE(Chmod(cachedPortoCtl.c_str(), MODE0755) == 0,
        "Cannot make cached portoctl executable: " << cachedPortoCtl);
    options->PortoCtlPath = cachedPortoCtl;
    return cachedPortoCtl;
}

void ConfigureTaskRunnerPorto(
    const NProto::TDqConfig::TYtBackend& config,
    const NTaskRunnerProxy::TPipeFactoryOptions& options)
{
    if (!options.EnablePorto) {
        return;
    }
    YQL_CLOG(INFO, ProviderDq) << "Configure task-runner Porto container=" << options.ContainerName
        << " root=" << (options.PortoLayer.empty() ? "/" : options.PortoLayer);
    RunCommand(options.PortoCtlPath, {"create", options.ContainerName});
    RunCommand(options.PortoCtlPath, {"set", options.ContainerName, "enable_porto", "isolate"});
    RunCommand(options.PortoCtlPath, {"set", options.ContainerName, "respawn", "true"});
    for (const auto& setting : config.GetPortoSettings().GetSetting()) {
        RunCommand(options.PortoCtlPath, {"set", options.ContainerName, setting.GetName(), setting.GetValue()});
    }
    RunCommand(options.PortoCtlPath, {"start", options.ContainerName});
    RunCommand(options.PortoCtlPath, {"wait", options.ContainerName});
}

} // namespace NYql::NDq
