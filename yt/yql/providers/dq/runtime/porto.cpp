#include "porto.h"

#include <yql/essentials/utils/log/log.h>

#include <yql/essentials/utils/yql_panic.h>

#include <util/datetime/base.h>

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
    const TString& jobRootPortoCtlPath)
{
    options->EnablePorto = config.GetEnablePorto() == "isolate";
    options->EnablePortoAnonLimitRaiseBeforeDestroy = config.GetEnablePortoAnonLimitRaiseBeforeDestroy();
    options->ContainerName = "Outer";
    options->PortoLayer.clear();
    options->UseJobRoot = config.GetPortoLayer().empty() && !config.GetOperationLayer().empty();

    if (!options->EnablePorto) {
        return {};
    }

    const auto startedAt = TInstant::Now();
    YQL_CLOG(INFO, ProviderDq) << "Preparing task-runner Porto use_job_root=" << options->UseJobRoot;
    TString layerDir;
    if (!options->UseJobRoot && !config.GetPortoLayer().empty()) {
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
    } else if (!options->UseJobRoot) {
        NFs::MakeDirectoryRecursive("mnt/work");
        NFs::MakeDirectoryRecursive("usr/local/bin");
    }

    // In job-root mode YT prepares layer_paths before starting the job.
    // No archives or mutable copy of the root are needed in its sandbox.
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
    YQL_CLOG(INFO, ProviderDq) << "Prepared task-runner Porto use_job_root=" << options->UseJobRoot
        << " elapsed_ms=" << (TInstant::Now() - startedAt).MilliSeconds();
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
        << " use_job_root=" << options.UseJobRoot
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
