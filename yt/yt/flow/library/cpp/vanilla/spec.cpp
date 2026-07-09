#include "spec.h"

#include <yt/yt/core/ytree/convert.h>
#include <yt/yt/core/ytree/ephemeral_node_factory.h>
#include <yt/yt/core/ytree/fluent.h>

#include <yt/yt/core/yson/string.h>

#include <yt/yt/library/auth/auth.h>

#include <util/system/env.h>

namespace NYT::NFlow {

using namespace NYTree;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

IMapNodePtr BuildVanillaOperationSpec(const TVanillaSpec& spec)
{
    // clang-format off
    auto node = BuildYsonNodeFluently()
        .BeginMap()
            .Item("max_speculative_job_count_per_task").Value(0)
            .Item("max_failed_job_count").Value(spec.MaxFailedJobCount)
            .Item("alias").Value(spec.Alias)
            .Item("secret_env").Value(spec.SecretEnv)
            .DoIf(spec.Pool.has_value(), [&] (auto fluent) {
                fluent.Item("pool").Value(*spec.Pool);
            })
            .DoIf(spec.Title.has_value(), [&] (auto fluent) {
                fluent.Item("title").Value(*spec.Title);
            })
            .DoIf(spec.UsePorto, [&] (auto fluent) {
                fluent.Item("scheduling_tag_filter").Value("porto");
            })
            .DoIf(static_cast<bool>(spec.Description), [&] (auto fluent) {
                fluent.Item("description").Value(spec.Description);
            })
            .Item("annotations").BeginMap()
                .Item("solomon_resolver_tag").Value(spec.SolomonResolverTag)
                .Item("solomon_resolver_ports").BeginList()
                    .Item().Value(spec.MonitoringPort)
                .EndList()
            .EndMap()
            .Item("tasks").DoMapFor(spec.Tasks, [&] (auto fluent, const TVanillaTaskSpec& task) {
                fluent.Item(task.Name).BeginMap()
                    .Item("job_count").Value(task.JobCount)
                    .Item("command").Value(task.Command)
                    .Item("memory_limit").Value(task.MemoryLimit)
                    .Item("cpu_limit").Value(task.CpuLimit)
                    .DoIf(task.PortCount > 0, [&] (auto fluent) {
                        fluent.Item("port_count").Value(task.PortCount);
                    })
                    .Item("file_paths").DoListFor(task.Files, [&] (auto fluent, const TVanillaJobFile& file) {
                        fluent.Item()
                            .BeginAttributes()
                                .Item("file_name").Value(file.FileName)
                                .DoIf(file.Executable, [&] (auto attributes) {
                                    attributes.Item("executable").Value(true);
                                })
                            .EndAttributes()
                            .Value(file.CypressPath);
                    })
                    .Item("environment").BeginMap()
                        .Item("YT_FLOW_MODE").Value(task.FlowMode)
                        .DoFor(task.Environment, [&] (auto fluent, const auto& pair) {
                            fluent.Item(pair.first).Value(pair.second);
                        })
                    .EndMap()
                    .DoIf(!task.Layers.empty(), [&] (auto fluent) {
                        fluent.Item("layer_paths").DoListFor(task.Layers, [&] (auto fluent, const NYPath::TYPath& layer) {
                            fluent.Item().Value(layer);
                        });
                    })
                    .DoIf(task.SystemLayerPath.has_value(), [&] (auto fluent) {
                        fluent.Item("system_layer_path").Value(*task.SystemLayerPath);
                    })
                    .DoIf(task.NetworkProject.has_value(), [&] (auto fluent) {
                        fluent.Item("network_project").Value(*task.NetworkProject);
                    })
                .EndMap();
            })
        .EndMap();
    // clang-format on

    return node->AsMap();
}

void InjectSecureVaultFromEnv(const IMapNodePtr& spec)
{
    std::vector<std::string> secretEnv;
    if (auto field = spec->FindChild("secret_env")) {
        secretEnv = ConvertTo<std::vector<std::string>>(field);
        spec->RemoveChild("secret_env");
    }

    auto secureVault = GetEphemeralNodeFactory()->CreateMap();
    secureVault->AddChild("YT_TOKEN", ConvertToNode(NAuth::LoadToken().value()));
    for (const auto& name : secretEnv) {
        auto value = GetEnv(TString(name));
        THROW_ERROR_EXCEPTION_IF(value.empty(),
            "Secret environment variable %Qv (declared in \"secret_env\") is not set",
            name);
        secureVault->AddChild(TString(name), ConvertToNode(value));
    }
    spec->AddChild("secure_vault", secureVault);
}

void StripSecureVault(const IMapNodePtr& spec)
{
    spec->RemoveChild("secure_vault");
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
