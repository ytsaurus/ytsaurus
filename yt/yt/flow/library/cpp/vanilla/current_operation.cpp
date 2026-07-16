#include "current_operation.h"

#include "files.h"
#include "spec.h"

#include <yt/yt/flow/lib/native_client/public.h>

#include <yt/yt/client/api/client.h>

#include <yt/yt/client/cache/rpc.h>

#include <yt/yt/client/object_client/public.h>

#include <yt/yt/client/scheduler/operation_id_or_alias.h>

#include <yt/yt/core/concurrency/scheduler_api.h>

#include <yt/yt/core/logging/log.h>

#include <yt/yt/core/ytree/convert.h>

namespace NYT::NFlow {

using namespace NConcurrency;
using namespace NYTree;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

static const NLogging::TLogger Logger("Reanimate");

////////////////////////////////////////////////////////////////////////////////

namespace {

//! Whether the operation behind the alias is present and in a non-terminal state. Only a definitive
//! "no such operation" answer from YT counts as not-alive; transient failures (YT unavailable,
//! network blips) propagate, so we never resubmit a second operation on a flaky lookup.
bool IsVanillaOperationAlive(
    const NApi::IClientPtr& operationClient,
    const std::string& alias)
{
    NScheduler::TOperationIdOrAlias operation{TString(alias)};
    NApi::TGetOperationOptions options;
    options.Attributes = THashSet<std::string>{"state"};
    // A running operation's alias only resolves through scheduler runtime state.
    options.IncludeRuntime = true;
    try {
        auto info = WaitFor(operationClient->GetOperation(operation, options)).ValueOrThrow();
        return info.State && !IsVanillaOperationStateTerminal(*info.State);
    } catch (const TErrorException& ex) {
        if (ex.Error().FindMatching(NApi::EErrorCode::NoSuchOperation) ||
            ex.Error().FindMatching(NScheduler::EErrorCode::NoSuchOperation))
        {
            return false;
        }
        throw;
    }
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

bool IsVanillaOperationStateTerminal(NScheduler::EOperationState state)
{
    return state == NScheduler::EOperationState::Completed ||
        state == NScheduler::EOperationState::Failed ||
        state == NScheduler::EOperationState::Aborted;
}

////////////////////////////////////////////////////////////////////////////////

void TVanillaOperationManifest::Register(TRegistrar registrar)
{
    registrar.Parameter("cluster", &TThis::Cluster);
    registrar.Parameter("alias", &TThis::Alias);
    registrar.Parameter("proxy_role", &TThis::ProxyRole)
        .Default();
}

////////////////////////////////////////////////////////////////////////////////

TVanillaOperationManifestPtr ReadVanillaOperationManifest(
    const NApi::IClientPtr& client,
    const NYPath::TYPath& pipelinePath)
{
    auto attrPath = Format("%v/@%v", pipelinePath, CurrentVanillaOperationAttribute);
    if (!WaitFor(client->NodeExists(attrPath)).ValueOrThrow()) {
        return nullptr;
    }
    return ConvertTo<TVanillaOperationManifestPtr>(WaitFor(client->GetNode(attrPath)).ValueOrThrow());
}

void WriteVanillaOperationManifest(
    const NApi::IClientPtr& client,
    const NYPath::TYPath& pipelinePath,
    const TVanillaOperationManifestPtr& manifest)
{
    auto attrPath = Format("%v/@%v", pipelinePath, CurrentVanillaOperationAttribute);
    WaitFor(client->SetNode(attrPath, ConvertToYsonString(manifest))).ThrowOnError();
}

void WriteVanillaOperationSpec(
    const NApi::IClientPtr& client,
    const NYPath::TYPath& pipelinePath,
    const NYTree::IMapNodePtr& spec)
{
    // Secret values must never reach Cypress; the spec keeps only the secret_env names.
    StripSecureVault(spec);

    auto dirPath = Format("%v/vanilla", pipelinePath);
    NApi::TCreateNodeOptions dirOptions;
    dirOptions.Recursive = true;
    dirOptions.IgnoreExisting = true;
    WaitFor(client->CreateNode(dirPath, NObjectClient::EObjectType::MapNode, dirOptions)).ThrowOnError();

    auto specPath = Format("%v/current_spec", dirPath);
    NApi::TCreateNodeOptions specOptions;
    specOptions.IgnoreExisting = true;
    WaitFor(client->CreateNode(specPath, NObjectClient::EObjectType::Document, specOptions)).ThrowOnError();
    WaitFor(client->SetNode(specPath, ConvertToYsonString(spec))).ThrowOnError();
}

NYTree::IMapNodePtr ReadVanillaOperationSpec(
    const NApi::IClientPtr& client,
    const NYPath::TYPath& pipelinePath)
{
    auto specPath = Format("%v/vanilla/current_spec", pipelinePath);
    return ConvertToNode(WaitFor(client->GetNode(specPath)).ValueOrThrow())->AsMap();
}

std::string ReanimateVanillaOperation(
    const NApi::IClientPtr& pipelineClient,
    const std::string& pipelineCluster,
    const NYPath::TYPath& pipelinePath,
    const std::optional<std::string>& runtimeClusterOverride,
    const std::optional<std::string>& runtimeProxyRoleOverride)
{
    auto manifest = ReadVanillaOperationManifest(pipelineClient, pipelinePath);
    THROW_ERROR_EXCEPTION_UNLESS(manifest,
        "No vanilla operation manifest at %Qv; nothing to reanimate",
        pipelinePath);

    // Resubmit on the override cluster (with the override role) if given, otherwise where the
    // operation last ran.
    auto target = runtimeClusterOverride.value_or(manifest->Cluster);
    auto role = runtimeProxyRoleOverride ? runtimeProxyRoleOverride : manifest->ProxyRole;
    bool crossCluster = target != pipelineCluster;
    auto operationClient = crossCluster
        ? NClient::NCache::CreateClient(target, role)
        : pipelineClient;

    THROW_ERROR_EXCEPTION_IF(
        IsVanillaOperationAlive(operationClient, manifest->Alias),
        "Vanilla operation %Qv is still alive; refusing to reanimate",
        manifest->Alias);

    // Record the manifest before the start: the alias is known up front, and a write after the
    // start could fail, leaving a running operation the manifest does not point at.
    manifest->Cluster = target;
    manifest->ProxyRole = role;
    WriteVanillaOperationManifest(pipelineClient, pipelinePath, manifest);

    auto spec = ReadVanillaOperationSpec(pipelineClient, pipelinePath);
    if (crossCluster) {
        // The durable files live in the pipeline folder on the pipeline cluster; the runtime cluster
        // cannot reference them, so re-stage them through its file cache and point the spec there.
        RebaseVanillaSpecFiles(spec, pipelineClient, operationClient, TString(VanillaFileCachePath));
    }

    InjectSecureVaultFromEnv(spec);

    auto operationId = WaitFor(operationClient->StartOperation(
        NScheduler::EOperationType::Vanilla,
        ConvertToYsonString(spec)))
        .ValueOrThrow();
    auto operationIdString = Format("%v", operationId);

    YT_TLOG_INFO("Reanimated pipeline")
        .With("Path", pipelinePath)
        .With("Cluster", target)
        .With("OperationId", operationIdString);

    return operationIdString;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
