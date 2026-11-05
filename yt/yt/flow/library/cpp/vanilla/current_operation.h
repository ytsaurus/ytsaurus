#pragma once

#include <yt/yt/client/api/public.h>

#include <yt/yt/client/scheduler/public.h>

#include <yt/yt/client/ypath/public.h>

#include <yt/yt/core/ytree/public.h>

#include <yt/yt/core/ytree/yson_struct.h>

#include <optional>
#include <string>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(TVanillaOperationManifest)

//! The `@current_vanilla_operation` attribute: a pointer to the pipeline's current vanilla operation.
//! The operation is identified by its alias (an operation id cannot be recorded reliably — the
//! attribute write may fail after the operation has already started). Everything needed to resubmit
//! the operation (files, layout, secret_env) lives in the `vanilla/current_spec` document, not here.
struct TVanillaOperationManifest
    : public NYTree::TYsonStruct
{
    //! Cluster the operation runs on (the pipeline's own cluster unless it was launched cross-cluster).
    std::string Cluster;
    std::string Alias;
    //! RPC proxy role to reach the operation's cluster with (the pipeline cluster's role may not
    //! exist there).
    std::optional<std::string> ProxyRole;

    REGISTER_YSON_STRUCT(TVanillaOperationManifest);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TVanillaOperationManifest)

////////////////////////////////////////////////////////////////////////////////

//! Reads the `@current_vanilla_operation` attribute. Returns nullptr if the pipeline has none.
TVanillaOperationManifestPtr ReadVanillaOperationManifest(
    const NApi::IClientPtr& client,
    const NYPath::TYPath& pipelinePath);

//! Writes the `@current_vanilla_operation` attribute, pointing it at the manifest's operation.
void WriteVanillaOperationManifest(
    const NApi::IClientPtr& client,
    const NYPath::TYPath& pipelinePath,
    const TVanillaOperationManifestPtr& manifest);

//! Persists the operation spec as the canonical reanimate source in `vanilla/current_spec`. The spec
//! is expected to carry only the `secret_env` names; any `secure_vault` is stripped defensively so
//! secret values never reach Cypress.
void WriteVanillaOperationSpec(
    const NApi::IClientPtr& client,
    const NYPath::TYPath& pipelinePath,
    const NYTree::IMapNodePtr& spec);

//! Reads the spec persisted by WriteVanillaOperationSpec.
NYTree::IMapNodePtr ReadVanillaOperationSpec(
    const NApi::IClientPtr& client,
    const NYPath::TYPath& pipelinePath);

//! Whether the operation state is terminal (completed, failed or aborted).
bool IsVanillaOperationStateTerminal(NScheduler::EOperationState state);

//! Reanimates the pipeline's vanilla operation from the persisted spec. Reads the manifest and spec
//! via `pipelineClient`, (re)starts the operation on the runtime cluster — the manifest's cluster
//! and proxy role, or the overrides when given — refuses if the operation behind the manifest's
//! alias is still alive, rebuilds the secure vault from the environment, and updates the manifest
//! before the start. For a remote runtime cluster the durable files are re-staged through that
//! cluster's file cache. Returns the new operation id.
std::string ReanimateVanillaOperation(
    const NApi::IClientPtr& pipelineClient,
    const std::string& pipelineCluster,
    const NYPath::TYPath& pipelinePath,
    const std::optional<std::string>& runtimeClusterOverride,
    const std::optional<std::string>& runtimeProxyRoleOverride);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
