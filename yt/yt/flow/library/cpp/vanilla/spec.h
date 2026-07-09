#pragma once

#include <yt/yt/client/ypath/public.h>

#include <yt/yt/core/ytree/public.h>

#include <util/generic/hash.h>

#include <optional>
#include <string>
#include <vector>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

//! A file to expose in a vanilla job: a Cypress path mounted under `FileName` in the job sandbox,
//! made executable for the binary.
struct TVanillaJobFile
{
    std::string FileName;
    NYPath::TYPath CypressPath;
    bool Executable = false;
};

//! Per-task description (controller or worker) of a flow vanilla operation.
struct TVanillaTaskSpec
{
    std::string Name;
    std::string FlowMode;
    int JobCount = 0;
    i64 MemoryLimit = 0;
    double CpuLimit = 0;
    //! When > 0, YT allocates that many ports (exposed as YT_PORT_<i>); otherwise the node uses the
    //! fixed ports from its config.
    int PortCount = 0;
    std::string Command;
    std::vector<TVanillaJobFile> Files;
    std::vector<NYPath::TYPath> Layers;
    std::optional<std::string> SystemLayerPath;
    std::optional<std::string> NetworkProject;
    //! Extra environment variables for the job, in addition to YT_FLOW_MODE (e.g.
    //! YT_PROXY_URL_ALIASING_CONFIG, or a YT_FLOW_CONFIG override for external launchers).
    THashMap<std::string, std::string> Environment;
};

//! Operation-level description of a flow vanilla operation.
struct TVanillaSpec
{
    std::optional<std::string> Pool;
    std::string Alias;
    std::optional<std::string> Title;
    int MaxFailedJobCount = 0;
    std::string SolomonResolverTag;
    int MonitoringPort = 0;
    //! Optional operation `description` annotation (shown in the YT UI).
    NYTree::IMapNodePtr Description;
    //! Requests the porto scheduling tag filter (needed when tasks use layers).
    bool UsePorto = false;
    //! Names of the secret environment variables; recorded in the spec so the secure vault can be
    //! rebuilt from the environment at operation start. Values never enter the spec.
    std::vector<std::string> SecretEnv;
    std::vector<TVanillaTaskSpec> Tasks;
};

//! Builds the YT vanilla operation spec node (the same shape `RunVanilla` materialized, assembled
//! directly so the launcher and reanimate submit it via `StartOperation`). The result carries only
//! the `secret_env` names; call InjectSecureVaultFromEnv right before starting to add the vault.
NYTree::IMapNodePtr BuildVanillaOperationSpec(const TVanillaSpec& spec);

//! Replaces the spec's `secret_env` field with a `secure_vault` rebuilt from the environment, so
//! secret values never have to be stored in Cypress. YT_TOKEN is always delivered; the rest are the
//! names the spec declared in `secret_env`. Used right before StartOperation by both launch and
//! reanimate.
void InjectSecureVaultFromEnv(const NYTree::IMapNodePtr& spec);

//! Removes the spec's `secure_vault` (the inverse of InjectSecureVaultFromEnv); applied before
//! persisting the spec so secret values never reach Cypress — only the `secret_env` names remain.
void StripSecureVault(const NYTree::IMapNodePtr& spec);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
