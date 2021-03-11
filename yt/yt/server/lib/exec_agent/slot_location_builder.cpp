#include "slot_location_builder.h"

#include <yt/server/lib/exec_agent/public.h>

#include <yt/core/misc/fs.h>
#include <yt/core/misc/proc.h>

#include <util/system/fs.h>

namespace NYT::NExecAgent {

////////////////////////////////////////////////////////////////////////////////

TSlotConfig::TSlotConfig()
{
    RegisterParameter("index", Index)
        .Default();
    RegisterParameter("uid", Uid)
        .Default();
}

////////////////////////////////////////////////////////////////////////////////

TSlotLocationBuilderConfig::TSlotLocationBuilderConfig()
{
    RegisterParameter("location_path", LocationPath)
        .Default();
    RegisterParameter("node_uid", NodeUid)
        .Default();
    RegisterParameter("slot_configs", SlotConfigs)
        .Default();
}

////////////////////////////////////////////////////////////////////////////////

void TSlotLocationBuilderTool::operator()(const TSlotLocationBuilderConfigPtr& arg) const
{
    bool needRoot = false;
    for (const auto& slotConfig : arg->SlotConfigs) {
        if (slotConfig->Uid) {
            needRoot = true;
        }
    }

    if (needRoot) {
        TrySetUid(0);
    }

    auto getSlotPath = [&] (int slotIndex) {
        return NFS::CombinePaths(arg->LocationPath, Format("%v", slotIndex));
    };

    auto getSandboxPath = [&] (int slotIndex, ESandboxKind sandboxKind) {
        auto slotPath = getSlotPath(slotIndex);

        const auto& sandboxName = SandboxDirectoryNames[sandboxKind];
        YT_VERIFY(sandboxName);
        return NFS::CombinePaths(slotPath, sandboxName);
    };

    for (const auto& slotConfig : arg->SlotConfigs) {
        int slotIndex = slotConfig->Index;
        auto uid = slotConfig->Uid;

        // Create slot directory if it does not exist.
        NFS::MakeDirRecursive(getSlotPath(slotIndex), 0755);
        ChownChmodDirectory(getSlotPath(slotIndex), arg->NodeUid, 0755);

        for (auto sandboxKind : TEnumTraits<ESandboxKind>::GetDomainValues()) {
            auto sandboxPath = getSandboxPath(slotIndex, sandboxKind);
            if (NFS::Exists(sandboxPath)) {
                NFS::RemoveRecursive(sandboxPath);
            }
            NFs::MakeDirectory(sandboxPath, NFs::EFilePermission::FP_SECRET_FILE);
        }

        // Since we make slot user to be owner, but job proxy creates some files during job shell
        // initialization we leave write access for everybody. Presumably this will not ruin job isolation.
        ChownChmodDirectory(getSandboxPath(slotIndex, ESandboxKind::Home), uid, 0777);

        // Tmp is accessible for everyone.
        ChownChmodDirectory(getSandboxPath(slotIndex, ESandboxKind::Tmp), uid, 0777);

        // CUDA library should have an access to cores directory to write GPU core dump into it.
        ChownChmodDirectory(getSandboxPath(slotIndex, ESandboxKind::Cores), uid, 0777);

        // Pipes are accessible for everyone.
        ChownChmodDirectory(getSandboxPath(slotIndex, ESandboxKind::Pipes), uid, 0777);

        // Node should have access to user sandbox during job preparation.
        ChownChmodDirectory(getSandboxPath(slotIndex, ESandboxKind::User), arg->NodeUid, 0755);

        // Process executor should have access to write logs before process start.
        ChownChmodDirectory(getSandboxPath(slotIndex, ESandboxKind::Logs), uid, 0755);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NExecAgent
