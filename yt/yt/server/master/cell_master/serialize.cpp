#include "serialize.h"

#include "private.h"

#include <yt/yt/server/master/object_server/object_manager.h>

#include <yt/yt/server/master/security_server/security_manager.h>

#include <util/generic/cast.h>

namespace NYT::NCellMaster {

using namespace NHydra;
using namespace NObjectServer;
using namespace NSecurityServer;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = CellMasterLogger;

////////////////////////////////////////////////////////////////////////////////

TReign GetCurrentReign()
{
    return ToUnderlying(TEnumTraits<EMasterReign>::GetMaxValue());
}

bool ValidateSnapshotReign(TReign reign)
{
    for (auto value : TEnumTraits<EMasterReign>::GetDomainValues()) {
        if (ToUnderlying(value) == reign) {
            return true;
        }
    }
    return false;
}

EFinalRecoveryAction GetActionToRecoverFromReign(TReign reign)
{
    // In Master we do it the hard way.
    YT_LOG_FATAL_UNLESS(reign == GetCurrentReign(),
        "Attempted to recover master from invalid reign "
        "(RecoverReign: %v, CurrentReign: %v)",
        reign,
        GetCurrentReign());

    return EFinalRecoveryAction::None;
}

////////////////////////////////////////////////////////////////////////////////

TSaveContext::TSaveContext(
    ICheckpointableOutputStream* output,
    NLogging::TLogger logger,
    NConcurrency::IThreadPoolPtr backgroundThreadPool)
    : NHydra::TSaveContext(
        output,
        std::move(logger),
        GetCurrentReign(),
        std::move(backgroundThreadPool))
{ }

TSaveContext::TSaveContext(
    IZeroCopyOutput* output,
    const TSaveContext* parentContext)
    : NHydra::TSaveContext(output, parentContext)
    , ParentContext_(parentContext)
{ }

TEntitySerializationKey TSaveContext::RegisterInternedYsonString(NYson::TYsonString str)
{
    if (ParentContext_) {
        return GetOrCrash(ParentContext_->InternedYsonStrings_, str);
    }

    TYsonStringMap::insert_ctx context;
    if (auto it = InternedYsonStrings_.find(str, context)) {
        return it->second;
    }

    auto key = std::ssize(InternedYsonStrings_);
    InternedYsonStrings_.emplace_direct(context, str, key);
    return InlineKey;
}

EMasterReign TSaveContext::GetVersion()
{
    return static_cast<EMasterReign>(NHydra::TSaveContext::GetVersion());
}

////////////////////////////////////////////////////////////////////////////////

TLoadContext::TLoadContext(
    TBootstrap* bootstrap,
    ICheckpointableInputStream* input)
    : NHydra::TLoadContext(input)
    , Bootstrap_(bootstrap)
{ }

TObject* TLoadContext::GetWeakGhostObject(TObjectId id) const
{
    const auto& objectManager = Bootstrap_->GetObjectManager();
    return objectManager->GetWeakGhostObject(id);
}

template <>
const TSecurityTagsRegistryPtr& TLoadContext::GetInternRegistry() const
{
    const auto& securityManager = Bootstrap_->GetSecurityManager();
    return securityManager->GetSecurityTagsRegistry();
}

TEntitySerializationKey TLoadContext::RegisterInternedYsonString(NYson::TYsonString str)
{
    auto key = static_cast<int>(InternedYsonStrings_.size());
    InternedYsonStrings_.push_back(std::move(str));
    return TEntitySerializationKey(key);
}

NYson::TYsonString TLoadContext::GetInternedYsonString(TEntitySerializationKey key)
{
    YT_ASSERT(key.Index >= 0);
    YT_ASSERT(key.Index < static_cast<int>(InternedYsonStrings_.size()));
    return InternedYsonStrings_[key.Index];
}

EMasterReign TLoadContext::GetVersion()
{
    return static_cast<EMasterReign>(NHydra::TLoadContext::GetVersion());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellMaster
