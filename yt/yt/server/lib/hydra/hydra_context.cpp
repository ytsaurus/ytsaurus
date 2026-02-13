#include "hydra_context.h"

#include <yt/yt/core/actions/bind.h>

#include <yt/yt/core/concurrency/fls.h>

#include <yt/yt/core/misc/error.h>

namespace NYT::NHydra {

////////////////////////////////////////////////////////////////////////////////

THydraContext::THydraContext(
    TLogicalVersion logicalVersion,
    TPhysicalVersion physicalVersion,
    TPhysicalVersion compatOnlyPhysicalVersion,
    TInstant timestamp,
    ui64 randomSeed,
    TSharedRef localHostNameOverride)
    : Version_(MaybeRotateVersion(logicalVersion, physicalVersion))
    , PhysicalVersion_(compatOnlyPhysicalVersion)
    , Timestamp_(timestamp)
    , RandomSeed_(randomSeed)
    , RandomGenerator_(New<TRandomGenerator>(randomSeed))
    , LocalHostName_(std::move(localHostNameOverride))
{ }

THydraContext::THydraContext(
    TLogicalVersion logicalVersion,
    TPhysicalVersion physicalVersion,
    TPhysicalVersion compatOnlyPhysicalVersion,
    TInstant timestamp,
    ui64 randomSeed,
    TIntrusivePtr<TRandomGenerator> randomGenerator,
    TSharedRef localHostNameOverride)
    : Version_(MaybeRotateVersion(logicalVersion, physicalVersion))
    , PhysicalVersion_(compatOnlyPhysicalVersion)
    , Timestamp_(timestamp)
    , RandomSeed_(randomSeed)
    , RandomGenerator_(std::move(randomGenerator))
    , LocalHostName_(std::move(localHostNameOverride))
{ }

THydraContext::THydraContext(THydraContext* parent, TLogicalVersion childVersion)
    : Version_(childVersion)
    , PhysicalVersion_(parent->PhysicalVersion_)
    , Timestamp_(parent->Timestamp_)
    , RandomSeed_(parent->RandomSeed_)
    , RandomGenerator_(parent->RandomGenerator_)
    , LocalHostName_(parent->LocalHostName_)
{ }

TLogicalVersion THydraContext::GetVersion() const
{
    return Version_;
}

TPhysicalVersion THydraContext::GetPhysicalVersion() const
{
    return PhysicalVersion_;
}

TInstant THydraContext::GetTimestamp() const
{
    return Timestamp_;
}

ui64 THydraContext::GetRandomSeed() const
{
    return RandomSeed_;
}

const TIntrusivePtr<TRandomGenerator>& THydraContext::RandomGenerator()
{
    return RandomGenerator_;
}

const TSharedRef& THydraContext::GetLocalHostName() const
{
    return LocalHostName_;
}

TLogicalVersion THydraContext::MaybeRotateVersion(
    TLogicalVersion logicalVersion,
    TPhysicalVersion physicalVersion)
{
    if (physicalVersion.SegmentId == logicalVersion.SegmentId) {
        return logicalVersion;
    }

    // Version was rotated, possibly several times.
    YT_VERIFY(physicalVersion.SegmentId > logicalVersion.SegmentId);
    YT_VERIFY(physicalVersion.RecordId == 0);

    return TLogicalVersion(physicalVersion.SegmentId, 0);
}

////////////////////////////////////////////////////////////////////////////////

THydraContextGuard::THydraContextGuard(THydraContext* context)
    : ErrorSanitizerGuard_(context->GetTimestamp(), context->GetLocalHostName())
    , Context_(context)
    , SavedContext_(TryGetCurrentHydraContext())
{
    SetCurrentHydraContext(Context_);
}

THydraContextGuard::~THydraContextGuard()
{
    YT_ASSERT(TryGetCurrentHydraContext() == Context_);
    SetCurrentHydraContext(SavedContext_);
}

////////////////////////////////////////////////////////////////////////////////

static NConcurrency::TFlsSlot<THydraContext*> CurrentHydraContextSlot;

THydraContext* TryGetCurrentHydraContext()
{
    return *CurrentHydraContextSlot;
}

THydraContext* GetCurrentHydraContext()
{
    auto* hydraContext = TryGetCurrentHydraContext();
    YT_ASSERT(hydraContext);
    return hydraContext;
}

void SetCurrentHydraContext(THydraContext* context)
{
    *CurrentHydraContextSlot = context;
}

bool HasHydraContext()
{
    return TryGetCurrentHydraContext() != nullptr;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHydra
