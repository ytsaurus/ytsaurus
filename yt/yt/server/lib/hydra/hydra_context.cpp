#include "hydra_context.h"

#include <yt/yt/core/actions/bind.h>

#include <yt/yt/core/concurrency/fls.h>

#include <yt/yt/core/misc/error.h>

#include <regex>

namespace NYT::NHydra {

////////////////////////////////////////////////////////////////////////////////

THydraContext::THydraContext(
    TVersion version,
    TInstant timestamp,
    ui64 randomSeed,
    TSharedRef localHostNameOverride)
    : Version_(version)
    , Timestamp_(timestamp)
    , RandomSeed_(randomSeed)
    , RandomGenerator_(New<TRandomGenerator>(randomSeed))
    , LocalHostName_(std::move(localHostNameOverride))
{ }

THydraContext::THydraContext(
    TVersion version,
    TInstant timestamp,
    ui64 randomSeed,
    TIntrusivePtr<TRandomGenerator> randomGenerator,
    TSharedRef localHostNameOverride)
    : Version_(version)
    , Timestamp_(timestamp)
    , RandomSeed_(randomSeed)
    , RandomGenerator_(std::move(randomGenerator))
    , LocalHostName_(std::move(localHostNameOverride))
{ }

TVersion THydraContext::GetVersion() const
{
    return Version_;
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
