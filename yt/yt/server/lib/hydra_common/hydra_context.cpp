#include "hydra_context.h"

#include <yt/yt/core/concurrency/fls.h>

#include <yt/yt/core/misc/error.h>

namespace NYT::NHydra {

////////////////////////////////////////////////////////////////////////////////

THydraContext::THydraContext(
    TVersion version,
    TInstant timestamp,
    ui64 randomSeed,
    bool isMutationLoggingEnabled)
    : Version_(version)
    , Timestamp_(timestamp)
    , RandomSeed_(randomSeed)
    , RandomGenerator_(New<TRandomGenerator>(randomSeed))
    , IsMutationLoggingEnabled_(isMutationLoggingEnabled)
    , ErrorSanitizerGuard_(/*datetimeOverride*/ timestamp)
{ }

THydraContext::THydraContext(
    TVersion version,
    TInstant timestamp,
    ui64 randomSeed,
    bool isMutationLoggingEnabled,
    TIntrusivePtr<TRandomGenerator> randomGenerator)
    : Version_(version)
    , Timestamp_(timestamp)
    , RandomSeed_(randomSeed)
    , RandomGenerator_(std::move(randomGenerator))
    , IsMutationLoggingEnabled_(isMutationLoggingEnabled)
    , ErrorSanitizerGuard_(/*datetimeOverride*/ timestamp)
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

bool THydraContext::IsMutationLoggingEnabled() const
{
    return IsMutationLoggingEnabled_;
}

////////////////////////////////////////////////////////////////////////////////

THydraContextGuard::THydraContextGuard(THydraContext* context)
    : Context_(context)
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

bool IsMutationLoggingEnabled()
{
    if (auto* hydraContext = TryGetCurrentHydraContext()) {
        return hydraContext->IsMutationLoggingEnabled();
    } else {
        // This branch is used by code that is executed both in
        // transient and persistent contexts.
        return true;
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHydra
