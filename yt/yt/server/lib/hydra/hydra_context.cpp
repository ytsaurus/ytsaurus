#include "hydra_context.h"

#include <yt/yt/core/concurrency/fls.h>

#include <yt/yt/core/misc/error.h>

namespace NYT::NHydra {

////////////////////////////////////////////////////////////////////////////////

THydraContext::THydraContext(
    TVersion version,
    TInstant timestamp,
    ui64 randomSeed)
    : Version_(version)
    , Timestamp_(timestamp)
    , RandomSeed_(randomSeed)
    , RandomGenerator_(New<TRandomGenerator>(randomSeed))
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

TRandomGenerator& THydraContext::RandomGenerator()
{
    return *RandomGenerator_;
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

static NConcurrency::TFls<THydraContext*> CurrentHydraContext;

THydraContext* TryGetCurrentHydraContext()
{
    return *CurrentHydraContext;
}

THydraContext* GetCurrentHydraContext()
{
    auto* hydraContext = TryGetCurrentHydraContext();
    YT_ASSERT(hydraContext);
    return hydraContext;
}

void SetCurrentHydraContext(THydraContext* context)
{
    *CurrentHydraContext = context;
}

bool HasHydraContext()
{
    return TryGetCurrentHydraContext() != nullptr;
}

TError SanitizeWithCurrentHydraContext(const TError& error)
{
    return error.Sanitize(GetCurrentHydraContext()->GetTimestamp());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHydra
