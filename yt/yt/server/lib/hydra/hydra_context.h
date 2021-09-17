#pragma once

#include "public.h"

#include <yt/yt/client/hydra/version.h>

#include <yt/yt/core/misc/random.h>

namespace NYT::NHydra {

////////////////////////////////////////////////////////////////////////////////

class THydraContext
{
public:
    THydraContext(
        TVersion version,
        TInstant timestamp,
        ui64 randomSeed);

    TVersion GetVersion() const;

    TInstant GetTimestamp() const;

    ui64 GetRandomSeed() const;
    TRandomGenerator& RandomGenerator();

private:
    const TVersion Version_;

    const TInstant Timestamp_;

    const ui64 RandomSeed_;
    const TIntrusivePtr<TRandomGenerator> RandomGenerator_;
};

////////////////////////////////////////////////////////////////////////////////

class THydraContextGuard
    : public TNonCopyable
{
public:
    explicit THydraContextGuard(THydraContext* context);
    ~THydraContextGuard();

private:
    THydraContext* Context_;
    THydraContext* SavedContext_;
};

////////////////////////////////////////////////////////////////////////////////

THydraContext* TryGetCurrentHydraContext();
THydraContext* GetCurrentHydraContext();
void SetCurrentHydraContext(THydraContext* context);
bool HasHydraContext();

TError SanitizeWithCurrentHydraContext(const TError& error);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHydra
