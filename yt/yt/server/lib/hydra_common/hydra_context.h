#pragma once

#include "public.h"

#include <yt/yt/client/hydra/version.h>

#include <yt/yt/core/misc/error.h>
#include <yt/yt/core/misc/random.h>

namespace NYT::NHydra {

////////////////////////////////////////////////////////////////////////////////

class THydraContext
{
public:
    THydraContext(
        TVersion version,
        TInstant timestamp,
        ui64 randomSeed,
        bool isMutationLoggingEnabled);

    THydraContext(
        TVersion version,
        TInstant timestamp,
        ui64 randomSeed,
        bool isMutationLoggingEnabled,
        TIntrusivePtr<TRandomGenerator> randomGenerator);

    TVersion GetVersion() const;

    TInstant GetTimestamp() const;

    ui64 GetRandomSeed() const;
    const TIntrusivePtr<TRandomGenerator>& RandomGenerator();

    bool IsMutationLoggingEnabled() const;

private:
    const TVersion Version_;

    const TInstant Timestamp_;

    const ui64 RandomSeed_;
    const TIntrusivePtr<TRandomGenerator> RandomGenerator_;

    const bool IsMutationLoggingEnabled_;

    //! Makes all errors inside mutation deterministic.
    const TErrorSanitizerGuard ErrorSanitizerGuard_;
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

////////////////////////////////////////////////////////////////////////////////

bool IsMutationLoggingEnabled();

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHydra
