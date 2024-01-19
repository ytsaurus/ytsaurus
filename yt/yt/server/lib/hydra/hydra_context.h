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
        TSharedRef localHostNameOverride);

    THydraContext(
        TVersion version,
        TInstant timestamp,
        ui64 randomSeed,
        TIntrusivePtr<TRandomGenerator> randomGenerator,
        TSharedRef localHostNameOverride);

    TVersion GetVersion() const;

    TInstant GetTimestamp() const;

    ui64 GetRandomSeed() const;
    const TIntrusivePtr<TRandomGenerator>& RandomGenerator();

    const TSharedRef& GetLocalHostName() const;

private:
    const TVersion Version_;

    const TInstant Timestamp_;

    const ui64 RandomSeed_;
    const TIntrusivePtr<TRandomGenerator> RandomGenerator_;

    const TSharedRef LocalHostName_;
};

////////////////////////////////////////////////////////////////////////////////

class THydraContextGuard
    //! Makes all errors inside mutation deterministic.
    : public TNonCopyable
{
public:
    explicit THydraContextGuard(THydraContext* context);
    ~THydraContextGuard();

private:
    TErrorSanitizerGuard ErrorSanitizerGuard_;
    THydraContext* Context_;
    THydraContext* SavedContext_;
};

////////////////////////////////////////////////////////////////////////////////

THydraContext* TryGetCurrentHydraContext();
THydraContext* GetCurrentHydraContext();
void SetCurrentHydraContext(THydraContext* context);
bool HasHydraContext();

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHydra
