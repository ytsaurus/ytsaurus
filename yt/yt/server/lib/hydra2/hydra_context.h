#pragma once

#include "public.h"

#include <yt/yt/client/hydra/version.h>

#include <yt/yt/core/misc/random.h>

namespace NYT::NHydra2 {

////////////////////////////////////////////////////////////////////////////////

class THydraContext
{
public:
    THydraContext(
        TVersion version,
        TInstant timestamp,
        ui64 randomSeed,
        TReign automatonReign);

    TVersion GetVersion() const;

    TInstant GetTimestamp() const;

    ui64 GetRandomSeed() const;
    TRandomGenerator& RandomGenerator();

    //! Returns the reign of current automaton state.
    //! For regular mutation equals to reign of mutation itself.
    //! For virtual mutations equals to reign of snapshot being loaded.
    template <class EReign>
    EReign GetAutomatonReign() const;

private:
    const TVersion Version_;

    const TInstant Timestamp_;

    const ui64 RandomSeed_;
    const TIntrusivePtr<TRandomGenerator> RandomGenerator_;

    const TReign AutomatonReign_;
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

} // namespace NYT::NHydra2

#define HYDRA_CONTEXT_INL_H_
#include "hydra_context-inl.h"
#undef HYDRA_CONTEXT_INL_H_
