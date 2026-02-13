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
        TLogicalVersion logicalVersion,
        TPhysicalVersion physicalVersion,
        TPhysicalVersion compatOnlyPhysicalVersion,
        TInstant timestamp,
        ui64 randomSeed,
        TSharedRef localHostNameOverride);

    THydraContext(
        TLogicalVersion logicalVersion,
        TPhysicalVersion physicalVersion,
        TPhysicalVersion compatOnlyPhysicalVersion,
        TInstant timestamp,
        ui64 randomSeed,
        TIntrusivePtr<TRandomGenerator> randomGenerator,
        TSharedRef localHostNameOverride);

    TLogicalVersion GetVersion() const;
    TPhysicalVersion GetPhysicalVersion() const;

    TInstant GetTimestamp() const;

    ui64 GetRandomSeed() const;
    const TIntrusivePtr<TRandomGenerator>& RandomGenerator();

    const TSharedRef& GetLocalHostName() const;

protected:
    explicit THydraContext(THydraContext* parent, TLogicalVersion childVersion);

private:
    const TLogicalVersion Version_;
    // COMPAT(h0pless): HydraLogicalRecordId.
    const TPhysicalVersion PhysicalVersion_;

    const TInstant Timestamp_;

    const ui64 RandomSeed_;
    const TIntrusivePtr<TRandomGenerator> RandomGenerator_;

    const TSharedRef LocalHostName_;

    static TLogicalVersion MaybeRotateVersion(
        TLogicalVersion logicalVersion,
        TPhysicalVersion physicalVersion);
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
