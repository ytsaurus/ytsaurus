#pragma once

#include "config.h"

#include <yt/yt/core/logging/log.h>

#include <yt/yt/core/misc/atomic_ptr.h>

namespace NYT::NDataNode {

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(ELocationState,
    (Enabled)
    (Resurrecting)
    (Disabling)
    (Disabled)
    (Destroying)
    (Destroyed)
);

////////////////////////////////////////////////////////////////////////////////

class TDiskLocation
    : public TRefCounted
{
public:
    TDiskLocation(
        TDiskLocationConfigPtr config,
        TString id,
        const NLogging::TLogger& logger);

    //! Returns the string id.
    const TString& GetId() const;

    //! Returns the runtime configuration.
    TDiskLocationConfigPtr GetRuntimeConfig() const;

    //! Updates the runtime configuration.
    void Reconfigure(TDiskLocationConfigPtr config);

    //! Returns |true| iff the location is enabled.
    bool IsEnabled() const;

    // Returns current location state.
    ELocationState GetState() const;

protected:
    const TString Id_;
    const NLogging::TLogger Logger;

    std::atomic<ELocationState> State_ = ELocationState::Disabled;

    void ValidateMinimumSpace() const;
    void ValidateLockFile() const;

    i64 GetTotalSpace() const;

private:
    const TDiskLocationConfigPtr StaticConfig_;

    TAtomicPtr<TDiskLocationConfig> RuntimeConfig_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDataNode

