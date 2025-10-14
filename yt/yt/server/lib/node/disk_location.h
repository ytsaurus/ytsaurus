#pragma once

#include <yt/yt/server/lib/misc/public.h>

#include <yt/yt/core/concurrency/public.h>

#include <yt/yt/core/logging/log.h>

#include <library/cpp/yt/memory/atomic_intrusive_ptr.h>

#include <library/cpp/yt/threading/rw_spin_lock.h>
#include <library/cpp/yt/threading/spin_lock.h>

namespace NYT::NNode {

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(ELocationState,
    (Enabled)

    (Enabling)
    (Disabling)
    (Destroying)

    (Disabled)
    (Destroyed)
    (Crashed)
);

////////////////////////////////////////////////////////////////////////////////

class TDiskLocation
    : public TRefCounted
{
public:
    TDiskLocation(
        NServer::TDiskLocationConfigPtr config,
        TString id,
        const NLogging::TLogger& logger);

    //! Returns the string id.
    const TString& GetId() const;

    //! Returns the runtime configuration.
    NServer::TDiskLocationConfigPtr GetRuntimeConfig() const;

    //! Updates the runtime configuration.
    void Reconfigure(NServer::TDiskLocationConfigPtr config);

    //! Returns |true| iff the location is enabled.
    bool IsEnabled() const;

    //! Returns |true| if the location can handle incoming actions.
    bool CanHandleIncomingActions() const;

    // Returns current location state.
    ELocationState GetState() const;

    // Before changing the location state, it is necessary to synchronize the work of some
    // actions (chunk deletion or initialization) on the location. It is necessary to wait
    // for the completion of the actions in order to avoid a race.
    template <class T>
    TFuture<T> RegisterAction(TCallback<TFuture<T>()> action);

protected:
    YT_DECLARE_SPIN_LOCK(NThreading::TReaderWriterSpinLock, StateChangingLock_);
    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, ActionsContainerLock_);

    const TString Id_;
    const NLogging::TLogger Logger;

    THashSet<TFuture<void>> Actions_;
    std::atomic<ELocationState> State_ = ELocationState::Enabling;

    void ValidateMinimumSpace() const;
    void ValidateLockFile() const;

    i64 GetTotalSpace() const;

    bool ChangeState(
        ELocationState newState,
        std::optional<ELocationState> expectedState = std::nullopt);

private:
    const NServer::TDiskLocationConfigPtr StaticConfig_;

    TAtomicIntrusivePtr<NServer::TDiskLocationConfig> RuntimeConfig_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NNode

#define DISK_LOCATION_INL_H_
#include "disk_location-inl.h"
#undef DISK_LOCATION_INL_H_
