#pragma once

#include "block_device.h"

#include <library/cpp/yt/threading/rw_spin_lock.h>

namespace NYT::NNbd {

////////////////////////////////////////////////////////////////////////////////

//! A convenience base for block devices providing the error subscription machinery.
class TBlockDeviceBase
    : public IBlockDevice
{
public:
    const TError& GetError() const final;
    void SetError(TError error) final;

    bool SubscribeForErrors(TGuid id, const TCallback<void()>& callback) final;
    bool UnsubscribeFromErrors(TGuid id) final;

private:
    YT_DECLARE_SPIN_LOCK(NThreading::TReaderWriterSpinLock, Lock_);
    THashMap<TGuid, TCallback<void()>> SubscriberCallbacks_;
    TError Error_;

    void CallSubscribers() const;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NNbd
