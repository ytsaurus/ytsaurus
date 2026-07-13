#pragma once

#include "block_device.h"

#include <yt/yt/core/actions/callback_list.h>

#include <yt/yt/core/ytree/fluent.h>

#include <library/cpp/yt/threading/atomic_object.h>

namespace NYT::NNbd {

////////////////////////////////////////////////////////////////////////////////

//! A convenience base for block devices providing the error-signal machinery.
class TBlockDeviceBase
    : public IBlockDevice
{
public:
    TError GetError() const final;
    void SetError(TError error) final;

    //! The device error is a one-shot terminal event, so a subscriber added after
    //! the error was set is invoked in situ (see TSingleShotCallbackList).
    void SubscribeError(const TCallback<void(const TError&)>& callback) override;
    void UnsubscribeError(const TCallback<void(const TError&)>& callback) override;

    //! Exposes the common device status (size, block size, read-only, description, error).
    //! Subclasses add device-specific fields by overriding #DoBuildOrchid.
    NYTree::IYPathServicePtr GetOrchidService() override;

protected:
    //! Called with the status map open; writes additional map items to #consumer. The default adds
    //! nothing. Must be thread-safe.
    virtual void DoBuildOrchid(NYson::IYsonConsumer* consumer) const;

private:
    NThreading::TAtomicObject<TError> Error_;
    TSingleShotCallbackList<void(const TError&)> ErrorList_;

    void BuildOrchid(NYson::IYsonConsumer* consumer) const;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NNbd
