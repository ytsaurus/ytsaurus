#pragma once

#include "public.h"

#include <yt/yt/core/actions/future.h>

#include <library/cpp/yt/memory/ref.h>

namespace NYT::NNbd {

////////////////////////////////////////////////////////////////////////////////

struct TReadOptions
{
    //! Request id issued by linux kernel (NBD module).
    ui64 Cookie = 0;
};

////////////////////////////////////////////////////////////////////////////////

struct TReadResponse
{
    TSharedRef Data;
    //! It is a polite request to stop using device. The device will be of no use some time soon.
    bool ShouldStopUsingDevice = false;
};

////////////////////////////////////////////////////////////////////////////////

struct TWriteOptions
{
    //! The 'FUA (Force Unit Access) flag'.
    bool Flush = false;
    //! Request id issued by linux kernel (NBD module).
    ui64 Cookie = 0;
};

////////////////////////////////////////////////////////////////////////////////

struct TWriteResponse
{
    //! It is a polite request to stop using device. The device will be of no use some time soon.
    bool ShouldStopUsingDevice = false;
};

////////////////////////////////////////////////////////////////////////////////

//! Represents a block device that can be exposed via the NBD protocol.
struct IBlockDevice
    : public virtual TRefCounted
{
    virtual i64 GetTotalSize() const = 0;
    virtual bool IsReadOnly() const = 0;
    virtual TString DebugString() const = 0;
    virtual TString GetProfileSensorTag() const = 0;

    virtual TFuture<void> Initialize() = 0;
    virtual TFuture<void> Finalize() = 0;

    virtual TFuture<TReadResponse> Read(
        i64 offset,
        i64 length,
        const TReadOptions& options = {}) = 0;

    virtual TFuture<TWriteResponse> Write(
        i64 offset,
        const TSharedRef& data,
        const TWriteOptions& options = {}) = 0;

    virtual TFuture<void> Flush() = 0;

    //! Get the latest error set for device.
    virtual const TError& GetError() const = 0;
    //! Set an error (error.IsOK() == false) for device.
    virtual void SetError(TError error) = 0;
    //! Subscribe #id (e.g. job id) for device errors.
    virtual bool SubscribeForErrors(TGuid id, const TCallback<void()>& callback) = 0;
    //! Unsubscribe #id (e.g. job id) from device errors.
    virtual bool UnsubscribeFromErrors(TGuid id) = 0;
};

DEFINE_REFCOUNTED_TYPE(IBlockDevice)

////////////////////////////////////////////////////////////////////////////////

class TBaseBlockDevice
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
