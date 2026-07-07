#pragma once

#include "public.h"

#include <yt/yt/core/actions/future.h>
#include <yt/yt/core/actions/signal.h>

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
    virtual std::string DebugString() const = 0;
    virtual std::string GetProfileSensorTag() const = 0;

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
    virtual TError GetError() const = 0;
    //! Set an error for device.
    virtual void SetError(TError error) = 0;
    //! Fired with the error once one is set on the device (see #SetError).
    //! A subscriber added after the error was set is invoked in situ.
    DECLARE_INTERFACE_SIGNAL(void(const TError&), Error);
};

DEFINE_REFCOUNTED_TYPE(IBlockDevice)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NNbd
