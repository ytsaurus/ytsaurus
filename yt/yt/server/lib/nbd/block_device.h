#pragma once

#include "public.h"

#include <yt/yt/core/actions/future.h>
#include <yt/yt/core/actions/signal.h>

#include <yt/yt/core/ytree/public.h>

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

struct TFlushOptions
{
    //! Request id issued by linux kernel (NBD module).
    ui64 Cookie = 0;
};

////////////////////////////////////////////////////////////////////////////////

struct TTrimOptions
{
    //! Request id issued by linux kernel (NBD module).
    ui64 Cookie = 0;
};

////////////////////////////////////////////////////////////////////////////////

//! Represents a block device that can be exposed via the NBD protocol.
struct IBlockDevice
    : public virtual TRefCounted
{
    //! Returns the total byte size of the device. Always divisible by block size.
    virtual i64 GetTotalSize() const = 0;

    //! Returns the minimum I/O granularity (offset and length alignment) that
    //! must be honored by the callers.
    virtual i64 GetBlockSize() const = 0;

    virtual bool IsReadOnly() const = 0;
    virtual std::string GetDescription() const = 0;
    virtual std::string GetProfileSensorTag() const = 0;
    virtual NYTree::IYPathServicePtr GetOrchidService() = 0;

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

    virtual TFuture<void> Flush(const TFlushOptions& options = {}) = 0;

    //! Whether the device implements #Trim; only such a device is advertised as trimmable.
    virtual bool IsTrimSupported() const = 0;

    //! Discards the contents of |[offset, offset + length)|, letting the device reclaim their space.
    /*!
     *  Advisory: the device may discard less than requested -- typically only the blocks the range
     *  fully covers -- or nothing at all. Whatever is discarded reads back as zeroes.
     */
    virtual TFuture<void> Trim(
        i64 offset,
        i64 length,
        const TTrimOptions& options = {}) = 0;

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
