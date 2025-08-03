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

    virtual const TError& GetError() const = 0;
    virtual void SetError(TError error) = 0;

    virtual void OnShouldStopUsingDevice() const = 0;
    DECLARE_INTERFACE_SIGNAL(void(), ShouldStopUsingDevice);
};

DEFINE_REFCOUNTED_TYPE(IBlockDevice)

////////////////////////////////////////////////////////////////////////////////

class TBaseBlockDevice
    : public IBlockDevice
{
public:
    const TError& GetError() const override;
    void SetError(TError error) override;

    void OnShouldStopUsingDevice() const override;
    DEFINE_SIGNAL_OVERRIDE(void(), ShouldStopUsingDevice);

private:
    TError Error_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NNbd
