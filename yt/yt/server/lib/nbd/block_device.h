#pragma once

#include "public.h"

#include <yt/yt/core/actions/future.h>

#include <library/cpp/yt/memory/ref.h>

namespace NYT::NNbd {

////////////////////////////////////////////////////////////////////////////////

struct TWriteOptions
{
    //! The 'FUA (Force Unit Access) flag'.
    bool Flush = false;
};

//! Represents a block device that can be exposed via the NBD protocol.
struct IBlockDevice
    : public virtual TRefCounted
{
    virtual i64 GetTotalSize() const = 0;
    virtual bool IsReadOnly() const = 0;
    virtual TString DebugString() const = 0;
    virtual TString GetProfileSensorTag() const = 0;

    virtual TFuture<void> Initialize() = 0;

    virtual TFuture<TSharedRef> Read(
        i64 offset,
        i64 length) = 0;

    virtual TFuture<void> Write(
        i64 offset,
        const TSharedRef& data,
        const TWriteOptions& options = {}) = 0;

    virtual TFuture<void> Flush() = 0;
};

DEFINE_REFCOUNTED_TYPE(IBlockDevice)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NNbd
