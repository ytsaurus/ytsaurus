#pragma once

#include "public.h"

#include <yt/yt/core/concurrency/public.h>

#include <util/stream/input.h>
#include <util/stream/zerocopy_output.h>

#include <util/generic/size_literals.h>

namespace NYT::NHydra {

////////////////////////////////////////////////////////////////////////////////

struct ICheckpointableInputStream
    : public IZeroCopyInput
{
    virtual void SkipToCheckpoint() = 0;
    virtual i64 GetOffset() const = 0;
};

////////////////////////////////////////////////////////////////////////////////

struct ICheckpointableOutputStream
    : public IZeroCopyOutput
{
    virtual void MakeCheckpoint() = 0;
};

////////////////////////////////////////////////////////////////////////////////

//! Wraps an input stream making it checkpointable.
std::unique_ptr<ICheckpointableInputStream> CreateCheckpointableInputStream(
    IZeroCopyInput* underlyingStream);

//! Wraps a zero-copy output stream making it checkpointable.
/*!
 *  #underlyingStream must ensure that it is capable of providing a buffer of
 *  size larger than TCheckpointableStreamBlockHeader after a flush.
 *
 *  If not sure about the actual implementation of #underlyingStream,
 *  use #CreateBufferedCheckpointableOutputStream since the latter does not impose
 *  such a requirement.
 */
std::unique_ptr<ICheckpointableOutputStream> CreateCheckpointableOutputStream(
    IZeroCopyOutput* underlyingStream);

//! Wraps an output stream making it buffered and checkpointable.
std::unique_ptr<ICheckpointableOutputStream> CreateBufferedCheckpointableOutputStream(
    IOutputStream* underlyingStream,
    size_t bufferSize = 8_KB);

//! Creates a synchronous checkpointable buffering adapter from a given asynchronous stream.
std::unique_ptr<ICheckpointableOutputStream> CreateBufferedCheckpointableSyncAdapter(
    NConcurrency::IAsyncOutputStreamPtr underlyingStream,
    NConcurrency::EWaitForStrategy strategy,
    size_t bufferSize = 8_KB);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHydra
