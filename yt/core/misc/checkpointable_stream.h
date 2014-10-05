#pragma once

#include "public.h"

#include <util/stream/input.h>
#include <util/stream/output.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

struct ICheckpointableInputStream
    : public TInputStream
{
    virtual void SkipToCheckpoint() = 0;
};

////////////////////////////////////////////////////////////////////////////////

struct ICheckpointableOutputStream
    : public TOutputStream
{
    virtual void MakeCheckpoint() = 0;
};

////////////////////////////////////////////////////////////////////////////////

//! Wraps an input stream making it checkpointable.
std::unique_ptr<ICheckpointableInputStream> CreateCheckpointableInputStream(
    TInputStream* underlyingStream);

//! Wraps an output stream making it checkpointable.
std::unique_ptr<ICheckpointableOutputStream> CreateCheckpointableOutputStream(
    TOutputStream* underlyingStream);

//! Wraps an input stream constructing another one whose binary content
//! can be parsed by a checkpointable stream parser as a single block.
//! Used for migrating pre-0.17 snapshots that were not checkpointable.
std::unique_ptr<TInputStream> CreateFakeCheckpointableInputStream(
    TInputStream* underlyingStream,
    size_t underlyingStreamLength);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
