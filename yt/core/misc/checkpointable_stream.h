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

std::unique_ptr<ICheckpointableInputStream> CreateCheckpointableInputStream(
    TInputStream* underlyingStream);
std::unique_ptr<ICheckpointableInputStream> CreateFakeCheckpointableInputStream(
    TInputStream* underlyingStream);

std::unique_ptr<ICheckpointableOutputStream> CreateCheckpointableOutputStream(
    TOutputStream* underlyingStream);
std::unique_ptr<ICheckpointableOutputStream> CreateFakeCheckpointableOutputStream(
    TOutputStream* underlyingStream);
std::unique_ptr<ICheckpointableOutputStream> CreateBufferedCheckpointableOutputStream(
    ICheckpointableOutputStream* underlyingStream,
    size_t bufferSize = 8192);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
