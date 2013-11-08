#pragma once

#include "blob.h"

#include <util/stream/input.h>
#include <util/stream/output.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

struct ICheckpointableInputStream
    : public TInputStream
{
public:
    virtual void Skip() = 0;
};

////////////////////////////////////////////////////////////////////////////////

class ICheckpointableOutputStream
    : public TOutputStream
{
public:
    virtual void MakeCheckpoint() = 0;
};

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<ICheckpointableInputStream> CreateCheckpointableInputStream(
    TInputStream* underlyingStream);

std::unique_ptr<ICheckpointableOutputStream> CreateCheckpointableOutputStream(
    TOutputStream* underlyingStream);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
