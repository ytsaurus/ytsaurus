#pragma once

#include "private.h"

namespace NYT::NClickHouseServer {

////////////////////////////////////////////////////////////////////////////////

struct IClickHouseServer
    : public TRefCounted
{
    virtual void Start() = 0;

    virtual void Stop() = 0;

    virtual DB::Context* GetContext() = 0;
};


DEFINE_REFCOUNTED_TYPE(IClickHouseServer);

////////////////////////////////////////////////////////////////////////////////

IClickHouseServerPtr CreateClickHouseServer(
    THost* host,
    TClickHouseConfigPtr config);

////////////////////////////////////////////////////////////////////////////////

} // namespace YT::NClickHouseServer
