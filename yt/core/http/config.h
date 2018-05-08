#pragma once

#include "public.h"

#include <yt/core/ytree/yson_serializable.h>

namespace NYT {
namespace NHttp {

////////////////////////////////////////////////////////////////////////////////

class TServerConfig
    : public NYTree::TYsonSerializable
{
public:
    int Port;

    //! Limit for number of open TCP connections.
    int MaxSimultaneousConnections;

    int ReadBufferSize;
    int WriteBufferSize;

    int BindRetryCount;
    TDuration BindRetryBackoff;

    TServerConfig();
};

DEFINE_REFCOUNTED_TYPE(TServerConfig);

////////////////////////////////////////////////////////////////////////////////

class TClientConfig
    : public NYTree::TYsonSerializable
{
public:
    int ReadBufferSize;
    int WriteBufferSize;

    TClientConfig();
};

DEFINE_REFCOUNTED_TYPE(TClientConfig);

////////////////////////////////////////////////////////////////////////////////

} // namespace NHttp
} // namespace NYT
