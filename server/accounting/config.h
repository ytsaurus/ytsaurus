#pragma once

#include "public.h"

#include <yp/server/objects/config.h>

#include <yp/server/net/config.h>

#include <yp/server/nodes/config.h>

#include <yp/server/scheduler/config.h>

#include <yt/ytlib/program/config.h>

#include <yt/client/api/config.h>

#include <yt/ytlib/auth/config.h>

#include <yt/core/http/config.h>

#include <yt/core/https/config.h>

#include <yt/core/rpc/grpc/config.h>

#include <yt/core/ypath/public.h>

namespace NYP {
namespace NServer {
namespace NAccounting {

////////////////////////////////////////////////////////////////////////////////

class TAccountingManagerConfig
    : public NYT::NYTree::TYsonSerializable
{
public:
    TAccountingManagerConfig()
    {
    }
};

DEFINE_REFCOUNTED_TYPE(TAccountingManagerConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NAccounting
} // namespace NNodes
} // namespace NYP
