#pragma once

#include "public.h"

#include <yt/yt/ytlib/api/native/connection.h>

#include <yt/yt/library/auth_server/public.h>

#include <yt/yt/client/api/connection.h>

#include <yt/yt/core/ytree/public.h>

namespace NYT::NApi {

////////////////////////////////////////////////////////////////////////////////

//! Creates a native or RPC connection depending on the config.
//!
//! \note Native part of connection options is ignored for an RPC connection.
IConnectionPtr CreateConnection(
    NYTree::INodePtr config,
    NNative::TConnectionOptions options = {});

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi
