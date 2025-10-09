#pragma once

#include <yt/yt/core/misc/error.h>

#include <yt/yt/core/rpc/public.h>

#include <yt/yt/core/ytree/public.h>

#include <yt/yt/core/ypath/public.h>

namespace NYT::NOrchid {

////////////////////////////////////////////////////////////////////////////////

struct TOrchidOptions
{
    //! If set to a non-trivial error, resulting service looks like |#| when accessing
    //! service ancestors, and throws #Error when trying to access it or its internals.
    //! This is useful for get-with-attributes to work on service ancestors
    //! (see also: test_orchid.py:test_invalid_orchid).
    TError Error;

    //! Channel to the remote orchid service. Must be non-null if #Error.IsOK().
    NRpc::IChannelPtr Channel;

    //! Arbitrary path prefix to navigate inside remote orchid.
    NYPath::TYPath RemoteRoot = "/";

    //! If set, RPC request default timeout is set to #Timeout.
    std::optional<TDuration> Timeout;
};

//! Create YPath service representing remote orchid.
NYTree::IYPathServicePtr CreateOrchidYPathService(TOrchidOptions options);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrchid
