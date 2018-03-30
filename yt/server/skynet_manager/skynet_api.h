#pragma once

#include "public.h"

#include <yt/core/actions/future.h>

namespace NYT {
namespace NSkynetManager {

////////////////////////////////////////////////////////////////////////////////

//! Wraps communication with skynet daemon.
struct ISkynetApi
    : virtual public TRefCounted
{
    virtual TFuture<void> AddResource(
        const TString& rbTorrentId,
        const TString& discoveryUrl,
        const TString& rbTorrent) = 0;

    virtual TFuture<void> RemoveResource(const TString& rbTorrentId) = 0;

    virtual TFuture<std::vector<TString>> ListResources() = 0;
};

DEFINE_REFCOUNTED_TYPE(ISkynetApi)

ISkynetApiPtr CreateShellSkynetApi(
    const IInvokerPtr& invoker,
    const TString& pythonInterpreterPath,
    const TString& mdsToolPath);

ISkynetApiPtr CreateNullSkynetApi();

////////////////////////////////////////////////////////////////////////////////

} // namespace NSkynetManager
} // namespace NYT
