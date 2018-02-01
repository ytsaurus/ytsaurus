#pragma once

#include "public.h"

#include "config.h"

#include <yt/core/concurrency/public.h>

#include <yt/core/net/public.h>

#include <yt/core/http/public.h>

namespace NYT {
namespace NSkynetManager {

////////////////////////////////////////////////////////////////////////////////

struct TBootstrap
    : public virtual TRefCounted
{
    explicit TBootstrap(TSkynetManagerConfigPtr config);

    void Start();
    void Stop();

    TSkynetManagerConfigPtr Config;

    NConcurrency::IPollerPtr Poller;

    NNet::IListenerPtr HttpListener;
    NHttp::IServerPtr HttpServer;

    NHttp::IClientPtr HttpClient;

    NConcurrency::TActionQueuePtr SkynetApiActionQueue;    
    ISkynetApiPtr SkynetApi;

    TSkynetManagerPtr Manager;
};

DEFINE_REFCOUNTED_TYPE(TBootstrap)

////////////////////////////////////////////////////////////////////////////////

} // namespace NSkynetManager
} // namespace NYT
