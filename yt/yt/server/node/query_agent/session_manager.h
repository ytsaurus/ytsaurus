#pragma once

#include "public.h"

#include <yt/yt/ytlib/query_client/public.h>

#include <yt/yt/library/query/distributed/public.h>

#include <yt/yt/core/actions/public.h>

#include <yt/yt/core/compression/public.h>

#include <library/cpp/yt/memory/public.h>

namespace NYT::NQueryAgent {

////////////////////////////////////////////////////////////////////////////////

struct IDistributedSessionManager
    : public TRefCounted
{
    virtual IDistributedSessionPtr GetDistributedSessionOrCreate(
        NQueryClient::TDistributedSessionId sessionId,
        TDuration retentionTime,
        NCompression::ECodec codecId,
        std::optional<i64> memoryLimitPerNode,
        IMemoryChunkProviderPtr memoryChunkProvider) = 0;

    virtual IDistributedSessionPtr GetDistributedSessionOrThrow(
        NQueryClient::TDistributedSessionId sessionId) = 0;

    virtual bool CloseDistributedSession(
        NQueryClient::TDistributedSessionId sessionId) = 0;

    virtual void OnDistributedSessionLeaseExpired(
        NQueryClient::TDistributedSessionId sessionId) = 0;
};

DEFINE_REFCOUNTED_TYPE(IDistributedSessionManager)

////////////////////////////////////////////////////////////////////////////////

IDistributedSessionManagerPtr CreateDistributedSessionManager(IInvokerPtr invoker);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryAgent
