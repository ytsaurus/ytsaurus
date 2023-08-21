#pragma once

#include "public.h"

#include <yt/yt/ytlib/tablet_client/public.h>
#include <yt/yt/ytlib/tablet_client/helpers.h>

#include <yt/yt/core/actions/public.h>

namespace NYT::NTabletNode {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(ILookupSession)

struct ILookupSession
    : public TRefCounted
{
    virtual void AddTabletRequest(
        TTabletId tabletId,
        TCellId cellId,
        NHydra::TRevision mountRevision,
        TSharedRef requestData) = 0;

    virtual TFuture<std::vector<TSharedRef>> Run() = 0;
};

DEFINE_REFCOUNTED_TYPE(ILookupSession)

////////////////////////////////////////////////////////////////////////////////

ILookupSessionPtr CreateLookupSession(
    NTabletClient::EInMemoryMode inMemoryMode,
    int tabletRequestCount,
    NCompression::ICodec* responseCodec,
    int maxRetryCount,
    int maxSubqueries,
    NTransactionClient::TReadTimestampRange timestampRange,
    std::optional<bool> useLookupCache,
    NChunkClient::TClientChunkReadOptions chunkReadOptions,
    NTableClient::TRetentionConfigPtr retentionConfig,
    bool enablePartialResult,
    const ITabletSnapshotStorePtr& snapshotStore,
    std::optional<TString> profilingUser,
    IInvokerPtr invoker);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
