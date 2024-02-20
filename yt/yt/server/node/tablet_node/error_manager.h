#pragma once

#include "public.h"

#include <yt/yt/server/node/cluster_node/public.h>

#include <yt/yt/core/misc/atomic_object.h>
#include <yt/yt/core/misc/sync_expiring_cache.h>

namespace NYT::NTabletNode {

////////////////////////////////////////////////////////////////////////////////

struct TErrorManagerContext
{
    std::optional<TString> TabletCellBundle;
    NTableClient::TTableId TableId;
    NTabletClient::TTabletId TabletId;

    operator bool() const;
    void Reset();
};

void SetErrorManagerContext(TErrorManagerContext context);
void SetErrorManagerContextFromTabletSnapshot(const TTabletSnapshotPtr& tabletSnapshot);
void ResetErrorManagerContext();
TError EnrichErrorForErrorManager(TError&& error, const TTabletSnapshotPtr& tabletSnapshot);

////////////////////////////////////////////////////////////////////////////////

class TErrorManager
    : public TRefCounted
{
    struct TDeduplicationKey
    {
        TString TabletCellBundle;
        NTableClient::TTableId TableId;
        NTabletClient::TTabletId TabletId;
        TString Method;
        TString ErrorMessage;

        TDeduplicationKey(const TErrorManagerContext& context, TString method, TString errorMessage);

        operator size_t() const;
        bool operator==(const TDeduplicationKey& other) const = default;
    };

    using TDeduplicationCache = TSyncExpiringCache<TDeduplicationKey, std::monostate>;

public:
    explicit TErrorManager(IBootstrap const* bootstrap);

    void Start();

    void Reconfigure(const NClusterNode::TClusterNodeDynamicConfigPtr& newConfig);

    void HandleError(const TError& error, const TString& method);

private:
    IBootstrap const* Bootstrap_;
    NConcurrency::TActionQueuePtr ActionQueue_;
    NConcurrency::TPeriodicExecutorPtr ExpiredErrorsCleanerExecutor_;
    TIntrusivePtr<TDeduplicationCache> DeduplicationCache_;
    std::atomic<TDuration> ErrorExpirationTimeout_;

    static void MaybeDropError(TAtomicObject<TError>* atomicError, TInstant expirationTime);

    void RemoveExpiredErrors();

    void ExtractContext(const TError& error, TErrorManagerContext* context);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
