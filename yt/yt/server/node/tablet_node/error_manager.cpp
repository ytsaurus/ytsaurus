#include "error_manager.h"
#include "bootstrap.h"
#include "tablet.h"
#include "tablet_snapshot_store.h"

#include <yt/yt/server/node/cluster_node/config.h>
#include <yt/yt/server/node/cluster_node/dynamic_config_manager.h>

#include <yt/yt/server/lib/tablet_node/config.h>

#include <yt/yt/core/concurrency/fls.h>
#include <yt/yt/core/concurrency/periodic_executor.h>

#include <yt/yt/core/logging/fluent_log.h>

#include <yt/yt/core/misc/atomic_object.h>
#include <yt/yt/core/misc/sync_expiring_cache.h>

namespace NYT::NTabletNode {

using namespace NYTree;
using namespace NConcurrency;
using namespace NClusterNode;
using namespace NLogging;
using namespace NTableClient;
using namespace NTabletClient;
using namespace NObjectClient;

////////////////////////////////////////////////////////////////////////////////

namespace {

////////////////////////////////////////////////////////////////////////////////

NConcurrency::TFlsSlot<TErrorManagerContext> Context;

struct TDeduplicationKey
{
    TString TabletCellBundle;
    NTableClient::TTableId TableId;
    NTabletClient::TTabletId TabletId;
    TString Method;
    TString ErrorMessage;

    TDeduplicationKey(
        const TErrorManagerContext& context,
        TString method,
        TString errorMessage)
        : TabletCellBundle(*context.TabletCellBundle)
        , TableId(context.TableId)
        , TabletId(context.TabletId)
        , Method(std::move(method))
        , ErrorMessage(std::move(errorMessage))
    { }

    operator size_t() const
    {
        return MultiHash(TabletCellBundle, TableId, TabletId, Method, ErrorMessage);
    }

    bool operator==(const TDeduplicationKey& other) const = default;
};

using TDeduplicationCache = TSyncExpiringCache<TDeduplicationKey, std::monostate>;

////////////////////////////////////////////////////////////////////////////////

} // namespace

////////////////////////////////////////////////////////////////////////////////

TErrorManagerContext::operator bool() const
{
    return TabletCellBundle && TableId && TabletId;
}

void TErrorManagerContext::Reset()
{
    TabletCellBundle.reset();
    TableId = {};
    TabletId = {};
}

void SetErrorManagerContext(TErrorManagerContext context)
{
    YT_ASSERT(context);
    *Context = std::move(context);
}

void SetErrorManagerContextFromTabletSnapshot(const TTabletSnapshotPtr& tabletSnapshot)
{
    SetErrorManagerContext({
        .TabletCellBundle = tabletSnapshot->TabletCellBundle,
        .TableId = tabletSnapshot->TableId,
        .TabletId = tabletSnapshot->TabletId,
    });
}

void ResetErrorManagerContext()
{
    Context->Reset();
}

TError EnrichErrorForErrorManager(TError&& error, const TTabletSnapshotPtr& tabletSnapshot)
{
    return std::move(error)
        << TErrorAttribute("tablet_cell_bundle", tabletSnapshot->TabletCellBundle)
        << TErrorAttribute("table_id", tabletSnapshot->TableId)
        << TErrorAttribute("tablet_id", tabletSnapshot->TabletId);
}

////////////////////////////////////////////////////////////////////////////////

class TErrorManager
    : public IErrorManager
{
public:
    explicit TErrorManager(IBootstrap* bootstrap)
        : Bootstrap_(bootstrap)
        , ActionQueue_(New<TActionQueue>("ErrorManager"))
        , ExpiredErrorsCleanerExecutor_(New<TPeriodicExecutor>(
            ActionQueue_->GetInvoker(),
            BIND(&TErrorManager::RemoveExpiredErrors, MakeWeak(this))))
        , DeduplicationCache_(New<TDeduplicationCache>(
            BIND([] (const TDeduplicationKey& /*key*/) -> std::monostate {
                return {};
            }),
            std::nullopt,
            ActionQueue_->GetInvoker()))
    {
        Reconfigure(Bootstrap_->GetDynamicConfigManager()->GetConfig());
    }

    void Start() override
    {
        ExpiredErrorsCleanerExecutor_->Start();
    }

    void Reconfigure(const NClusterNode::TClusterNodeDynamicConfigPtr& newConfig) override
    {
        const auto& config = newConfig->TabletNode->ErrorManager;

        ErrorExpirationTimeout_.store(config->ErrorExpirationTimeout, std::memory_order::relaxed);
        ExpiredErrorsCleanerExecutor_->SetPeriod(config->ErrorExpirationTimeout);

        DeduplicationCache_->SetExpirationTimeout(config->DeduplicationCacheTimeout);
    }

    void HandleError(const TError& error, const TString& method) override
    {
        auto context = *Context;

        if (!context) {
            ExtractContext(error, &context);
            if (!context) {
                return;
            }
        }

        TDeduplicationKey deduplicationKey(context, method, error.GetMessage());
        if (!DeduplicationCache_->Find(deduplicationKey)) {
            DeduplicationCache_->Set(std::move(deduplicationKey), {});

            LogStructuredEventFluently(TabletErrorsLogger(), ELogLevel::Info)
                .Item("tablet_cell_bundle").Value(*context.TabletCellBundle)
                .Item("table_id").Value(context.TableId)
                .Item("tablet_id").Value(context.TabletId)
                .Item("timestamp").Value(Now().MicroSeconds())
                .Item("method").Value(method)
                .Item("error").Value(error)
            .Finish();
        }
    }

private:
    IBootstrap* const Bootstrap_;
    NConcurrency::TActionQueuePtr ActionQueue_;
    NConcurrency::TPeriodicExecutorPtr ExpiredErrorsCleanerExecutor_;
    TIntrusivePtr<TDeduplicationCache> DeduplicationCache_;
    std::atomic<TDuration> ErrorExpirationTimeout_;

    static void MaybeDropError(TAtomicObject<TError>* atomicError, TInstant expirationTime)
    {
        atomicError->Transform([expirationTime] (TError& error) {
            if (error.HasDatetime() && error.GetDatetime() <= expirationTime) {
                error = {};
            }
        });
    }

    void RemoveExpiredErrors()
    {
        auto expirationTime = Now() - ErrorExpirationTimeout_.load(std::memory_order::relaxed);
        for (const auto& tabletSnapshot : Bootstrap_->GetTabletSnapshotStore()->GetTabletSnapshots()) {
            auto& errors = tabletSnapshot->TabletRuntimeData->Errors;

            for (auto key : TEnumTraits<ETabletBackgroundActivity>::GetDomainValues()) {
                MaybeDropError(&errors.BackgroundErrors[key], expirationTime);
            }
            MaybeDropError(&errors.ConfigError, expirationTime);
        }
    }

    void ExtractContext(const TError& error, TErrorManagerContext* context)
    {
        const auto& attributes = error.Attributes();

        if (!context->TabletCellBundle) {
            context->TabletCellBundle = attributes.Find<TString>("tablet_cell_bundle");
        }
        if (!context->TableId) {
            context->TableId = attributes.Find<TTableId>("table_id").value_or(TTableId());
        }
        if (!context->TabletId) {
            context->TabletId = attributes.Find<TTabletId>("tablet_id").value_or(TTabletId());
        }

        if (*context) {
            return;
        }

        for (const auto& innerError : error.InnerErrors()) {
            ExtractContext(innerError, context);
        }
    }
};

DEFINE_REFCOUNTED_TYPE(TErrorManager)

IErrorManagerPtr CreateErrorManager(IBootstrap* bootstrap)
{
    return New<TErrorManager>(bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
