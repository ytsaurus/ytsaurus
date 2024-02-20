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

namespace NYT::NTabletNode {

using namespace NYTree;
using namespace NConcurrency;
using namespace NClusterNode;
using namespace NLogging;
using namespace NTableClient;
using namespace NTabletClient;
using namespace NObjectClient;

////////////////////////////////////////////////////////////////////////////////

static NConcurrency::TFlsSlot<TErrorManagerContext> Context;

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

TErrorManager::TDeduplicationKey::TDeduplicationKey(
    const TErrorManagerContext& context,
    TString method,
    TString errorMessage)
    : TabletCellBundle(*context.TabletCellBundle)
    , TableId(context.TableId)
    , TabletId(context.TabletId)
    , Method(std::move(method))
    , ErrorMessage(std::move(errorMessage))
{ }

TErrorManager::TDeduplicationKey::operator size_t() const
{
    return MultiHash(TabletCellBundle, TableId, TabletId, Method, ErrorMessage);
}

TErrorManager::TErrorManager(IBootstrap const* bootstrap)
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

void TErrorManager::Start()
{
    ExpiredErrorsCleanerExecutor_->Start();
}

void TErrorManager::Reconfigure(const NClusterNode::TClusterNodeDynamicConfigPtr& newConfig)
{
    const auto& config = newConfig->TabletNode->ErrorManager;

    ErrorExpirationTimeout_.store(config->ErrorExpirationTimeout, std::memory_order::relaxed);
    ExpiredErrorsCleanerExecutor_->SetPeriod(config->ErrorExpirationTimeout);

    DeduplicationCache_->SetExpirationTimeout(config->DeduplicationCacheTimeout);
}

void TErrorManager::HandleError(const TError& error, const TString& method)
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

        LogStructuredEventFluently(TabletErrorsLogger, ELogLevel::Info)
            .Item("tablet_cell_bundle").Value(*context.TabletCellBundle)
            .Item("table_id").Value(context.TableId)
            .Item("tablet_id").Value(context.TabletId)
            .Item("timestamp").Value(Now().MicroSeconds())
            .Item("method").Value(method)
            .Item("error").Value(error)
        .Finish();
    }
}

void TErrorManager::MaybeDropError(TAtomicObject<TError>* atomicError, TInstant expirationTime)
{
    atomicError->Transform([expirationTime] (TError& error) {
        if (error.HasDatetime() && error.GetDatetime() <= expirationTime) {
            error = {};
        }
    });
}

void TErrorManager::RemoveExpiredErrors()
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

void TErrorManager::ExtractContext(const TError& error, TErrorManagerContext* context)
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

DEFINE_REFCOUNTED_TYPE(TErrorManager)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
