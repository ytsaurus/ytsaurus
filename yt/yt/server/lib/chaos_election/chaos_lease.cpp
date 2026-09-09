#include "chaos_lease.h"

#include "private.h"

#include <yt/yt/client/api/client.h>

#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt/core/ytree/attributes.h>
#include <yt/yt/core/ytree/convert.h>

namespace NYT::NChaosElection {

using namespace NApi;
using namespace NChaosClient;
using namespace NObjectClient;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

constinit const auto Logger = ChaosElectionLogger;

////////////////////////////////////////////////////////////////////////////////

TChaosLeaseFactory::TChaosLeaseFactory(
    IClientPtr client,
    std::string chaosCellBundle,
    TDuration cellIdsExpirationTime)
    : Client_(std::move(client))
    , ChaosCellBundle_(std::move(chaosCellBundle))
    , CellIdsExpirationTime_(cellIdsExpirationTime)
{ }

TFuture<TChaosLeaseId> TChaosLeaseFactory::CreateLease(
    TDuration timeout,
    IAttributeDictionaryPtr attributes)
{
    return GetCellIds(/*forceRefresh*/ false)
        .Apply(BIND([=, this, this_ = MakeStrong(this)] (const std::vector<TCellId>& cellIds) {
            return CreateLeaseOnCells(RotateCells(cellIds), /*index*/ 0, timeout, attributes, /*refreshed*/ false);
        }));
}

std::vector<TCellId> TChaosLeaseFactory::RotateCells(std::vector<TCellId> cellIds)
{
    // Cells known to be disabled go last. They are still tried, since lease serving migrates
    // between sibling cells, but they no longer cost a rejected request per creation.
    auto enabledEnd = cellIds.end();
    {
        auto guard = Guard(CellIdsLock_);
        if (!NotEnabledCellIds_.empty()) {
            enabledEnd = std::stable_partition(cellIds.begin(), cellIds.end(), [&] (TCellId cellId) {
                return !NotEnabledCellIds_.contains(cellId);
            });
        }
    }

    // Each creation starts from its own cell, so that the leases spread evenly instead of piling
    // up on whichever cell answered first. Only the enabled cells take part: starting the walk at
    // a cell known to reject would waste a request on every creation.
    if (auto enabledCount = enabledEnd - cellIds.begin(); enabledCount > 1) {
        auto shift = NextCellIndex_.fetch_add(1) % enabledCount;
        std::rotate(cellIds.begin(), cellIds.begin() + shift, enabledEnd);
    }
    return cellIds;
}

TFuture<std::vector<TCellId>> TChaosLeaseFactory::GetCellIds(bool forceRefresh)
{
    TPromise<std::vector<TCellId>> promise;
    {
        auto guard = Guard(CellIdsLock_);
        if (!forceRefresh && !CellIds_.empty() && TInstant::Now() < CellIdsDeadline_) {
            return MakeFuture(CellIds_);
        }
        // Tens of thousands of leases are created in bursts; without sharing the refetch every one
        // of them would issue its own Cypress request the moment the cache goes stale.
        if (CellIdsFuture_) {
            return CellIdsFuture_;
        }
        promise = NewPromise<std::vector<TCellId>>();
        CellIdsFuture_ = promise.ToFuture();
    }

    auto path = Format("//sys/chaos_cell_bundles/%v/@tablet_cell_ids", ChaosCellBundle_);

    TGetNodeOptions options;
    // A stale answer is harmless here: an absent cell is skipped by the walk, and a new one
    // arrives with the next refresh.
    options.ReadFrom = EMasterChannelKind::Cache;

    Client_->GetNode(path, options)
        .Subscribe(BIND([this, this_ = MakeStrong(this), promise] (const TErrorOr<NYson::TYsonString>& resultOrError) mutable {
            TErrorOr<std::vector<TCellId>> cellIdsOrError;
            if (resultOrError.IsOK()) {
                try {
                    cellIdsOrError = ConvertTo<std::vector<TCellId>>(resultOrError.Value());
                } catch (const std::exception& ex) {
                    cellIdsOrError = TError(ex);
                }
            } else {
                cellIdsOrError = TError(resultOrError);
            }

            {
                auto guard = Guard(CellIdsLock_);
                CellIdsFuture_.Reset();
                if (cellIdsOrError.IsOK()) {
                    CellIds_ = cellIdsOrError.Value();
                    NotEnabledCellIds_.clear();
                    CellIdsDeadline_ = TInstant::Now() + CellIdsExpirationTime_;
                } else if (!CellIds_.empty()) {
                    // The cached list is stale, not wrong, and creation validates every cell it
                    // walks anyway.
                    YT_LOG_DEBUG(cellIdsOrError, "Failed to refresh chaos cell ids, using the cached ones");
                    cellIdsOrError = CellIds_;
                }
            }

            promise.Set(std::move(cellIdsOrError));
        }));

    return promise.ToFuture();
}

TFuture<TChaosLeaseId> TChaosLeaseFactory::CreateLeaseOnCells(
    std::vector<TCellId> cellIds,
    int index,
    TDuration timeout,
    IAttributeDictionaryPtr attributes,
    bool refreshed)
{
    if (index >= std::ssize(cellIds)) {
        // The whole cached list turned out to be stale — the bundle could have been reconfigured.
        if (!refreshed) {
            return GetCellIds(/*forceRefresh*/ true)
                .Apply(BIND([=, this, this_ = MakeStrong(this)] (const std::vector<TCellId>& freshCellIds) {
                    return CreateLeaseOnCells(freshCellIds, /*index*/ 0, timeout, attributes, /*refreshed*/ true);
                }));
        }
        return MakeFuture<TChaosLeaseId>(TError("No enabled chaos cell found in bundle %Qv",
            ChaosCellBundle_));
    }

    auto cellId = cellIds[index];

    auto leaseAttributes = attributes ? attributes->Clone() : CreateEphemeralAttributes();
    leaseAttributes->Set("chaos_cell_id", cellId);
    leaseAttributes->Set("timeout", timeout);

    TCreateObjectOptions options;
    options.Attributes = std::move(leaseAttributes);

    return Client_->CreateObject(EObjectType::ChaosLease, options)
        .Apply(BIND([=, this, this_ = MakeStrong(this)] (const TErrorOr<TObjectId>& leaseIdOrError) {
            if (leaseIdOrError.IsOK()) {
                {
                    auto guard = Guard(CellIdsLock_);
                    NotEnabledCellIds_.erase(cellId);
                }
                return MakeFuture<TChaosLeaseId>(leaseIdOrError.Value());
            }

            if (!leaseIdOrError.FindMatching(NChaosClient::EErrorCode::ChaosCellIsNotEnabled)) {
                return MakeFuture<TChaosLeaseId>(TError(leaseIdOrError));
            }

            {
                auto guard = Guard(CellIdsLock_);
                NotEnabledCellIds_.insert(cellId);
            }
            YT_LOG_DEBUG("Chaos cell is not enabled, trying next (CellId: %v)",
                cellId);
            return CreateLeaseOnCells(cellIds, index + 1, timeout, attributes, refreshed);
        }));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChaosElection
