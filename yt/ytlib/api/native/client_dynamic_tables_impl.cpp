#include "client_impl.h"
#include "config.h"
#include "connection.h"
#include "transaction.h"

#include <yt/client/object_client/helpers.h>

#include <yt/client/tablet_client/table_mount_cache.h>

#include <yt/client/transaction_client/timestamp_provider.h>

#include <yt/ytlib/query_client/query_service_proxy.h>

#include <yt/ytlib/cypress_client/cypress_ypath_proxy.h>

#include <yt/ytlib/object_client/object_service_proxy.h>

#include <yt/ytlib/tablet_client/tablet_service_proxy.h>
#include <yt/ytlib/tablet_client/tablet_cell_bundle_ypath_proxy.h>

#include <yt/ytlib/transaction_client/action.h>

namespace NYT::NApi::NNative {

using namespace NConcurrency;
using namespace NYPath;
using namespace NYTree;
using namespace NYson;
using namespace NObjectClient;
using namespace NTransactionClient;
using namespace NTableClient;
using namespace NTabletClient;
using namespace NQueryClient;

////////////////////////////////////////////////////////////////////////////////

namespace {

template <class TReq>
void SetDynamicTableCypressRequestFullPath(TReq* /*req*/, const TYPath& /*fullPath*/)
{ }

template <>
void SetDynamicTableCypressRequestFullPath<NTabletClient::NProto::TReqMount>(
    NTabletClient::NProto::TReqMount* req,
    const TYPath& fullPath)
{
    req->set_path(fullPath);
}

} // namespace

std::vector<TTabletInfo> TClient::DoGetTabletInfos(
    const TYPath& path,
    const std::vector<int>& tabletIndexes,
    const TGetTabletsInfoOptions& options)
{
    const auto& tableMountCache = Connection_->GetTableMountCache();
    auto tableInfo = WaitFor(tableMountCache->GetTableInfo(path))
        .ValueOrThrow();

    tableInfo->ValidateDynamic();

    struct TSubrequest
    {
        TQueryServiceProxy::TReqGetTabletInfoPtr Request;
        std::vector<size_t> ResultIndexes;
    };

    THashMap<TCellId, TSubrequest> cellIdToSubrequest;

    for (size_t resultIndex = 0; resultIndex < tabletIndexes.size(); ++resultIndex) {
        auto tabletIndex = tabletIndexes[resultIndex];
        auto tabletInfo = tableInfo->GetTabletByIndexOrThrow(tabletIndex);
        auto& subrequest = cellIdToSubrequest[tabletInfo->CellId];
        if (!subrequest.Request) {
            auto channel = GetReadCellChannelOrThrow(tabletInfo->CellId);
            TQueryServiceProxy proxy(channel);
            proxy.SetDefaultTimeout(options.Timeout.value_or(Connection_->GetConfig()->DefaultGetTabletInfosTimeout));
            subrequest.Request = proxy.GetTabletInfo();
        }
        ToProto(subrequest.Request->add_tablet_ids(), tabletInfo->TabletId);
        subrequest.ResultIndexes.push_back(resultIndex);
    }

    std::vector<TFuture<TQueryServiceProxy::TRspGetTabletInfoPtr>> asyncRspsOrErrors;
    std::vector<const TSubrequest*> subrequests;
    for (const auto& pair : cellIdToSubrequest) {
        const auto& subrequest = pair.second;
        subrequests.push_back(&subrequest);
        asyncRspsOrErrors.push_back(subrequest.Request->Invoke());
    }

    auto rspsOrErrors = WaitFor(Combine(asyncRspsOrErrors))
        .ValueOrThrow();

    std::vector<TTabletInfo> results(tabletIndexes.size());
    for (size_t subrequestIndex = 0; subrequestIndex < rspsOrErrors.size(); ++subrequestIndex) {
        const auto& subrequest = *subrequests[subrequestIndex];
        const auto& rsp = rspsOrErrors[subrequestIndex];
        YT_VERIFY(rsp->tablets_size() == subrequest.ResultIndexes.size());
        for (size_t resultIndexIndex = 0; resultIndexIndex < subrequest.ResultIndexes.size(); ++resultIndexIndex) {
            auto& result = results[subrequest.ResultIndexes[resultIndexIndex]];
            const auto& tabletInfo = rsp->tablets(static_cast<int>(resultIndexIndex));
            result.TotalRowCount = tabletInfo.total_row_count();
            result.TrimmedRowCount = tabletInfo.trimmed_row_count();
            result.BarrierTimestamp = tabletInfo.barrier_timestamp();
        }
    }
    return results;
}

std::unique_ptr<IAttributeDictionary> TClient::ResolveExternalTable(
    const TYPath& path,
    TTableId* tableId,
    TCellTag* cellTag,
    const std::vector<TString>& extraAttributes)
{
    auto proxy = CreateReadProxy<TObjectServiceProxy>(TMasterReadOptions());
    auto batchReq = proxy->ExecuteBatch();

    {
        auto req = TTableYPathProxy::Get(path + "/@");
        std::vector<TString> attributeKeys{
            "id",
            "type",
            "external_cell_tag"
        };
        for (const auto& attribute : extraAttributes) {
            attributeKeys.push_back(attribute);
        }
        ToProto(req->mutable_attributes()->mutable_keys(), attributeKeys);
        batchReq->AddRequest(req, "get_attributes");
    }

    auto batchRspOrError = WaitFor(batchReq->Invoke());
    THROW_ERROR_EXCEPTION_IF_FAILED(GetCumulativeError(batchRspOrError), "Error getting attributes of table %v", path);

    const auto& batchRsp = batchRspOrError.Value();
    auto getAttributesRspOrError = batchRsp->GetResponse<TYPathProxy::TRspGet>("get_attributes");

    auto& rsp = getAttributesRspOrError.Value();
    auto attributes = ConvertToAttributes(TYsonString(rsp->value()));
    if (!IsTableType(attributes->Get<EObjectType>("type"))) {
        THROW_ERROR_EXCEPTION("%v is not a table");
    }
    *tableId = attributes->Get<TTableId>("id");
    *cellTag = attributes->Get<TCellTag>("external_cell_tag", PrimaryMasterCellTag);
    return attributes;
}

template <class TReq>
void TClient::ExecuteTabletServiceRequest(const TYPath& path, TReq* req)
{
    TTableId tableId;
    TCellTag cellTag;
    auto attributes = ResolveExternalTable(
        path,
        &tableId,
        &cellTag,
        {"path"});

    if (!IsTableType(TypeFromId(tableId))) {
        THROW_ERROR_EXCEPTION("Object %v is not a table", path);
    }

    TTransactionStartOptions txOptions;
    txOptions.Multicell = cellTag != PrimaryMasterCellTag;
    txOptions.CellTag = cellTag;
    auto asyncTransaction = StartNativeTransaction(
        NTransactionClient::ETransactionType::Master,
        txOptions);
    auto transaction = WaitFor(asyncTransaction)
        .ValueOrThrow();

    ToProto(req->mutable_table_id(), tableId);

    auto fullPath = attributes->Get<TString>("path");
    SetDynamicTableCypressRequestFullPath(req, fullPath);

    auto actionData = MakeTransactionActionData(*req);
    auto primaryCellId = GetNativeConnection()->GetPrimaryMasterCellId();
    transaction->AddAction(primaryCellId, actionData);

    if (cellTag != PrimaryMasterCellTag) {
        transaction->AddAction(ReplaceCellTagInId(primaryCellId, cellTag), actionData);
    }

    TTransactionCommitOptions commitOptions;
    commitOptions.CoordinatorCommitMode = ETransactionCoordinatorCommitMode::Lazy;
    commitOptions.Force2PC = true;

    WaitFor(transaction->Commit(commitOptions))
        .ThrowOnError();
}

void TClient::DoMountTable(
    const TYPath& path,
    const TMountTableOptions& options)
{
    if (Connection_->GetConfig()->UseTabletService) {
        NTabletClient::NProto::TReqMount req;
        if (options.FirstTabletIndex) {
            req.set_first_tablet_index(*options.FirstTabletIndex);
        }
        if (options.LastTabletIndex) {
            req.set_last_tablet_index(*options.LastTabletIndex);
        }
        if (options.CellId) {
            ToProto(req.mutable_cell_id(), options.CellId);
        }
        if (!options.TargetCellIds.empty()) {
            ToProto(req.mutable_target_cell_ids(), options.TargetCellIds);
        }
        req.set_freeze(options.Freeze);

        auto mountTimestamp = WaitFor(Connection_->GetTimestampProvider()->GenerateTimestamps())
            .ValueOrThrow();
        req.set_mount_timestamp(mountTimestamp);

        ExecuteTabletServiceRequest(path, &req);
    } else {
        auto req = TTableYPathProxy::Mount(path);
        SetMutationId(req, options);

        if (options.FirstTabletIndex) {
            req->set_first_tablet_index(*options.FirstTabletIndex);
        }
        if (options.LastTabletIndex) {
            req->set_last_tablet_index(*options.LastTabletIndex);
        }
        if (options.CellId) {
            ToProto(req->mutable_cell_id(), options.CellId);
        }
        if (!options.TargetCellIds.empty()) {
            ToProto(req->mutable_target_cell_ids(), options.TargetCellIds);
        }
        req->set_freeze(options.Freeze);

        auto mountTimestamp = WaitFor(Connection_->GetTimestampProvider()->GenerateTimestamps())
            .ValueOrThrow();
        req->set_mount_timestamp(mountTimestamp);

        auto proxy = CreateWriteProxy<TObjectServiceProxy>();
        WaitFor(proxy->Execute(req))
            .ThrowOnError();
    }
}

void TClient::DoUnmountTable(
    const TYPath& path,
    const TUnmountTableOptions& options)
{
    if (Connection_->GetConfig()->UseTabletService) {
        NTabletClient::NProto::TReqUnmount req;
        if (options.FirstTabletIndex) {
            req.set_first_tablet_index(*options.FirstTabletIndex);
        }
        if (options.LastTabletIndex) {
            req.set_last_tablet_index(*options.LastTabletIndex);
        }
        req.set_force(options.Force);

        ExecuteTabletServiceRequest(path, &req);
    } else {
        auto req = TTableYPathProxy::Unmount(path);
        SetMutationId(req, options);
        if (options.FirstTabletIndex) {
            req->set_first_tablet_index(*options.FirstTabletIndex);
        }
        if (options.LastTabletIndex) {
            req->set_last_tablet_index(*options.LastTabletIndex);
        }
        req->set_force(options.Force);

        auto proxy = CreateWriteProxy<TObjectServiceProxy>();
        WaitFor(proxy->Execute(req))
            .ThrowOnError();
    }
}

void TClient::DoRemountTable(
    const TYPath& path,
    const TRemountTableOptions& options)
{
    if (Connection_->GetConfig()->UseTabletService) {
        NTabletClient::NProto::TReqRemount req;
        if (options.FirstTabletIndex) {
            req.set_first_tablet_index(*options.FirstTabletIndex);
        }
        if (options.LastTabletIndex) {
            req.set_first_tablet_index(*options.LastTabletIndex);
        }

        ExecuteTabletServiceRequest(path, &req);
    } else {
        auto req = TTableYPathProxy::Remount(path);
        SetMutationId(req, options);
        if (options.FirstTabletIndex) {
            req->set_first_tablet_index(*options.FirstTabletIndex);
        }
        if (options.LastTabletIndex) {
            req->set_first_tablet_index(*options.LastTabletIndex);
        }

        auto proxy = CreateWriteProxy<TObjectServiceProxy>();
        WaitFor(proxy->Execute(req))
            .ThrowOnError();
    }
}

void TClient::DoFreezeTable(
    const TYPath& path,
    const TFreezeTableOptions& options)
{
    if (Connection_->GetConfig()->UseTabletService) {
        NTabletClient::NProto::TReqFreeze req;
        if (options.FirstTabletIndex) {
            req.set_first_tablet_index(*options.FirstTabletIndex);
        }
        if (options.LastTabletIndex) {
            req.set_last_tablet_index(*options.LastTabletIndex);
        }

        ExecuteTabletServiceRequest(path, &req);
    } else {
        auto req = TTableYPathProxy::Freeze(path);
        SetMutationId(req, options);
        if (options.FirstTabletIndex) {
            req->set_first_tablet_index(*options.FirstTabletIndex);
        }
        if (options.LastTabletIndex) {
            req->set_last_tablet_index(*options.LastTabletIndex);
        }

        auto proxy = CreateWriteProxy<TObjectServiceProxy>();
        WaitFor(proxy->Execute(req))
            .ThrowOnError();
    }
}

void TClient::DoUnfreezeTable(
    const TYPath& path,
    const TUnfreezeTableOptions& options)
{
    if (Connection_->GetConfig()->UseTabletService) {
        NTabletClient::NProto::TReqUnfreeze req;

        if (options.FirstTabletIndex) {
            req.set_first_tablet_index(*options.FirstTabletIndex);
        }
        if (options.LastTabletIndex) {
            req.set_last_tablet_index(*options.LastTabletIndex);
        }

        ExecuteTabletServiceRequest(path, &req);
    } else {
        auto req = TTableYPathProxy::Unfreeze(path);
        SetMutationId(req, options);
        if (options.FirstTabletIndex) {
            req->set_first_tablet_index(*options.FirstTabletIndex);
        }
        if (options.LastTabletIndex) {
            req->set_last_tablet_index(*options.LastTabletIndex);
        }

        auto proxy = CreateWriteProxy<TObjectServiceProxy>();
        WaitFor(proxy->Execute(req))
            .ThrowOnError();
    }
}

NTabletClient::NProto::TReqReshard TClient::MakeReshardRequest(
    const TReshardTableOptions& options)
{
    NTabletClient::NProto::TReqReshard req;
    if (options.FirstTabletIndex) {
        req.set_first_tablet_index(*options.FirstTabletIndex);
    }
    if (options.LastTabletIndex) {
        req.set_last_tablet_index(*options.LastTabletIndex);
    }
    return req;
}

TTableYPathProxy::TReqReshardPtr TClient::MakeYPathReshardRequest(
    const TYPath& path,
    const TReshardTableOptions& options)
{
    auto req = TTableYPathProxy::Reshard(path);
    SetMutationId(req, options);

    if (options.FirstTabletIndex) {
        req->set_first_tablet_index(*options.FirstTabletIndex);
    }
    if (options.LastTabletIndex) {
        req->set_last_tablet_index(*options.LastTabletIndex);
    }
    return req;
}

void TClient::DoReshardTableWithPivotKeys(
    const TYPath& path,
    const std::vector<TOwningKey>& pivotKeys,
    const TReshardTableOptions& options)
{
    if (Connection_->GetConfig()->UseTabletService) {
        auto req = MakeReshardRequest(options);
        ToProto(req.mutable_pivot_keys(), pivotKeys);
        req.set_tablet_count(pivotKeys.size());

        ExecuteTabletServiceRequest(path, &req);
    } else {
        auto req = MakeYPathReshardRequest(path, options);
        ToProto(req->mutable_pivot_keys(), pivotKeys);
        req->set_tablet_count(pivotKeys.size());

        auto proxy = CreateWriteProxy<TObjectServiceProxy>();
        WaitFor(proxy->Execute(req))
            .ThrowOnError();
    }
}

void TClient::DoReshardTableWithTabletCount(
    const TYPath& path,
    int tabletCount,
    const TReshardTableOptions& options)
{
    if (Connection_->GetConfig()->UseTabletService) {
        auto req = MakeReshardRequest(options);
        req.set_tablet_count(tabletCount);

        ExecuteTabletServiceRequest(path, &req);
    } else {
        auto req = MakeYPathReshardRequest(path, options);
        req->set_tablet_count(tabletCount);

        auto proxy = CreateWriteProxy<TObjectServiceProxy>();
        WaitFor(proxy->Execute(req))
            .ThrowOnError();
    }
}

std::vector<TTabletActionId> TClient::DoReshardTableAutomatic(
    const TYPath& path,
    const TReshardTableAutomaticOptions& options)
{
    if (options.FirstTabletIndex || options.LastTabletIndex) {
        THROW_ERROR_EXCEPTION("Tablet indices cannot be specified for automatic reshard");
    }

    TTableId tableId;
    TCellTag cellTag;
    auto attributes = ResolveExternalTable(
        path,
        &tableId,
        &cellTag,
        {"tablet_cell_bundle", "dynamic"});

    if (TypeFromId(tableId) != EObjectType::Table) {
        THROW_ERROR_EXCEPTION("Invalid object type: expected %v, got %v", EObjectType::Table, TypeFromId(tableId))
            << TErrorAttribute("path", path);
    }

    if (!attributes->Get<bool>("dynamic")) {
        THROW_ERROR_EXCEPTION("Table %v must be dynamic",
            path);
    }

    auto bundle = attributes->Get<TString>("tablet_cell_bundle");
    InternalValidatePermission("//sys/tablet_cell_bundles/" + ToYPathLiteral(bundle), EPermission::Use);

    auto req = TTableYPathProxy::ReshardAutomatic(FromObjectId(tableId));
    SetMutationId(req, options);
    req->set_keep_actions(options.KeepActions);
    auto proxy = CreateWriteProxy<TObjectServiceProxy>(cellTag);
    auto protoRsp = WaitFor(proxy->Execute(req))
        .ValueOrThrow();
    return FromProto<std::vector<TTabletActionId>>(protoRsp->tablet_actions());
}

void TClient::DoAlterTable(
    const TYPath& path,
    const TAlterTableOptions& options)
{
    auto req = TTableYPathProxy::Alter(path);
    SetTransactionId(req, options, true);
    SetMutationId(req, options);

    if (options.Schema) {
        ToProto(req->mutable_schema(), *options.Schema);
    }
    if (options.Dynamic) {
        req->set_dynamic(*options.Dynamic);
    }
    if (options.UpstreamReplicaId) {
        ToProto(req->mutable_upstream_replica_id(), *options.UpstreamReplicaId);
    }

    auto proxy = CreateWriteProxy<TObjectServiceProxy>();
    WaitFor(proxy->Execute(req))
        .ThrowOnError();
}

void TClient::DoTrimTable(
    const TYPath& path,
    int tabletIndex,
    i64 trimmedRowCount,
    const TTrimTableOptions& options)
{
    const auto& tableMountCache = Connection_->GetTableMountCache();
    auto tableInfo = WaitFor(tableMountCache->GetTableInfo(path))
        .ValueOrThrow();

    tableInfo->ValidateDynamic();
    tableInfo->ValidateOrdered();

    auto tabletInfo = tableInfo->GetTabletByIndexOrThrow(tabletIndex);

    auto channel = GetCellChannelOrThrow(tabletInfo->CellId);

    TTabletServiceProxy proxy(channel);
    proxy.SetDefaultTimeout(Connection_->GetConfig()->DefaultTrimTableTimeout);

    auto req = proxy.Trim();
    ToProto(req->mutable_tablet_id(), tabletInfo->TabletId);
    req->set_mount_revision(tabletInfo->MountRevision);
    req->set_trimmed_row_count(trimmedRowCount);

    WaitFor(req->Invoke())
        .ValueOrThrow();
}

void TClient::DoAlterTableReplica(
    TTableReplicaId replicaId,
    const TAlterTableReplicaOptions& options)
{
    InternalValidateTableReplicaPermission(replicaId, EPermission::Write);

    auto req = TTableReplicaYPathProxy::Alter(FromObjectId(replicaId));
    if (options.Enabled) {
        req->set_enabled(*options.Enabled);
    }
    if (options.Mode) {
        req->set_mode(static_cast<int>(*options.Mode));
    }
    if (options.PreserveTimestamps) {
        req->set_preserve_timestamps(*options.PreserveTimestamps);
    }
    if (options.Atomicity) {
        req->set_atomicity(static_cast<int>(*options.Atomicity));
    }

    auto cellTag = CellTagFromId(replicaId);
    auto proxy = CreateWriteProxy<TObjectServiceProxy>(cellTag);
    WaitFor(proxy->Execute(req))
        .ThrowOnError();
}

std::vector<TTabletActionId> TClient::DoBalanceTabletCells(
    const TString& tabletCellBundle,
    const std::vector<TYPath>& movableTables,
    const TBalanceTabletCellsOptions& options)
{
    InternalValidatePermission("//sys/tablet_cell_bundles/" + ToYPathLiteral(tabletCellBundle), EPermission::Use);

    std::vector<TFuture<TTabletCellBundleYPathProxy::TRspBalanceTabletCellsPtr>> cellResponses;

    if (movableTables.empty()) {
        auto cellTags = Connection_->GetSecondaryMasterCellTags();
        cellTags.push_back(Connection_->GetPrimaryMasterCellTag());
        auto req = TTabletCellBundleYPathProxy::BalanceTabletCells("//sys/tablet_cell_bundles/" + tabletCellBundle);
        SetMutationId(req, options);
        req->set_keep_actions(options.KeepActions);
        for (const auto& cellTag : cellTags) {
            auto proxy = CreateWriteProxy<TObjectServiceProxy>(cellTag);
            cellResponses.emplace_back(proxy->Execute(req));
        }
    } else {
        THashMap<TCellTag, std::vector<TTableId>> tablesByCells;

        for (const auto& path : movableTables) {
            TTableId tableId;
            TCellTag cellTag;
            auto attributes = ResolveExternalTable(
                path,
                &tableId,
                &cellTag,
                {"dynamic", "tablet_cell_bundle"});

            if (TypeFromId(tableId) != EObjectType::Table) {
                THROW_ERROR_EXCEPTION(
                    "Invalid object type: expected %v, got %v",
                    EObjectType::Table, TypeFromId(tableId))
                    << TErrorAttribute("path", path);
            }

            if (!attributes->Get<bool>("dynamic")) {
                THROW_ERROR_EXCEPTION("Table must be dynamic")
                    << TErrorAttribute("path", path);
            }

            auto actualBundle = attributes->Find<TString>("tablet_cell_bundle");
            if (!actualBundle || *actualBundle != tabletCellBundle) {
                THROW_ERROR_EXCEPTION("All tables must be from the tablet cell bundle %Qv", tabletCellBundle);
            }

            tablesByCells[cellTag].push_back(tableId);
        }

        for (const auto& [cellTag, tableIds] : tablesByCells) {
            auto req = TTabletCellBundleYPathProxy::BalanceTabletCells("//sys/tablet_cell_bundles/" + tabletCellBundle);
            req->set_keep_actions(options.KeepActions);
            SetMutationId(req, options);
            ToProto(req->mutable_movable_tables(), tableIds);
            auto proxy = CreateWriteProxy<TObjectServiceProxy>(cellTag);
            cellResponses.emplace_back(proxy->Execute(req));
        }
    }

    std::vector<TTabletActionId> tabletActions;
    for (auto& future : cellResponses) {
        auto errorOrRsp = WaitFor(future);
        if (errorOrRsp.IsOK()) {
            const auto& rsp = errorOrRsp.Value();
            auto tabletActionsFromCell = FromProto<std::vector<TTabletActionId>>(rsp->tablet_actions());
            tabletActions.insert(tabletActions.end(), tabletActionsFromCell.begin(), tabletActionsFromCell.end());
        } else {
            YT_LOG_DEBUG(errorOrRsp, "Tablet cell balancing subrequest failed");
        }
    }

    return tabletActions;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi::NNative
