#include "tablet_operation.h"

#include "client.h"
#include "connection.h"
#include "transaction.h"

#include <yt/yt/ytlib/table_client/helpers.h>

#include <yt/yt/ytlib/transaction_client/action.h>

#include <yt/yt/client/api/security_client.h>
#include <yt/yt/client/api/transaction.h>
#include <yt/yt/client/api/transaction_client.h>

#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt/core/concurrency/scheduler.h>

#include <yt/yt/core/ytree/helpers.h>

namespace NYT::NApi::NNative {
namespace {

using namespace NConcurrency;
using namespace NObjectClient;
using namespace NTableClient;
using namespace NTransactionClient;
using namespace NYPath;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

void ValidatePermission(
    const IClientPtr& client,
    const TYPath& path,
    EPermission permission)
{
    const auto& user = client->GetOptions().GetAuthenticatedUser();
    auto result = WaitFor(client->CheckPermission(user, path, permission))
        .ValueOrThrow();
    result.ToError(user, permission)
        .ThrowOnError();
}

template <class TMasterRequest, CTabletOperationRequest TRequest>
void CopyTabletRange(
    const TRequest& request,
    TMasterRequest* result)
{
    if (request.has_first_tablet_index()) {
        result->set_first_tablet_index(request.first_tablet_index());
    }
    if (request.has_last_tablet_index()) {
        result->set_last_tablet_index(request.last_tablet_index());
    }
}

} // namespace

NTabletClient::NProto::TReqMount NTabletOperationDetail::MakeMasterTabletOperationRequest(
    const NTableClient::NProto::TReqMount& request)
{
    NTabletClient::NProto::TReqMount result;
    CopyTabletRange(request, &result);
    if (request.has_cell_id()) {
        result.mutable_cell_id()->CopyFrom(request.cell_id());
    }
    result.set_freeze(request.freeze());
    result.set_mount_timestamp(request.mount_timestamp());
    result.mutable_target_cell_ids()->CopyFrom(request.target_cell_ids());
    return result;
}

NTabletClient::NProto::TReqUnmount NTabletOperationDetail::MakeMasterTabletOperationRequest(
    const NTableClient::NProto::TReqUnmount& request)
{
    NTabletClient::NProto::TReqUnmount result;
    CopyTabletRange(request, &result);
    result.set_force(request.force());
    return result;
}

NTabletClient::NProto::TReqRemount NTabletOperationDetail::MakeMasterTabletOperationRequest(
    const NTableClient::NProto::TReqRemount& request)
{
    NTabletClient::NProto::TReqRemount result;
    CopyTabletRange(request, &result);
    return result;
}

NTabletClient::NProto::TReqFreeze NTabletOperationDetail::MakeMasterTabletOperationRequest(
    const NTableClient::NProto::TReqFreeze& request)
{
    NTabletClient::NProto::TReqFreeze result;
    CopyTabletRange(request, &result);
    return result;
}

NTabletClient::NProto::TReqUnfreeze NTabletOperationDetail::MakeMasterTabletOperationRequest(
    const NTableClient::NProto::TReqUnfreeze& request)
{
    NTabletClient::NProto::TReqUnfreeze result;
    CopyTabletRange(request, &result);
    return result;
}

NTabletClient::NProto::TReqReshard NTabletOperationDetail::MakeMasterTabletOperationRequest(
    const NTableClient::NProto::TReqReshard& request)
{
    NTabletClient::NProto::TReqReshard result;
    CopyTabletRange(request, &result);
    result.mutable_pivot_keys()->CopyFrom(request.pivot_keys());
    result.set_tablet_count(request.tablet_count());
    result.mutable_trimmed_row_counts()->CopyFrom(request.trimmed_row_counts());
    result.mutable_cumulative_data_weights()->CopyFrom(request.cumulative_data_weights());
    return result;
}

void NTabletOperationDetail::DoExecuteTabletOperationViaMaster(
    const IClientPtr& client,
    const TTabletOperationTarget& target,
    TStringBuf action,
    TTransactionActionData actionData)
{
    auto transactionAttributes = CreateEphemeralAttributes();
    transactionAttributes->Set(
        "title",
        Format("%v node %v", action, target.RequestPath));

    TTransactionStartOptions transactionOptions;
    transactionOptions.Attributes = std::move(transactionAttributes);
    transactionOptions.SuppressStartTimestampGeneration = true;
    transactionOptions.CoordinatorMasterCellTag = target.NativeCellTag;
    transactionOptions.ReplicateToMasterCellTags = TCellTagList{target.ExternalCellTag};
    transactionOptions.StartCypressTransaction = false;

    auto transaction = WaitFor(client->StartNativeTransaction(
        ETransactionType::Master,
        transactionOptions))
        .ValueOrThrow();

    const auto& connection = client->GetNativeConnection();
    auto nativeCellId = connection->GetMasterCellId(target.NativeCellTag);
    auto externalCellId = connection->GetMasterCellId(target.ExternalCellTag);
    transaction->AddAction(nativeCellId, actionData);
    if (nativeCellId != externalCellId) {
        transaction->AddAction(externalCellId, actionData);
    }

    WaitFor(transaction->Commit(TTransactionCommitOptions{
        .Force2PC = true,
        .CoordinatorCommitMode = ETransactionCoordinatorCommitMode::Lazy,
        .CellIdsToSyncWithBeforePrepare = {nativeCellId},
    }))
        .ThrowOnError();
}

TTabletOperationTarget ResolveTabletOperationTarget(
    const IClientPtr& client,
    const TYPath& path)
{
    TTableId tableId;
    TCellTag externalCellTag;
    auto tableAttributes = ResolveExternalTable(
        client,
        path,
        &tableId,
        &externalCellTag,
        {"tablet_cell_bundle", "path"});

    if (IsSequoiaId(tableId)) {
        // COMPAT(h0pless): This is a quick and dirty fix for dynamic tables in Sequoia in 25.4.
        auto bundle = tableAttributes->Get<std::string>("tablet_cell_bundle");
        ValidatePermission(
            client,
            "//sys/tablet_cell_bundles/" + ToYPathLiteral(bundle),
            EPermission::Use);
        ValidatePermission(client, path, EPermission::Mount);
    }

    return TTabletOperationTarget{
        .TableId = tableId,
        .NativeCellTag = CellTagFromId(tableId),
        .ExternalCellTag = externalCellTag,
        .RequestPath = path,
        .FullPath = tableAttributes->Get<TYPath>("path"),
    };
}

TTabletOperationTarget ResolveTabletOperationTarget(
    TObjectServiceProxy& proxy,
    const TYPath& path)
{
    TTableId tableId;
    TCellTag externalCellTag;
    auto tableAttributes = ResolveExternalTable(
        proxy,
        path,
        &tableId,
        &externalCellTag,
        {"path"});

    return TTabletOperationTarget{
        .TableId = tableId,
        .NativeCellTag = CellTagFromId(tableId),
        .ExternalCellTag = externalCellTag,
        .RequestPath = path,
        .FullPath = tableAttributes->Get<TYPath>("path"),
    };
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi::NNative
