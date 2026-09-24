#include "two_phase_table_operation.h"

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

using namespace NConcurrency;
using namespace NObjectClient;
using namespace NTableClient;
using namespace NTransactionClient;
using namespace NYPath;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

namespace {

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

////////////////////////////////////////////////////////////////////////////////

} // namespace

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

////////////////////////////////////////////////////////////////////////////////

void DoExecuteTwoPhaseTableOperationViaMaster(
    const IClientPtr& client,
    const TTwoPhaseTableOperationTarget& target,
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

////////////////////////////////////////////////////////////////////////////////

} // namespace NDetail

////////////////////////////////////////////////////////////////////////////////

TTwoPhaseTableOperationTarget ResolveTwoPhaseTableOperationTarget(
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

    return TTwoPhaseTableOperationTarget{
        .TableId = tableId,
        .NativeCellTag = CellTagFromId(tableId),
        .ExternalCellTag = externalCellTag,
        .RequestPath = path,
        .FullPath = tableAttributes->Get<TYPath>("path"),
    };
}

TTwoPhaseTableOperationTarget ResolveTwoPhaseTableOperationTarget(
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

    return TTwoPhaseTableOperationTarget{
        .TableId = tableId,
        .NativeCellTag = CellTagFromId(tableId),
        .ExternalCellTag = externalCellTag,
        .RequestPath = path,
        .FullPath = tableAttributes->Get<TYPath>("path"),
    };
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi::NNative
