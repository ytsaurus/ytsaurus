#include "helpers.h"
#include "transaction_manager.h"

#include <yt/yt/ytlib/cypress_client/rpc_helpers.h>

#include <yt/yt/ytlib/object_client/proto/object_ypath.pb.h>

#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt/client/api/transaction.h>

#include <yt/yt/core/rpc/client.h>

namespace NYT::NTransactionClient {

using namespace NApi;
using namespace NRpc;
using namespace NObjectClient;

////////////////////////////////////////////////////////////////////////////////

void SetTransactionId(IClientRequestPtr request, ITransactionPtr transaction)
{
    NCypressClient::SetTransactionId(
        request,
        transaction ? transaction->GetId() : NullTransactionId);
}

void SetPrerequisites(
    const IClientRequestPtr& request,
    const TPrerequisiteOptions& options)
{
    if (options.PrerequisiteTransactionIds.empty() && options.PrerequisiteRevisions.empty()) {
        return;
    }

    auto* prerequisitesExt = request->Header().MutableExtension(NObjectClient::NProto::TPrerequisitesExt::prerequisites_ext);
    for (auto id : options.PrerequisiteTransactionIds) {
        auto* prerequisiteTransaction = prerequisitesExt->add_transactions();
        ToProto(prerequisiteTransaction->mutable_transaction_id(), id);
    }
    for (const auto& revision : options.PrerequisiteRevisions) {
        auto* prerequisiteRevision = prerequisitesExt->add_revisions();
        prerequisiteRevision->set_path(revision->Path);
        prerequisiteRevision->set_revision(revision->Revision);
    }
}

void SetSuppressUpstreamSyncs(
    const TObjectServiceProxy::TReqExecuteBatchBasePtr& request,
    const TTransactionalOptions& options)
{
    request->SetSuppressUpstreamSync(options.SuppressUpstreamSync);
    request->SetSuppressTransactionCoordinatorSync(options.SuppressTransactionCoordinatorSync);
}

TTransactionId MakeTabletTransactionId(
    EAtomicity atomicity,
    TCellTag cellTag,
    TTimestamp startTimestamp,
    ui32 hash)
{
    EObjectType type;
    switch (atomicity) {
        case EAtomicity::Full:
            type = EObjectType::AtomicTabletTransaction;
            break;
        case EAtomicity::None:
            type = EObjectType::NonAtomicTabletTransaction;
            break;
        default:
            YT_ABORT();
    }

    return MakeId(
        type,
        cellTag,
        startTimestamp,
        hash);
}

TTransactionId MakeExternalizedTransactionId(
    TTransactionId originalId,
    TCellTag externalizingCellTag)
{
    if (!originalId) {
        return {};
    }

    auto originalType = TypeFromId(originalId);
    YT_VERIFY(originalType == EObjectType::Transaction || originalType == EObjectType::NestedTransaction);
    auto externalizedType = (originalType == EObjectType::Transaction)
        ? EObjectType::ExternalizedTransaction
        : EObjectType::ExternalizedNestedTransaction;

    auto nativeCellTag = CellTagFromId(originalId);
    return TTransactionId(
        (originalId.Parts32[0] &  0xffff) | (static_cast<ui32>(nativeCellTag.Underlying()) << 16), // keep the original cell tag
        (static_cast<ui32>(externalizingCellTag.Underlying()) << 16) | static_cast<ui32>(externalizedType), // replace type and native cell tag
        originalId.Parts32[2],
        originalId.Parts32[3]);
}

TTransactionId OriginalFromExternalizedTransactionId(TTransactionId externalizedId)
{
    if (!externalizedId) {
        return {};
    }

    auto externalizedType = TypeFromId(externalizedId);
    YT_VERIFY(externalizedType == EObjectType::ExternalizedTransaction || externalizedType == EObjectType::ExternalizedNestedTransaction);
    auto originalType = (externalizedType == EObjectType::ExternalizedTransaction)
        ? EObjectType::Transaction
        : EObjectType::NestedTransaction;

    auto originalCellTag = (externalizedId.Parts32[0] >> 16);
    return TTransactionId(
        (externalizedId.Parts32[0] &  0xffff),                     // erase the original cell tag
        (originalCellTag << 16) | static_cast<ui32>(originalType), // replace type and restore the original cell tag
        externalizedId.Parts32[2],
        externalizedId.Parts32[3]);
}

TTimestamp TimestampFromTransactionId(TTransactionId id)
{
    if (IsSequoiaId(id)) {
        // Sequoia master transaction.
        return TimestampFromId(id);
    } else {
        // Tablet transaction or non-Sequoia master transaction.
        return TTimestamp(CounterFromId(id));
    }
}

EAtomicity AtomicityFromTransactionId(TTransactionId id)
{
    switch (TypeFromId(id)) {
        case EObjectType::Transaction:
        case EObjectType::AtomicTabletTransaction:
            return EAtomicity::Full;

        case EObjectType::NonAtomicTabletTransaction:
            return EAtomicity::None;

        default:
            YT_ABORT();
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTransactionClient

