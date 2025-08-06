#include "transactions.h"

#include <yt/yt/client/api/transaction.h>

#include <yt/yt/client/object_client/helpers.h>
#include <yt/yt/client/object_client/public.h>

#include <yt/yt/ytlib/api/native/client.h>

#include <yt/yt/core/ytree/convert.h>
#include <yt/yt/core/ytree/fluent.h>

#include <yt/yt/core/misc/guid.h>
#include <yt/yt/core/misc/protobuf_helpers.h>

#include <util/digest/numeric.h>

namespace NYT::NScheduler {

using namespace NYT::NTransactionClient;
using namespace NYT::NApi;
using namespace NYT::NLogging;
using namespace NYT::NYTree;
using namespace NYT::NYson;
using namespace NYT::NObjectClient;

using NYT::FromProto;
using NYT::ToProto;

////////////////////////////////////////////////////////////////////////////////

TRichTransactionId MakeRichTransactionId(TTransactionId id)
{
    return TRichTransactionId { .Id = id };
}

void FormatValue(TStringBuilderBase* builder, const TRichTransactionId& transactionId, TStringBuf /*spec*/)
{
    builder->AppendString(ConvertToYsonString(transactionId, EYsonFormat::Text).ToString());
}

void Deserialize(TRichTransactionId& transaction, const NYTree::INodePtr& node)
{
    transaction.Id = node->GetValue<TTransactionId>();
    transaction.ParentId = node->Attributes().Get<TTransactionId>("parent_id");
    auto clusterNode = node->Attributes().ToMap()->FindChild("cluster");
    if (clusterNode && clusterNode->GetType() == ENodeType::String) {
        transaction.Cluster = clusterNode->GetValue<TClusterName>();
    }
}

void Serialize(const TRichTransactionId& transaction, NYson::IYsonConsumer* consumer)
{
    NYTree::BuildYsonFluently(consumer)
        .BeginAttributes()
            .Item("parent_id").Value(transaction.ParentId)
            .Item("cluster").Do(
                [&transaction] (TFluentAny fluent) {
                    if (IsLocal(transaction.Cluster)) {
                        fluent.Entity();
                    } else {
                        // NB(coteeq): Underlying is intentional here.
                        // Value in Cypress should be raw (unformatted) string,
                        // rather than human-intended "<local>" notation.
                        fluent.Value(*transaction.Cluster.Underlying());
                    }
                })
        .EndAttributes()
        .Value(transaction.Id);
}

void ToProto(NProto::TRichTransactionId* transactionIdProto, const TRichTransactionId& transactionId)
{
    ToProto(transactionIdProto->mutable_id(), transactionId.Id);
    ToProto(transactionIdProto->mutable_parent_id(), transactionId.ParentId);
    YT_OPTIONAL_TO_PROTO(transactionIdProto, cluster, transactionId.Cluster.Underlying());
}

void FromProto(TRichTransactionId* transactionId, const NProto::TRichTransactionId& transactionIdProto)
{
    transactionId->Id = FromProto<TTransactionId>(transactionIdProto.id());
    transactionId->ParentId = FromProto<TTransactionId>(transactionIdProto.parent_id());
    transactionId->Cluster = TClusterName(YT_OPTIONAL_FROM_PROTO(transactionIdProto, cluster));
    // COMPAT(coteeq): Remove in 25.4
    if (const auto& underlying = transactionId->Cluster.Underlying(); underlying && underlying->empty()) {
        transactionId->Cluster = LocalClusterName;
    }
}

IAttributeDictionaryPtr TControllerTransactionIds::ToCypressAttributes() const
{
    return BuildAttributeDictionaryFluently()
        .Item("async_scheduler_transaction_id").Value(AsyncId)
        .Item("input_transaction_id").Value(InputId)
        .Item("output_transaction_id").Value(OutputId)
        .Item("debug_transaction_id").Value(DebugId)
        .Item("output_completion_transaction_id").Value(OutputCompletionId)
        .Item("debug_completion_transaction_id").Value(DebugCompletionId)
        .Item("input_transaction_ids").Value(InputIds)
        .Finish();
}

TControllerTransactionIds TControllerTransactionIds::FromCypressAttributes(NYTree::IAttributeDictionaryPtr attributes)
{
    return TControllerTransactionIds {
        .AsyncId = attributes->Get<TTransactionId>("async_scheduler_transaction_id", NullTransactionId),
        .InputId = attributes->Get<TTransactionId>("input_transaction_id", NullTransactionId),
        .OutputId = attributes->Get<TTransactionId>("output_transaction_id", NullTransactionId),
        .DebugId = attributes->Get<TTransactionId>("debug_transaction_id", NullTransactionId),
        .OutputCompletionId = attributes->Get<TTransactionId>("output_completion_transaction_id", NullTransactionId),
        .DebugCompletionId = attributes->Get<TTransactionId>("debug_completion_transaction_id", NullTransactionId),
        .InputIds = attributes->Get<std::vector<TRichTransactionId>>("input_transaction_ids", {}),
    };
}

const std::vector<TString> TControllerTransactionIds::AttributeKeys = {
    "async_scheduler_transaction_id",
    "input_transaction_id",
    "output_transaction_id",
    "debug_transaction_id",
    "output_completion_transaction_id",
    "debug_completion_transaction_id",
    "input_transaction_ids",
};

void ToProto(NControllerAgent::NProto::TControllerTransactionIds* transactionIdsProto, const TControllerTransactionIds& transactionIds)
{
    ToProto(transactionIdsProto->mutable_async_id(), transactionIds.AsyncId);
    ToProto(transactionIdsProto->mutable_input_id(), transactionIds.InputId);
    ToProto(transactionIdsProto->mutable_output_id(), transactionIds.OutputId);
    ToProto(transactionIdsProto->mutable_debug_id(), transactionIds.DebugId);
    ToProto(transactionIdsProto->mutable_output_completion_id(), transactionIds.OutputCompletionId);
    ToProto(transactionIdsProto->mutable_debug_completion_id(), transactionIds.DebugCompletionId);
    ToProto(transactionIdsProto->mutable_input_ids(), transactionIds.InputIds);
}

void FromProto(TControllerTransactionIds* transactionIds, const NControllerAgent::NProto::TControllerTransactionIds& transactionIdsProto)
{
    transactionIds->AsyncId = FromProto<TTransactionId>(transactionIdsProto.async_id());
    transactionIds->InputId = FromProto<TTransactionId>(transactionIdsProto.input_id());
    transactionIds->OutputId = FromProto<TTransactionId>(transactionIdsProto.output_id());
    transactionIds->DebugId = FromProto<TTransactionId>(transactionIdsProto.debug_id());
    transactionIds->OutputCompletionId = FromProto<TTransactionId>(transactionIdsProto.output_completion_id());
    transactionIds->DebugCompletionId = FromProto<TTransactionId>(transactionIdsProto.debug_completion_id());
    transactionIds->InputIds = FromProto<std::vector<TRichTransactionId>>(transactionIdsProto.input_ids());
}

////////////////////////////////////////////////////////////////////////////////

TOperationTransactions AttachControllerTransactions(
    TAttachTransactionCallback attachTransaction,
    TControllerTransactionIds transactionIds)
{
    TOperationTransactions transactions;
    THashMap<TTransactionId, ITransactionPtr> transactionIdToTransaction;

    // NB(coteeq): This could've been a bare noexcept in callback signature,
    // but neither std::function, nor TCallback offer an noexcept'able overload.
    auto attachTransactionNoexcept = [&] (TTransactionId transactionId, const TString& name) {
        try {
            return attachTransaction(transactionId, name);
        } catch (const std::exception& ex) {
            YT_ABORT();
        }
    };

    auto attachIfNotAttached = [&] (TTransactionId transactionId, const TString& name) {
        auto it = transactionIdToTransaction.find(transactionId);
        if (it != transactionIdToTransaction.end()) {
            return it->second;
        }
        auto transaction = attachTransactionNoexcept(transactionId, name);
        YT_VERIFY(transactionIdToTransaction.emplace(transactionId, transaction).second);
        return transaction;
    };

    transactions.AsyncTransaction = attachIfNotAttached(
        transactionIds.AsyncId,
        "async");
    transactions.InputTransaction = attachIfNotAttached(
        transactionIds.InputId,
        "input");
    for (const auto& transaction : transactionIds.InputIds) {
        transactions.InputTransactions.push_back(attachIfNotAttached(transaction.Id, "input"));
    }
    transactions.OutputTransaction = attachIfNotAttached(
        transactionIds.OutputId,
        "output");
    transactions.OutputCompletionTransaction = attachIfNotAttached(
        transactionIds.OutputCompletionId,
        "output completion");
    transactions.DebugTransaction = attachIfNotAttached(
        transactionIds.DebugId,
        "debug");
    transactions.DebugCompletionTransaction = attachIfNotAttached(
        transactionIds.DebugCompletionId,
        "debug completion");

    transactions.ControllerTransactionIds = std::move(transactionIds);

    return transactions;
}

////////////////////////////////////////////////////////////////////////////////

void ToProto(
    NControllerAgent::NProto::TControllerTransactionIds* transactionIdsProto,
    const TOperationTransactions& transactions)
{
    ToProto(transactionIdsProto, transactions.ControllerTransactionIds);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler

size_t THash<NYT::NScheduler::TRichTransactionId>::operator()(const NYT::NScheduler::TRichTransactionId& transactionId) const
{
    size_t hash = 0;
    hash = CombineHashes(hash, THash<NYT::NTransactionClient::TTransactionId>()(transactionId.Id));
    hash = CombineHashes(hash, THash<NYT::NTransactionClient::TTransactionId>()(transactionId.ParentId));
    hash = CombineHashes(hash, THash<NYT::NScheduler::TClusterName>()(transactionId.Cluster));
    return hash;
}
