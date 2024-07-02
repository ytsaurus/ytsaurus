#pragma once

#include <yt/yt/client/api/public.h>

#include <yt/yt/ytlib/api/native/public.h>

#include <yt/yt/ytlib/controller_agent/proto/controller_agent_service.pb.h>

#include <yt/yt/ytlib/scheduler/cluster_name.h>

#include <functional>

namespace NYT::NScheduler {

////////////////////////////////////////////////////////////////////////////////

struct TRichTransactionId
{
    NTransactionClient::TTransactionId Id;
    NTransactionClient::TTransactionId ParentId;

    TClusterName Cluster;

    std::strong_ordering operator <=>(const TRichTransactionId& other) const = default;
};

TRichTransactionId MakeRichTransactionId(NTransactionClient::TTransactionId id);

void FormatValue(TStringBuilderBase* builder, const TRichTransactionId& transactionId, TStringBuf spec);

void Deserialize(TRichTransactionId& transaction, const NYTree::INodePtr& node);
void Serialize(const TRichTransactionId& transaction, NYson::IYsonConsumer* consumer);

void ToProto(NProto::TRichTransactionId* transactionIdProto, const TRichTransactionId& transactionId);
void FromProto(TRichTransactionId* transactionId, const NProto::TRichTransactionId& transactionIdProto);

////////////////////////////////////////////////////////////////////////////////

struct TControllerTransactionIds
{
    NTransactionClient::TTransactionId AsyncId;
    NTransactionClient::TTransactionId InputId;
    NTransactionClient::TTransactionId OutputId;
    NTransactionClient::TTransactionId DebugId;
    NTransactionClient::TTransactionId OutputCompletionId;
    NTransactionClient::TTransactionId DebugCompletionId;
    std::vector<NTransactionClient::TTransactionId> NestedInputIds;
    std::vector<TRichTransactionId> InputIds;

    static const std::vector<TString> AttributeKeys;

    NYTree::IAttributeDictionaryPtr ToCypressAttributes() const;
    static TControllerTransactionIds FromCypressAttributes(NYTree::IAttributeDictionaryPtr attributes);
};

void ToProto(NControllerAgent::NProto::TControllerTransactionIds* transactionIdsProto, const TControllerTransactionIds& transactionIds);
void FromProto(TControllerTransactionIds* transactionIds, const NControllerAgent::NProto::TControllerTransactionIds& transactionIdsProto);

////////////////////////////////////////////////////////////////////////////////

// NB: Keep sync with TControllerTransactionIds.
struct TOperationTransactions
{
    NApi::ITransactionPtr AsyncTransaction;
    NApi::ITransactionPtr InputTransaction;
    NApi::ITransactionPtr OutputTransaction;
    NApi::ITransactionPtr DebugTransaction;
    NApi::ITransactionPtr OutputCompletionTransaction;
    NApi::ITransactionPtr DebugCompletionTransaction;
    std::vector<NApi::ITransactionPtr> NestedInputTransactions;
    std::vector<NApi::ITransactionPtr> InputTransactions;

    TControllerTransactionIds ControllerTransactionIds;
};

using TAttachTransactionFn = std::function<NApi::ITransactionPtr(NTransactionClient::TTransactionId, TString)>;

TOperationTransactions AttachControllerTransactions(
    TAttachTransactionFn attachTransaction,
    TControllerTransactionIds&& transactionIds);

void ToProto(
    NControllerAgent::NProto::TControllerTransactionIds* transactionIdsProto,
    const TOperationTransactions& transactions);

void FromProto(
    TOperationTransactions* transactions,
    const NControllerAgent::NProto::TControllerTransactionIds& transactionIdsProto,
    std::function<NApi::NNative::IClientPtr(NObjectClient::TCellTag)> getClient,
    TDuration pingPeriod);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler
