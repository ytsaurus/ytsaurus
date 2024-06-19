#pragma once

#include <yt/yt/server/controller_agent/public.h>

#include <yt/yt/server/lib/scheduler/transactions.h>

#include <yt/yt/ytlib/api/native/public.h>

#include <yt/yt/client/transaction_client/public.h>

#include <yt/yt/client/ypath/rich.h>

#include <vector>

namespace NYT::NControllerAgent::NControllers {

////////////////////////////////////////////////////////////////////////////////

struct TInputClients
{
    NApi::NNative::IClientPtr Client;
    NApi::NNative::IClientPtr SchedulerClient;
};

////////////////////////////////////////////////////////////////////////////////

class TInputTransactionsManager
    : public TRefCounted
{
public:
    TInputTransactionsManager(
        TInputClients clients,
        NScheduler::TOperationId operationId,
        const std::vector<NYPath::TRichYPath>& inputTablesAndFiles,
        // NB(coteeq): Operation may require input transaction even if it is not
        //             required by input tables or user files, for example this
        //             may happen if all the inputs are taken from different
        //             transactions, but there is a request for disk space
        //             for a job.
        bool forceStartNativeTransaction,
        NTransactionClient::TTransactionId userTransactionId,
        TControllerAgentConfigPtr config,
        NLogging::TLogger logger);

    TFuture<void> Start(NYTree::IAttributeDictionaryPtr transactionAttributes);
    TFuture<void> Revive(NScheduler::TControllerTransactionIds transactionIds);
    TFuture<void> Abort();

    NTransactionClient::TTransactionId GetTransactionIdForObject(
        const NYPath::TRichYPath& path) const;

    void FillSchedulerTransactionIds(
        NScheduler::TControllerTransactionIds* transactionIds) const;

    NTransactionClient::TTransactionId GetNativeInputTransactionId() const;

private:
    const NScheduler::TOperationId OperationId_;
    NApi::NNative::IClientPtr Client_;
    NApi::NNative::IClientPtr SchedulerClient_;

    std::map<NTransactionClient::TTransactionId, NApi::ITransactionPtr> ParentToTransaction_;
    // COMPAT(coteeq)
    std::vector<NTransactionClient::TTransactionId> OldNonTrivialInputTransactionParents_;

    NLogging::TLogger Logger;
    TControllerAgentConfigPtr ControllerConfig_;
    NTransactionClient::TTransactionId UserTransactionId_;

    // Points to child of UserTransaction, if such transaction exists
    // among #ParentToTransaction_'s values, null otherwise.
    NApi::ITransactionPtr NativeInputTransaction_;

    NTransactionClient::TTransactionId GetTransactionParentFromPath(
        const NYPath::TRichYPath& path) const;

    std::vector<NScheduler::TRichTransactionId> RestoreFromNestedTransactions(
        const NScheduler::TControllerTransactionIds& transactionIds) const;

    std::vector<NTransactionClient::TTransactionId> GetCompatDuplicatedNestedTransactionIds() const;

    TError ValidateSchedulerTransactions(
        const NScheduler::TControllerTransactionIds& transactionIds) const;
};

DEFINE_REFCOUNTED_TYPE(TInputTransactionsManager)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NControllerAgent::NControllers
