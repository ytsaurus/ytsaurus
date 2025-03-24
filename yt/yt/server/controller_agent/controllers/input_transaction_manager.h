#pragma once

#include "private.h"

#include <yt/yt/server/controller_agent/public.h>

#include <yt/yt/server/lib/scheduler/transactions.h>

#include <yt/yt/ytlib/api/native/public.h>

#include <yt/yt/ytlib/chunk_client/helpers.h>

#include <yt/yt/ytlib/controller_agent/persistence.h>

#include <yt/yt/ytlib/hive/public.h>

#include <yt/yt/ytlib/scheduler/cluster_name.h>

#include <yt/yt/client/transaction_client/public.h>

#include <yt/yt/client/ypath/rich.h>

#include <vector>

namespace NYT::NControllerAgent::NControllers {

////////////////////////////////////////////////////////////////////////////////

/// Some notes about clusters and their names.
///
/// There are multiple ways of specifying a cluster for a table:
///   1. The `<cluster="my_cluster">` attribute on table's RichYPath
///   2. The `cluster_name="my_cluster"` option in remote_copy's spec
///   3. The `cluster_connection=*giant_yson_document*` option in remote_copy's spec
///   4. The operation's host cluster
///
/// The `cluster_connection` option is very annoying, because it does
/// not provide us a meaningful cluster name. For that single reason,
/// let's just omit (or pass an empty string) cluster name in
/// situations, where it is not explicitly defined - 3rd and 4th rows in
/// the list above. Thankfully, we can easily get client to such cluster
/// either via Host's connection or via the connection config passed as
/// `cluster_connection`.
///
class TClusterResolver
    : public TRefCounted
{
public:
    TClusterResolver() = default;
    explicit TClusterResolver(const NApi::NNative::IClientPtr& client);

    NScheduler::TClusterName GetClusterName(const NYPath::TRichYPath& path);
    const std::string& GetLocalClusterName() const;

    void Persist(const TPersistenceContext& context);

private:
    std::string LocalClusterName_;

    bool IsLocalClusterName(const std::string& name) const;
};

DEFINE_REFCOUNTED_TYPE(TClusterResolver)

////////////////////////////////////////////////////////////////////////////////

class TInputTransactionManager
    : public TRefCounted
{
public:
    TInputTransactionManager(
        NApi::NNative::IClientPtr client,
        TClusterResolverPtr clusterResolver,
        NScheduler::TOperationId operationId,
        const std::vector<NYPath::TRichYPath>& inputTablesAndFiles,
        // NB(coteeq): Operation may require input transaction even if it is not
        //             required by input tables or user files, for example this
        //             may happen if all the inputs are taken from different
        //             transactions, but there is a request for disk space
        //             for a job.
        bool forceStartLocalTransaction,
        NTransactionClient::TTransactionId userTransactionId,
        TControllerAgentConfigPtr config,
        NLogging::TLogger logger);

    TFuture<void> Start(NYTree::IAttributeDictionaryPtr transactionAttributes);
    TFuture<void> Revive(NScheduler::TControllerTransactionIds transactionIds);
    TFuture<void> Abort(NApi::NNative::IClientPtr schedulerClient);

    NTransactionClient::TTransactionId GetTransactionIdForObject(
        const NYPath::TRichYPath& path) const;

    void FillSchedulerTransactionIds(
        NScheduler::TControllerTransactionIds* transactionIds) const;

    NTransactionClient::TTransactionId GetLocalInputTransactionId() const;

    TClusterResolverPtr GetClusterResolver() const;

private:
    const NScheduler::TOperationId OperationId_;

    THashMap<NScheduler::TClusterName, NApi::NNative::IClientPtr> Clients_;

    std::map<NScheduler::TRichTransactionId, NApi::ITransactionPtr> ParentToTransaction_;

    NLogging::TLogger Logger; // NOLINT
    TControllerAgentConfigPtr ControllerConfig_;
    NTransactionClient::TTransactionId UserTransactionId_;
    TClusterResolverPtr ClusterResolver_;

    // Points to child of UserTransaction, if such transaction exists
    // among #ParentToTransaction_'s values, null otherwise.
    NApi::ITransactionPtr LocalInputTransaction_;

    NScheduler::TRichTransactionId GetTransactionParentFromPath(
        const NYPath::TRichYPath& path) const;

    TError ValidateSchedulerTransactions(
        const NScheduler::TControllerTransactionIds& transactionIds) const;

    void ValidateRemoteOperationsAllowed(
        const NScheduler::TClusterName& clusterName,
        const std::string& authenticatedUser,
        const NYPath::TRichYPath& path) const;
};

DEFINE_REFCOUNTED_TYPE(TInputTransactionManager)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NControllerAgent::NControllers
