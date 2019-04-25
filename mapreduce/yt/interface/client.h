#pragma once

#include "fwd.h"

#include "client_method_options.h"
#include "constants.h"
#include "batch_request.h"
#include "cypress.h"
#include "init.h"
#include "io.h"
#include "node.h"
#include "operation.h"

#include <library/threading/future/future.h>

#include <util/datetime/base.h>
#include <util/generic/maybe.h>
#include <util/system/compiler.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

struct TAuthorizationInfo
{
    TString Login;
    TString Realm;
};

////////////////////////////////////////////////////////////////////////////////

class ILock
    : public TThrRefBase
{
public:
    virtual ~ILock() = default;

    virtual const TLockId& GetId() const = 0;

    // Returns locked Cypress node id.
    virtual TNodeId GetLockedNodeId() const = 0;

    // Returns future that will be set once lock is in "acquired" state.
    //
    // Note that future might contain exception if some error occurred
    // e.g. lock transaction was aborted.
    virtual const NThreading::TFuture<void>& GetAcquiredFuture() const = 0;

    // Convenient wrapper that waits until lock is in "acquired" state.
    // Throws exception if timeout exceeded or some error occurred
    // e.g. lock transaction was aborted.
    void Wait(TDuration timeout = TDuration::Max());
};

////////////////////////////////////////////////////////////////////////////////

class IClientBase
    : public TThrRefBase
    , public ICypressClient
    , public IIOClient
    , public IOperationClient
{
public:
    [[nodiscard]] virtual ITransactionPtr StartTransaction(
        const TStartTransactionOptions& options = TStartTransactionOptions()) = 0;

    //
    // Change properties of table:
    //   - switch table between dynamic/static mode
    //   - or change table schema
    virtual void AlterTable(
        const TYPath& path,
        const TAlterTableOptions& options = TAlterTableOptions()) = 0;

    //
    // Create batch request object that allows to execute several light requests in parallel.
    // https://wiki.yandex-team.ru/yt/userdoc/api/#executebatch18.4
    virtual TBatchRequestPtr CreateBatchRequest() = 0;

    //
    // Return 'this' for IClient and the underlying client for ITransaction.
    virtual IClientPtr GetParentClient() = 0;
};

////////////////////////////////////////////////////////////////////////////////

class ITransaction
    : virtual public IClientBase
{
public:
    virtual const TTransactionId& GetId() const = 0;

    //
    // Try to lock given path.
    //
    // Lock will be held until transaction is commited/aborted or `Unlock` method is called.
    // Lock modes:
    //   LM_EXCLUSIVE: if exclusive lock is taken no other transaction can take exclusive or shared lock.
    //   LM_SHARED: if shared lock is taken other transactions can take shared lock but not exclusive.
    //
    //   LM_SNAPSHOT: snapshot lock always succeeds, when snapshot lock is taken current transaction snapshots object.
    //   It will not see changes that occured to it in other transactions.
    //
    // Exclusive/shared lock can be waitable or not.
    // If nonwaitable lock cannot be taken exception is thrown.
    // If waitable lock cannot be taken it is created in pending state and client can wait until it actually taken.
    // Check TLockOptions::Waitable and ILock::GetAcquiredFuture for more details.
    virtual ILockPtr Lock(
        const TYPath& path,
        ELockMode mode,
        const TLockOptions& options = TLockOptions()) = 0;

    //
    // Remove all the locks (including pending ones)
    // for this transaction from a Cypress node at `path`.
    //
    // If the locked version of the node differs from the original one,
    // an error will be thrown.
    //
    // Command is successful even if the node has no locks.
    // Only explicit (created by `Lock` method) locks are removed.
    virtual void Unlock(
        const TYPath& path,
        const TUnlockOptions& options = TUnlockOptions()) = 0;

    //
    // Commit transaction. All changes that are made by transactions become visible globaly or to parent transaction.
    virtual void Commit() = 0;

    //
    // Abort transaction. All changes that are made by current transaction are lost.
    virtual void Abort() = 0;

    //
    // Ping transaction.
    virtual void Ping() = 0;
};

////////////////////////////////////////////////////////////////////////////////

class IClient
    : virtual public IClientBase
{
public:
    //
    // Attach to existing transaction.
    //
    // Returned object WILL NOT ping transaction automatically.
    // Otherwise returened object is similar to the object returned by StartTransaction
    // and it can see all the changes made inside the transaction.
    [[nodiscard]] virtual ITransactionPtr AttachTransaction(
        const TTransactionId& transactionId) = 0;

    virtual void MountTable(
        const TYPath& path,
        const TMountTableOptions& options = TMountTableOptions()) = 0;

    virtual void UnmountTable(
        const TYPath& path,
        const TUnmountTableOptions& options = TUnmountTableOptions()) = 0;

    virtual void RemountTable(
        const TYPath& path,
        const TRemountTableOptions& options = TRemountTableOptions()) = 0;

    // Switch dynamic table from `mounted' into `frozen' state.
    // When table is in frozen state all its data is flushed to disk and writes are disabled.
    //
    // NOTE: this function launches the process of switching, but doesn't wait until switching is accomplished.
    // Waiting has to be performed by user.
    virtual void FreezeTable(
        const TYPath& path,
        const TFreezeTableOptions& options = TFreezeTableOptions()) = 0;

    // Switch dynamic table from `frozen' into `mounted' state.
    //
    // NOTE: this function launches the process of switching, but doesn't wait until switching is accomplished.
    // Waiting has to be performed by user.
    virtual void UnfreezeTable(
        const TYPath& path,
        const TUnfreezeTableOptions& options = TUnfreezeTableOptions()) = 0;

    virtual void ReshardTable(
        const TYPath& path,
        const TVector<TKey>& pivotKeys,
        const TReshardTableOptions& options = TReshardTableOptions()) = 0;

    virtual void ReshardTable(
        const TYPath& path,
        i64 tabletCount,
        const TReshardTableOptions& options = TReshardTableOptions()) = 0;

    virtual void InsertRows(
        const TYPath& path,
        const TNode::TListType& rows,
        const TInsertRowsOptions& options = TInsertRowsOptions()) = 0;

    virtual void DeleteRows(
        const TYPath& path,
        const TNode::TListType& keys,
        const TDeleteRowsOptions& options = TDeleteRowsOptions()) = 0;

    virtual void TrimRows(
        const TYPath& path,
        i64 tabletIndex,
        i64 rowCount,
        const TTrimRowsOptions& options = TTrimRowsOptions()) = 0;

    virtual TNode::TListType LookupRows(
        const TYPath& path,
        const TNode::TListType& keys,
        const TLookupRowsOptions& options = TLookupRowsOptions()) = 0;

    virtual TNode::TListType SelectRows(
        const TString& query,
        const TSelectRowsOptions& options = TSelectRowsOptions()) = 0;

    // Is not supported since YT 19.2 version
    virtual void EnableTableReplica(const TReplicaId& replicaid) = 0;

    // Is not supported since YT 19.2 version
    virtual void DisableTableReplica(const TReplicaId& replicaid) = 0;

    // Change properties of table replica.
    // Allows to enable/disable replica and/or change its mode.
    virtual void AlterTableReplica(
        const TReplicaId& replicaId,
        const TAlterTableReplicaOptions& alterTableReplicaOptions) = 0;

    virtual ui64 GenerateTimestamp() = 0;

    // Return YT username of current client.
    virtual TAuthorizationInfo WhoAmI() = 0;

    // Get operation attributes.
    virtual TOperationAttributes GetOperation(
        const TOperationId& operationId,
        const TGetOperationOptions& options = TGetOperationOptions()) = 0;

    // List operations satisfying given filters.
    virtual TListOperationsResult ListOperations(
        const TListOperationsOptions& options = TListOperationsOptions()) = 0;

    // Update operation runtime parameters.
    virtual void UpdateOperationParameters(
        const TOperationId& operationId,
        const TUpdateOperationParametersOptions& options) = 0;

    // Get job attributes.
    virtual TJobAttributes GetJob(
        const TOperationId& operationId,
        const TJobId& jobId,
        const TGetJobOptions& options = TGetJobOptions()) = 0;

    // List jobs satisfying given filters.
    virtual TListJobsResult ListJobs(
        const TOperationId& operationId,
        const TListJobsOptions& options = TListJobsOptions()) = 0;

    // Get the input of a running or failed job.
    // (TErrorResponse exception is thrown if it is missing).
    virtual IFileReaderPtr GetJobInput(
        const TJobId& jobId,
        const TGetJobInputOptions& options = TGetJobInputOptions()) = 0;

    // Get fail context of a failed job.
    // (TErrorResponse exception is thrown if it is missing).
    virtual IFileReaderPtr GetJobFailContext(
        const TOperationId& operationId,
        const TJobId& jobId,
        const TGetJobFailContextOptions& options = TGetJobFailContextOptions()) = 0;

    // Get stderr of a running or failed job
    // (TErrorResponse exception is thrown if it is missing).
    virtual IFileReaderPtr GetJobStderr(
        const TOperationId& operationId,
        const TJobId& jobId,
        const TGetJobStderrOptions& options = TGetJobStderrOptions()) = 0;

    // Get a file with given md5 from Cypress file cache located at 'cachePath'.
    virtual TMaybe<TYPath> GetFileFromCache(
        const TString& md5Signature,
        const TYPath& cachePath,
        const TGetFileFromCacheOptions& options = TGetFileFromCacheOptions()) = 0;

    // Put a file 'filePath' to Cypress file cache located at 'cachePath'.
    // The file must have "md5" attribute and 'md5Signature' must match its value.
    virtual TYPath PutFileToCache(
        const TYPath& filePath,
        const TString& md5Signature,
        const TYPath& cachePath,
        const TPutFileToCacheOptions& options = TPutFileToCacheOptions()) = 0;
};

IClientPtr CreateClient(
    const TString& serverName,
    const TCreateClientOptions& options = TCreateClientOptions());

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
