#pragma once

#include "fwd.h"

#include "client_method_options.h"
#include "batch_request.h"
#include "cypress.h"
#include "io.h"
#include "node.h"
#include "operation.h"

#include <util/datetime/base.h>
#include <util/generic/maybe.h>
#include <util/system/compiler.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

enum ELockMode : int
{
    LM_EXCLUSIVE,
    LM_SHARED,
    LM_SNAPSHOT
};

////////////////////////////////////////////////////////////////////////////////

class IClientBase
    : public TThrRefBase
    , public ICypressClient
    , public IIOClient
    , public IOperationClient
{
public:
    virtual Y_WARN_UNUSED_RESULT ITransactionPtr StartTransaction(
        const TStartTransactionOptions& options = TStartTransactionOptions()) = 0;

    virtual void AlterTable(
        const TYPath& path,
        const TAlterTableOptions& options = TAlterTableOptions()) = 0;
};

class ITransaction
    : virtual public IClientBase
{
public:
    virtual const TTransactionId& GetId() const = 0;

    virtual TLockId Lock(
        const TYPath& path,
        ELockMode mode,
        const TLockOptions& options = TLockOptions()) = 0;

    virtual void Commit() = 0;
    virtual void Abort() = 0;
};

class IClient
    : virtual public IClientBase
{
public:
    virtual Y_WARN_UNUSED_RESULT ITransactionPtr AttachTransaction(
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

    virtual void ReshardTable(
        const TYPath& path,
        const yvector<TKey>& pivotKeys,
        const TReshardTableOptions& options = TReshardTableOptions()) = 0;

    virtual void ReshardTable(
        const TYPath& path,
        i32 tabletCount,
        const TReshardTableOptions& options = TReshardTableOptions()) = 0;

    // TODO: move to transaction
    virtual void InsertRows(
        const TYPath& path,
        const TNode::TList& rows) = 0;

    // TODO: move to transaction
    virtual void DeleteRows(
        const TYPath& path,
        const TNode::TList& keys) = 0;

    virtual TNode::TList LookupRows(
        const TYPath& path,
        const TNode::TList& keys,
        const TLookupRowsOptions& options = TLookupRowsOptions()) = 0;

    virtual TNode::TList SelectRows(
        const Stroka& query,
        const TSelectRowsOptions& options = TSelectRowsOptions()) = 0;

    virtual ui64 GenerateTimestamp() = 0;

    // Execute several light requests in parallel.
    // It is undefined in which order these requests are executed.
    //
    // Single TBatchRequest instance may be executed only once
    // and cannot be modified (filled with additional requests) after execution.
    // Exception is thrown on attempt to modify executed batch request
    // or execute it again.
    //
    // https://wiki.yandex-team.ru/yt/userdoc/api/#executebatch18.4
    virtual void ExecuteBatch(
        const TBatchRequest& batchRequest,
        const TExecuteBatchOptions& executeBatch = TExecuteBatchOptions()) = 0;
};

IClientPtr CreateClient(
    const Stroka& serverName,
    const TCreateClientOptions& options = TCreateClientOptions());

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
