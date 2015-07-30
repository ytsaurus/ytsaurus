#pragma once

#include "cypress.h"
#include "io.h"
#include "operation.h"
#include "node.h"

#include <util/datetime/base.h>
#include <util/generic/maybe.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

enum ELockMode
{
    LM_EXCLUSIVE,
    LM_SHARED,
    LM_SNAPSHOT
};

struct TStartTransactionOptions
{
    using TSelf = TStartTransactionOptions;

    FLUENT_FIELD_DEFAULT(bool, PingAncestors, false);
    FLUENT_FIELD_OPTION(TDuration, Timeout);
    FLUENT_FIELD_OPTION(TNode, Attributes);
};

struct TLockOptions
{
    using TSelf = TLockOptions;

    FLUENT_FIELD_DEFAULT(bool, Waitable, false);
};

struct TLookupRowsOptions
{
    using TSelf = TLookupRowsOptions;

    FLUENT_FIELD_OPTION(TDuration, Timeout);
    // ColumnFilter
    FLUENT_FIELD_DEFAULT(bool, KeepMissingRows, false);
};

struct TSelectRowsOptions
{
    using TSelf = TSelectRowsOptions;

    FLUENT_FIELD_OPTION(TDuration, Timeout);
    FLUENT_FIELD_OPTION(i64, InputRowLimit);
    FLUENT_FIELD_OPTION(i64, OutputRowLimit);
    FLUENT_FIELD_DEFAULT(ui64, RangeExpansionLimit, 1000);
    FLUENT_FIELD_DEFAULT(bool, FailOnIncompleteResult, true);
    FLUENT_FIELD_DEFAULT(bool, VerboseLogging, false);
    FLUENT_FIELD_DEFAULT(bool, EnableCodeCache, true);
};

////////////////////////////////////////////////////////////////////////////////

class ITransaction;
using ITransactionPtr = TIntrusivePtr<ITransaction>;

class IClientBase
    : public TThrRefBase
    , public ICypressClient
    , public IIOClient
    , public IOperationClient
{
public:
    virtual ITransactionPtr StartTransaction(
        const TStartTransactionOptions& options = TStartTransactionOptions()) = 0;
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
};

using IClientPtr = TIntrusivePtr<IClient>;

IClientPtr CreateClient(const Stroka& serverName);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
