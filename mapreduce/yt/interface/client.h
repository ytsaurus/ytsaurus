#pragma once

#include "fwd.h"
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

template <class TDerived>
struct TTabletOptions
{
    using TSelf = TDerived;

    FLUENT_FIELD_OPTION(i64, FirstTabletIndex);
    FLUENT_FIELD_OPTION(i64, LastTabletIndex);
};

struct TMountTableOptions
    : public TTabletOptions<TMountTableOptions>
{
    FLUENT_FIELD_OPTION(TTabletCellId, CellId);
    FLUENT_FIELD_DEFAULT(bool, Freeze, false);
};

struct TUnmountTableOptions
    : public TTabletOptions<TUnmountTableOptions>
{
    FLUENT_FIELD_DEFAULT(bool, Force, false);
};

struct TRemountTableOptions
    : public TTabletOptions<TRemountTableOptions>
{ };

struct TReshardTableOptions
    : public TTabletOptions<TReshardTableOptions>
{ };

struct TAlterTableOptions
{
    using TSelf = TAlterTableOptions;

    FLUENT_FIELD_OPTION(TTableSchema, Schema);
    FLUENT_FIELD_OPTION(bool, Dynamic);
};

struct TLookupRowsOptions
{
    using TSelf = TLookupRowsOptions;

    FLUENT_FIELD_OPTION(TDuration, Timeout);
    FLUENT_FIELD_OPTION(TKeyColumns, Columns);
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

struct TCreateClientOptions
{
    using TSelf = TCreateClientOptions;

    FLUENT_FIELD(Stroka, Token);
    FLUENT_FIELD(Stroka, TokenPath);
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
};

IClientPtr CreateClient(
    const Stroka& serverName,
    const TCreateClientOptions& options = TCreateClientOptions());

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
