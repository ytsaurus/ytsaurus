#pragma once

#include <util/datetime/base.h>

#include "common.h"

namespace NYT {

// https://wiki.yandex-team.ru/yt/userdoc/api/#create
struct TCreateOptions
{
    using TSelf = TCreateOptions;

    FLUENT_FIELD_DEFAULT(bool, Recursive, false);
    FLUENT_FIELD_DEFAULT(bool, IgnoreExisting, false);
    FLUENT_FIELD_OPTION(TNode, Attributes);
};

// https://wiki.yandex-team.ru/yt/userdoc/api/#remove
struct TRemoveOptions
{
    using TSelf = TRemoveOptions;

    FLUENT_FIELD_DEFAULT(bool, Recursive, false);
    FLUENT_FIELD_DEFAULT(bool, Force, false);
};

// https://wiki.yandex-team.ru/yt/userdoc/api/#get
struct TGetOptions
{
    using TSelf = TGetOptions;

    FLUENT_FIELD_OPTION(TAttributeFilter, AttributeFilter);
    FLUENT_FIELD_OPTION(i64, MaxSize); // TODO: rename to limit
    FLUENT_FIELD_DEFAULT(bool, IgnoreOpaque, false); // TODO: remove ???
};

// https://wiki.yandex-team.ru/yt/userdoc/api/#list
struct TListOptions
{
    using TSelf = TListOptions;

    FLUENT_FIELD_OPTION(TAttributeFilter, AttributeFilter);
    FLUENT_FIELD_OPTION(i64, MaxSize); // TODO: rename to limit
};

// https://wiki.yandex-team.ru/yt/userdoc/api/#copy
struct TCopyOptions
{
    using TSelf = TCopyOptions;

    FLUENT_FIELD_DEFAULT(bool, Recursive, false);
    FLUENT_FIELD_DEFAULT(bool, Force, false);
    FLUENT_FIELD_DEFAULT(bool, PreserveAccount, false);
    FLUENT_FIELD_OPTION(bool, PreserveExpirationTime);
};

// https://wiki.yandex-team.ru/yt/userdoc/api/#move
struct TMoveOptions
{
    using TSelf = TMoveOptions;

    // Will create missing directories in destination path.
    FLUENT_FIELD_DEFAULT(bool, Recursive, false);

    // Allows to use existing node as destination, it will be overwritten.
    FLUENT_FIELD_DEFAULT(bool, Force, false);

    // Preserves account of source node.
    FLUENT_FIELD_DEFAULT(bool, PreserveAccount, false);

    // Preserve `expiration_time` attribute of existing node.
    // TODO: Make it FLUENT_FIELD_DEFAULT
    FLUENT_FIELD_OPTION(bool, PreserveExpirationTime);
};

// https://wiki.yandex-team.ru/yt/userdoc/api/#link
struct TLinkOptions
{
    using TSelf = TLinkOptions;

    FLUENT_FIELD_DEFAULT(bool, Recursive, false);
    FLUENT_FIELD_DEFAULT(bool, IgnoreExisting, false);
    FLUENT_FIELD_OPTION(TNode, Attributes);
};

// https://wiki.yandex-team.ru/yt/userdoc/api/#concatenate
struct TConcatenateOptions
{
    using TSelf = TConcatenateOptions;

    FLUENT_FIELD_DEFAULT(bool, Append, false);
};

template <class TDerived>
struct TIOOptions
{
    using TSelf = TDerived;

    FLUENT_FIELD_OPTION(TNode, Config);
};

struct TFileReaderOptions
    : public TIOOptions<TFileReaderOptions>
{
    FLUENT_FIELD_OPTION(i64, Offset);
    FLUENT_FIELD_OPTION(i64, Length);
};

struct TFileWriterOptions
    : public TIOOptions<TFileWriterOptions>
{ };

struct TTableReaderOptions
    : public TIOOptions<TTableReaderOptions>
{
    FLUENT_FIELD_DEFAULT(size_t, SizeLimit, 4 << 20);
};

struct TTableWriterOptions
    : public TIOOptions<TTableWriterOptions>
{ };

struct TStartTransactionOptions
{
    using TSelf = TStartTransactionOptions;

    FLUENT_FIELD_DEFAULT(bool, PingAncestors, false);
    FLUENT_FIELD_OPTION(TDuration, Timeout);
    FLUENT_FIELD_OPTION(TNode, Attributes);
};

// https://wiki.yandex-team.ru/yt/userdoc/api/#lock
struct TLockOptions
{
    using TSelf = TLockOptions;

    FLUENT_FIELD_DEFAULT(bool, Waitable, false);
    FLUENT_FIELD_OPTION(Stroka, AttributeKey);
    FLUENT_FIELD_OPTION(Stroka, ChildKey);
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

struct TExecuteBatchOptions
{
    using TSelf = TExecuteBatchOptions;

    // How may requests will be executed in parallel on the cluster.
    // This parameter could be used to avoid RequestLimitExceeded errors.
    FLUENT_FIELD_OPTION(ui64, Concurrency);
};

} // namespace NYT
