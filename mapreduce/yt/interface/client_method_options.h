#pragma once

#include "common.h"

#include <util/datetime/base.h>

namespace NYT {

enum ENodeType : int
{
    NT_STRING               /* "string_node" */,
    NT_INT64                /* "int64_node" */,
    NT_UINT64               /* "uint64_node" */,
    NT_DOUBLE               /* "double_node" */,
    NT_BOOLEAN              /* "boolean_node" */,
    NT_MAP                  /* "map_node" */,
    NT_LIST                 /* "list_node" */,
    NT_FILE                 /* "file" */,
    NT_TABLE                /* "table" */,
    NT_DOCUMENT             /* "document" */,
    NT_REPLICATED_TABLE     /* "replicated_table" */,
    NT_TABLE_REPLICA        /* "table_replica" */,
};

// https://wiki.yandex-team.ru/yt/userdoc/api/#create
struct TCreateOptions
{
    using TSelf = TCreateOptions;

    // Create missing parent directories if required.
    FLUENT_FIELD_DEFAULT(bool, Recursive, false);

    // Do not raise error if node exists already.
    // Node is not recreated.
    // Force and IgnoreExisting MUST NOT be used simultaneously.
    FLUENT_FIELD_DEFAULT(bool, IgnoreExisting, false);

    // Recreate node if it exists.
    // Force and IgnoreExisting MUST NOT be used simultaneously.
    FLUENT_FIELD_DEFAULT(bool, Force, false);

    // Set attributes when creating node.
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

    //
    // When false current content of result table is discared and replaced by result of concatenation.
    // When true result of concatenation is appended to current content of result table.
    FLUENT_FIELD_DEFAULT(bool, Append, false);
};

// https://wiki.yandex-team.ru/yt/userdoc/api/#readblobtable
struct TBlobTableReaderOptions
{
    using TSelf = TBlobTableReaderOptions;

    //
    // Name of the part index column. By default it is part_index.
    FLUENT_FIELD_OPTION(TString, PartIndexColumnName);

    //
    // Name of the `part index' column. By default it is part_index.
    FLUENT_FIELD_OPTION(TString, DataColumnName);

    //
    // Size of each part. All blob parts except the last part of the blob must be of this size
    // otherwise blob table reader emits error.
    FLUENT_FIELD_DEFAULT(ui64, PartSize, 4 * 1024 * 1024);
};


template <class TDerived>
struct TIOOptions
{
    using TSelf = TDerived;

    FLUENT_FIELD_OPTION(TNode, Config);

    // If `CreateTransaction` is set to `false`
    // reader/writer doesn't create internal transaction and doesn't lock table.
    // This option is overriden (effectively `false`) for writers by `SingleHttpRequest`.
    //
    // WARNING: if `CreateTransaction` is `false`, read/write might become non-atomic.
    // Change ONLY if you are sure what you are doing!
    FLUENT_FIELD_DEFAULT(bool, CreateTransaction, true);
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
{
    // If set to true no retry is made but we also make less requests to master.
    // If set to false writer can make up to `TConfig::RetryCount` attempts to send each block of data.
    //
    // NOTE: writers's methods might throw strange exceptions that might look like network error
    // when `SingleHttpRequest == true` and YT node encounters an error
    // (due to limitations of HTTP protocol YT node have no chance to report error
    // before it reads the whole input so it just drops the connection).
    FLUENT_FIELD_DEFAULT(bool, SingleHttpRequest, false);
};

// https://wiki.yandex-team.ru/yt/userdoc/api/#starttx
struct TStartTransactionOptions
{
    using TSelf = TStartTransactionOptions;

    FLUENT_FIELD_DEFAULT(bool, PingAncestors, false);
    FLUENT_FIELD_OPTION(TDuration, Timeout);

    // Set the title attribute of transaction. If title was not specified
    // nither using `Title` option nor using `Attributes` option
    // wrapper will generate default title for transaction.
    // Such default title includes machine name, pid, user name and some other useful info.
    FLUENT_FIELD_OPTION(TString, Title);

    // Set custom transaction attributes, NOTE: `Title` option overrides `title` attribute.
    FLUENT_FIELD_OPTION(TNode, Attributes);
};

enum ELockMode : int
{
    LM_EXCLUSIVE    /* "exclusive" */,
    LM_SHARED       /* "shared" */,
    LM_SNAPSHOT     /* "snapshot" */,
};

// https://wiki.yandex-team.ru/yt/userdoc/api/#lock
struct TLockOptions
{
    using TSelf = TLockOptions;

    // If `Waitable' is set to true Lock method will create
    // waitable lock, that will be taken once other transactions
    // that hold lock to that node are commited / aborted.
    //
    // NOTE: Lock method DOES NOT wait until lock is actually acquired.
    // Waiting should be done using corresponding methods of ILock.
    FLUENT_FIELD_DEFAULT(bool, Waitable, false);

    FLUENT_FIELD_OPTION(TString, AttributeKey);
    FLUENT_FIELD_OPTION(TString, ChildKey);
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

struct TFreezeTableOptions
    : public TTabletOptions<TFreezeTableOptions>
{ };

struct TUnfreezeTableOptions
    : public TTabletOptions<TFreezeTableOptions>
{ };

struct TAlterTableOptions
{
    using TSelf = TAlterTableOptions;

    // Change table schema.
    FLUENT_FIELD_OPTION(TTableSchema, Schema);

    // Alter table between static and dynamic mode.
    FLUENT_FIELD_OPTION(bool, Dynamic);

    // Changes id of upstream replica on metacluster.
    // https://wiki.yandex-team.ru/yt/userdoc/dynamicreplicatedtables/
    FLUENT_FIELD_OPTION(TReplicaId, UpstreamReplicaId);
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

    FLUENT_FIELD(TString, Token);
    FLUENT_FIELD(TString, TokenPath);
};

struct TExecuteBatchOptions
{
    using TSelf = TExecuteBatchOptions;

    // How may requests will be executed in parallel on the cluster.
    // This parameter could be used to avoid RequestLimitExceeded errors.
    FLUENT_FIELD_OPTION(ui64, Concurrency);

    // Huge batches are executed using multiple requests.
    // BatchPartMaxSize is maximum size of single request that goes to server
    // If not specified it is set to `Concurrency * 5'
    FLUENT_FIELD_OPTION(ui64, BatchPartMaxSize);
};

enum class EDurability
{
    Sync    /* "sync" */,
    Async   /* "async" */,
};

enum class EAtomicity
{
    None    /* "none" */,
    Full    /* "full" */,
};

enum class ETableReplicaMode
{
    Sync    /* "sync" */,
    Async   /* "async" */,
};

template <typename TDerived>
struct TTabletTransactionOptions
{
    using TSelf = TDerived;

    FLUENT_FIELD_OPTION(EAtomicity, Atomicity);
    FLUENT_FIELD_OPTION(EDurability, Durability);
};

// https://wiki.yandex-team.ru/yt/userdoc/api/#insertrows
struct TInsertRowsOptions
    : public TTabletTransactionOptions<TInsertRowsOptions>
{
    // By default all columns missing in input data are set to Null and overwrite currently stored value.
    // If `Update' is set to true currently stored value will not be overwritten for columns that are missing in input data.
    FLUENT_FIELD_OPTION(bool, Update);

    // Used with aggregating columns.
    // https://wiki.yandex-team.ru/yt/userdoc/dynamicsortedtables/#agregirujushhiekolonki
    // By default value in aggregating column will be overwritten.
    // If `Aggregate' is set to true row will be considered as delta and it will be aggregated with currently stored value.
    FLUENT_FIELD_OPTION(bool, Aggregate);

    //Used for insert operation for tables without sync replica
    //https://wiki.yandex-team.ru/yt/userdoc/dynamicreplicatedtables/#zapis
    //Default value is 'false'. So insertion into table without sync replias fails
    FLUENT_FIELD_OPTION(bool, RequireSyncReplica);
};

struct TDeleteRowsOptions
    : public TTabletTransactionOptions<TDeleteRowsOptions>
{
    //Used for delete operation for tables without sync replica
    //https://wiki.yandex-team.ru/yt/userdoc/dynamicreplicatedtables/#zapis
    //Default value is 'false'. So deletion into table without sync replias fails
    FLUENT_FIELD_OPTION(bool, RequireSyncReplica);
};

// https://wiki.yandex-team.ru/yt/userdoc/api/#altertablereplica
// https://wiki.yandex-team.ru/yt/userdoc/dynamicreplicatedtables/
struct TAlterTableReplicaOptions
{
    using TSelf = TAlterTableReplicaOptions;

    // Enable replica if `Enabled' is set to true.
    // Disable replica if `Enabled' is set to false.
    // Doesn't change state of replica if `Enabled' is not set.
    FLUENT_FIELD_OPTION(bool, Enabled);

    // If `Mode' is set replica mode is changed to specified value.
    // If `Mode' is not set replica mode is untouched.
    FLUENT_FIELD_OPTION(ETableReplicaMode, Mode);
};

} // namespace NYT
