#pragma once

#include "common.h"
#include "format.h"
#include "retry_policy.h"

#include <util/datetime/base.h>

namespace NYT {

/// Type of the cypress node.
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
    NT_USER                 /* "user" */,
    NT_SCHEDULER_POOL       /* "scheduler_pool" */,
};

///
/// @brief Options for @ref NYT::ICypressClient::Create
///
/// @see https://yt.yandex-team.ru/docs/api/commands.html#create
struct TCreateOptions
{
    using TSelf = TCreateOptions;

    /// Create missing parent directories if required.
    FLUENT_FIELD_DEFAULT(bool, Recursive, false);

    ///
    /// @brief Do not raise error if node already exists.
    ///
    /// Node is not recreated.
    /// Force and IgnoreExisting MUST NOT be used simultaneously.
    FLUENT_FIELD_DEFAULT(bool, IgnoreExisting, false);

    ///
    /// @brief Recreate node if it exists.
    ///
    /// Force and IgnoreExisting MUST NOT be used simultaneously.
    FLUENT_FIELD_DEFAULT(bool, Force, false);

    /// @brief Set node attributes.
    FLUENT_FIELD_OPTION(TNode, Attributes);
};

///
/// @brief Options for @ref NYT::ICypressClient::Remove
///
/// @see https://yt.yandex-team.ru/docs/api/commands.html#remove
struct TRemoveOptions
{
    using TSelf = TRemoveOptions;

    ///
    /// @brief Remove whole tree when removing composite cypress node (e.g. `map_node`).
    ///
    /// Without this option removing nonempty composite node will fail.
    FLUENT_FIELD_DEFAULT(bool, Recursive, false);

    /// @brief Do not fail if removing node doesn't exist.
    FLUENT_FIELD_DEFAULT(bool, Force, false);
};

///
/// @brief Options for @ref NYT::ICypressClient::Get
///
/// @see https://yt.yandex-team.ru/docs/api/commands.html#get
struct TGetOptions
{
    using TSelf = TGetOptions;

    /// Attributes that should be fetched with each node.
    FLUENT_FIELD_OPTION(TAttributeFilter, AttributeFilter);

    /// @brief Limit for the number of children node.
    FLUENT_FIELD_OPTION(i64, MaxSize);
};

///
/// @brief Options for @ref NYT::ICypressClient::Set
///
/// @see https://yt.yandex-team.ru/docs/api/commands.html#set
struct TSetOptions
{
    using TSelf = TSetOptions;

    /// Create missing parent directories if required.
    FLUENT_FIELD_DEFAULT(bool, Recursive, false);

    /// Allow setting any nodes, not only attribute and document ones.
    FLUENT_FIELD_OPTION(bool, Force);
};

///
/// @brief Options for @ref NYT::ICypressClient::MultisetAttributes
///
/// @see https://yt.yandex-team.ru/docs/api/commands.html#multiset_attributes
struct TMultisetAttributesOptions
{ };

///
/// @brief Options for @ref NYT::ICypressClient::List
///
/// @see https://yt.yandex-team.ru/docs/api/commands.html#list
struct TListOptions
{
    using TSelf = TListOptions;

    /// Attributes that should be fetched for each node.
    FLUENT_FIELD_OPTION(TAttributeFilter, AttributeFilter);

    /// Limit for the number of children that will be fetched.
    FLUENT_FIELD_OPTION(i64, MaxSize);
};

///
/// @brief Options for @ref NYT::ICypressClient::Copy
///
/// @see https://yt.yandex-team.ru/docs/api/commands.html#copy
struct TCopyOptions
{
    using TSelf = TCopyOptions;

    /// Create missing directories in destination path if required.
    FLUENT_FIELD_DEFAULT(bool, Recursive, false);

    /// Allows to use existing node as destination, it will be overwritten.
    FLUENT_FIELD_DEFAULT(bool, Force, false);

    /// Wether to preserves account of source node.
    FLUENT_FIELD_DEFAULT(bool, PreserveAccount, false);

    /// Wether to preserve `expiration_time` attribute of source node.
    FLUENT_FIELD_OPTION(bool, PreserveExpirationTime);
};

///
/// @brief Options for @ref NYT::ICypressClient::Move
///
/// @see https://yt.yandex-team.ru/docs/api/commands.html#move
struct TMoveOptions
{
    using TSelf = TMoveOptions;

    /// Create missing directories in destination path if required.
    FLUENT_FIELD_DEFAULT(bool, Recursive, false);

    /// Allows to use existing node as destination, it will be overwritten.
    FLUENT_FIELD_DEFAULT(bool, Force, false);

    /// Wether to preserves account of source node.
    FLUENT_FIELD_DEFAULT(bool, PreserveAccount, false);

    /// Wether to preserve `expiration_time` attribute of source node.
    FLUENT_FIELD_OPTION(bool, PreserveExpirationTime);
};

///
/// @brief Options for @ref NYT::ICypressClient::Link
///
/// @see https://yt.yandex-team.ru/docs/api/commands.html#link
struct TLinkOptions
{
    using TSelf = TLinkOptions;

    FLUENT_FIELD_DEFAULT(bool, Recursive, false);
    FLUENT_FIELD_DEFAULT(bool, IgnoreExisting, false);
    FLUENT_FIELD_DEFAULT(bool, Force, false);
    FLUENT_FIELD_OPTION(TNode, Attributes);
};

///
/// @brief Options for @ref NYT::ICypressClient::Concatenate
///
/// @see https://yt.yandex-team.ru/docs/api/commands.html#concatenate
struct TConcatenateOptions
{
    using TSelf = TConcatenateOptions;

    /// Whether we should append to destination or rewrite it.
    FLUENT_FIELD_OPTION(bool, Append);
};

/// https://yt.yandex-team.ru/docs/api/commands.html#read_blob_table
struct TBlobTableReaderOptions
{
    using TSelf = TBlobTableReaderOptions;

    /// Name of the part index column. By default it is part_index.
    FLUENT_FIELD_OPTION(TString, PartIndexColumnName);

    /// Name of the `part index' column. By default it is part_index.
    FLUENT_FIELD_OPTION(TString, DataColumnName);

    ///
    /// @brief Size of each part.
    ///
    /// All blob parts except the last part of the blob must be of this size
    /// otherwise blob table reader emits error.
    FLUENT_FIELD_DEFAULT(ui64, PartSize, 4 * 1024 * 1024);

    /// @brief Offset from which to start reading
    FLUENT_FIELD_DEFAULT(i64, Offset, 0);
};

// https://yt.yandex-team.ru/docs/description/mr/scheduler_and_pools.html#resursy
struct TResourceLimits
{
    using TSelf = TResourceLimits;

    // Number of slots for user jobs.
    FLUENT_FIELD_OPTION(i64, UserSlots);

    // Number of cpu cores.
    FLUENT_FIELD_OPTION(double, Cpu);

    // Network usage in some units.
    FLUENT_FIELD_OPTION(i64, Network);

    // Memory in bytes.
    FLUENT_FIELD_OPTION(i64, Memory);
};

struct TSchedulingOptions
{
    using TSelf = TSchedulingOptions;

    // Pool to switch operation to.
    // NOTE: switching is currently disabled on the server (will induce an exception).
    FLUENT_FIELD_OPTION(TString, Pool);

    // Operation weight.
    FLUENT_FIELD_OPTION(double, Weight);

    // Operation resource limits.
    FLUENT_FIELD_OPTION(TResourceLimits, ResourceLimits);
};

struct TSchedulingOptionsPerPoolTree
{
    using TSelf = TSchedulingOptionsPerPoolTree;

    TSchedulingOptionsPerPoolTree(const THashMap<TString, TSchedulingOptions>& options = {})
        : Options_(options)
    { }

    TSelf& Add(TStringBuf poolTreeName, const TSchedulingOptions& schedulingOptions)
    {
        Y_ENSURE(Options_.emplace(poolTreeName, schedulingOptions).second);
        return *this;
    }

    THashMap<TString, TSchedulingOptions> Options_;
};

// https://yt.yandex-team.ru/docs/api/commands.html#suspend_op
struct TSuspendOperationOptions
{
    using TSelf = TSuspendOperationOptions;

    FLUENT_FIELD_OPTION(bool, AbortRunningJobs);
};

// https://yt.yandex-team.ru/docs/api/commands.html#resume_op
struct TResumeOperationOptions
{
    using TSelf = TResumeOperationOptions;
};

// https://yt.yandex-team.ru/docs/api/commands.html#update_op_parameters
struct TUpdateOperationParametersOptions
{
    using TSelf = TUpdateOperationParametersOptions;

    // New owners of the operation.
    FLUENT_VECTOR_FIELD(TString, Owner);

    // Pool to switch operation to (for all pool trees it is running in).
    FLUENT_FIELD_OPTION(TString, Pool);

    // New operation weight (for all pool trees it is running in).
    FLUENT_FIELD_OPTION(double, Weight);

    // Scheduling options for each pool tree the operation is running in.
    FLUENT_FIELD_OPTION(TSchedulingOptionsPerPoolTree, SchedulingOptionsPerPoolTree);
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

struct TWriterOptions
{
    using TSelf = TWriterOptions;

    /// When set to true upload will be considered successful as soon as
    /// MinUploadReplicationFactor number of replicas are created.
    FLUENT_FIELD_OPTION(bool, EnableEarlyFinish);

    /// Number of replicas to be created.
    FLUENT_FIELD_OPTION(ui64, UploadReplicationFactor);

    /// Min number of created replicas needed to consider upload successful.
    FLUENT_FIELD_OPTION(ui64, MinUploadReplicationFactor);
    
    ///
    /// @brief Desired size of a chunk.
    ///
    /// @see @ref NYT::TWriterOptions::RetryBlockSize
    FLUENT_FIELD_OPTION(ui64, DesiredChunkSize);
    
    ///
    /// @brief Size of data block accumulated in memory to provide retries.
    ///
    /// Data is accumulated in memory buffer so in case error occurs data could be resended.
    /// 
    /// If `RetryBlockSize` is not set buffer size is set to `DesiredChunkSize`.
    /// If niether `RetryBlockSize` nor `DesiredChunkSize` is set size of buffer is 64MB.
    ///
    /// NOTE: written chunks cannot be greater than size of this memory buffer.
    ///
    /// Since DesiredChunkSize is compared against data already compressed with compression codec
    /// it makes sense to set `RetryBlockSize = DesiredChunkSize / ExpectedCompressionRatio`
    /// 
    /// @see @ref NYT::TWriterOptions::DesiredChunkSize
    /// @see @ref NYT::TWriterOptions::SingleHttpRequest
    FLUENT_FIELD_OPTION(size_t, RetryBlockSize);
};

struct TFileWriterOptions
    : public TIOOptions<TFileWriterOptions>
{
    // Compute MD5 sum of written file. If `ComputeMD5 == true` and we are appending to an existing file
    // the `md5` attribute must be set (i.e. it was previously written only with `ComputeMD5 == true`).
    FLUENT_FIELD_OPTION(bool, ComputeMD5);

    FLUENT_FIELD_OPTION(TWriterOptions, WriterOptions);
};

class TFormatHints
{
public:
    using TSelf = TFormatHints;

    // When set to true TNode doesn't contain null column values.
    //
    // Only meaningful for TNode representation.
    //
    // Useful for sparse tables which have many columns in schema
    // but only few columns are set in any row.
    FLUENT_FIELD_DEFAULT(bool, SkipNullValuesForTNode, false);

    // Enable type conversions when writing data to static tables
    // or inserting/looking up/deleting data from dynamic tables.
    //
    // Convert string to numeric and boolean types (e.g. "42u" -> 42u, "false" -> %false).
    FLUENT_FIELD_OPTION(bool, EnableStringToAllConversion);

    // Convert numeric and boolean types to string (e.g., 3.14 -> "3.14", %true -> "true").
    FLUENT_FIELD_OPTION(bool, EnableAllToStringConversion);

    // Convert uint64 <-> int64. On overflow the corresponding error with be raised.
    // NOTE: this options is enabled by default.
    FLUENT_FIELD_OPTION(bool, EnableIntegralTypeConversion);

    // Convert uint64 and int64 to double (e.g. 42 -> 42.0).
    FLUENT_FIELD_OPTION(bool, EnableIntegralToDoubleConversion);

    // Shortcut for enabling all type conversions.
    FLUENT_FIELD_OPTION(bool, EnableTypeConversion);

    /// @brief Apply the patch to the fields.
    ///
    /// Non-default and non-empty values replace the default and empty ones.
    void Merge(const TFormatHints& patch);
};

struct TTableReaderOptions
    : public TIOOptions<TTableReaderOptions>
{
    FLUENT_FIELD_DEFAULT(size_t, SizeLimit, 4 << 20);

    // Allows to fine tune format that is used for reading tables.
    //
    // Has no effect when used with raw-reader.
    FLUENT_FIELD_OPTION(TFormatHints, FormatHints);
};


struct TTableWriterOptions
    : public TIOOptions<TTableWriterOptions>
{
    ///
    /// @brief Enable or disable retryful writing.
    ///
    /// If set to true no retry is made but we also make less requests to master.
    /// If set to false writer can make up to `TConfig::RetryCount` attempts to send each block of data.
    ///
    /// NOTE: writers' methods might throw strange exceptions that might look like network error
    /// when `SingleHttpRequest == true` and YT node encounters an error
    /// (due to limitations of HTTP protocol YT node have no chance to report error
    /// before it reads the whole input so it just drops the connection).
    FLUENT_FIELD_DEFAULT(bool, SingleHttpRequest, false);

    // Allows to change the size of locally buffered rows before flushing to yt
    // Measured in bytes
    // Default value is 64Mb
    FLUENT_FIELD_DEFAULT(size_t, BufferSize, 64 << 20);

    // Allows to fine tune format that is used for writing tables.
    //
    // Has no effect when used with raw-writer.
    FLUENT_FIELD_OPTION(TFormatHints, FormatHints);

    // Try to infer schema of inexistent table from the type of written rows.
    //
    // NOTE: Default values for this option may differ depending on the row type.
    // For protobuf it's currently false by default.
    FLUENT_FIELD_OPTION(bool, InferSchema);

    FLUENT_FIELD_OPTION(TWriterOptions, WriterOptions);
};

// https://yt.yandex-team.ru/docs/api/commands.html#start_tx
struct TStartTransactionOptions
{
    using TSelf = TStartTransactionOptions;

    FLUENT_FIELD_DEFAULT(bool, PingAncestors, false);
    FLUENT_FIELD_OPTION(TDuration, Timeout);
    FLUENT_FIELD_OPTION(TInstant, Deadline);

    // When set to true library creates a thread that pings transaction.
    // When set to false library doesn't ping transaction and it's user responsibility to ping it.
    FLUENT_FIELD_DEFAULT(bool, AutoPingable, true);

    // Set the title attribute of transaction. If title was not specified
    // nither using `Title` option nor using `Attributes` option
    // wrapper will generate default title for transaction.
    // Such default title includes machine name, pid, user name and some other useful info.
    FLUENT_FIELD_OPTION(TString, Title);

    // Set custom transaction attributes, NOTE: `Title` option overrides `title` attribute.
    FLUENT_FIELD_OPTION(TNode, Attributes);
};

struct TAttachTransactionOptions
{
    using TSelf = TAttachTransactionOptions;

    ///
    /// @brief Ping transaction automatically.
    ///
    /// When set to |true| library creates a thread that pings transaction.
    /// When set to |false| library doesn't ping transaction and
    /// it's user responsibility to ping it.
    FLUENT_FIELD_DEFAULT(bool, AutoPingable, false);

    ///
    /// @brief Abort transaction on program termination.
    ///
    /// Should the transaction be aborted on program termination
    /// (either normal or by a signal or uncaught exception -- two latter
    /// only if @ref TInitializeOptions::CleanupOnTermination is set).
    FLUENT_FIELD_DEFAULT(bool, AbortOnTermination, false);
};

enum ELockMode : int
{
    LM_EXCLUSIVE    /* "exclusive" */,
    LM_SHARED       /* "shared" */,
    LM_SNAPSHOT     /* "snapshot" */,
};

// https://yt.yandex-team.ru/docs/api/commands.html#lock
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

// https://yt.yandex-team.ru/docs/api/commands.html#unlock
struct TUnlockOptions
{
    using TSelf = TUnlockOptions;
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
    : public TTabletOptions<TUnfreezeTableOptions>
{ };

struct TAlterTableOptions
{
    using TSelf = TAlterTableOptions;

    // Change table schema.
    FLUENT_FIELD_OPTION(TTableSchema, Schema);

    // Alter table between static and dynamic mode.
    FLUENT_FIELD_OPTION(bool, Dynamic);

    // Changes id of upstream replica on metacluster.
    // https://yt.yandex-team.ru/docs/description/dynamic_tables/replicated_dynamic_tables
    FLUENT_FIELD_OPTION(TReplicaId, UpstreamReplicaId);
};

struct TLookupRowsOptions
{
    using TSelf = TLookupRowsOptions;

    FLUENT_FIELD_OPTION(TDuration, Timeout);
    FLUENT_FIELD_OPTION(TKeyColumns, Columns);
    FLUENT_FIELD_DEFAULT(bool, KeepMissingRows, false);
    FLUENT_FIELD_OPTION(bool, Versioned);
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

    // RetryConfig provider allows to fine tune request retries.
    // E.g. set total timeout for all retries.
    FLUENT_FIELD_DEFAULT(IRetryConfigProviderPtr, RetryConfigProvider, nullptr);
};

struct TExecuteBatchOptions
{
    using TSelf = TExecuteBatchOptions;

    // How many requests will be executed in parallel on the cluster.
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

// https://yt.yandex-team.ru/docs/api/commands.html#insert_rows
struct TInsertRowsOptions
    : public TTabletTransactionOptions<TInsertRowsOptions>
{
    // By default all columns missing in input data are set to Null and overwrite currently stored value.
    // If `Update' is set to true currently stored value will not be overwritten for columns that are missing in input data.
    FLUENT_FIELD_OPTION(bool, Update);

    // Used with aggregating columns.
    // By default value in aggregating column will be overwritten.
    // If `Aggregate' is set to true row will be considered as delta and it will be aggregated with currently stored value.
    FLUENT_FIELD_OPTION(bool, Aggregate);

    // Used for insert operation for tables without sync replica
    // https://yt.yandex-team.ru/docs/description/dynamic_tables/replicated_dynamic_tables#write
    // Default value is 'false'. So insertion into table without sync replias fails
    FLUENT_FIELD_OPTION(bool, RequireSyncReplica);
};

struct TDeleteRowsOptions
    : public TTabletTransactionOptions<TDeleteRowsOptions>
{
    // Used for delete operation for tables without sync replica
    // https://yt.yandex-team.ru/docs/description/dynamic_tables/replicated_dynamic_tables#write
    // Default value is 'false'. So deletion into table without sync replias fails
    FLUENT_FIELD_OPTION(bool, RequireSyncReplica);
};

struct TTrimRowsOptions
    : public TTabletTransactionOptions<TTrimRowsOptions>
{ };

// https://yt.yandex-team.ru/docs/api/commands.html#alter_table_replica
// https://yt.yandex-team.ru/docs/description/dynamic_tables/replicated_dynamic_tables
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

struct TGetFileFromCacheOptions
{
    using TSelf = TGetFileFromCacheOptions;
};

struct TPutFileToCacheOptions
{
    using TSelf = TPutFileToCacheOptions;
};

enum class EPermission : int
{
    // Applies to: all objects.
    Read         /* "read" */,

    // Applies to: all objects.
    Write        /* "write" */,

    // Applies to: accounts.
    Use          /* "use" */,

    // Applies to: all objects.
    Administer   /* "administer" */,

    // Applies to: schemas.
    Create       /* "create" */,

    // Applies to: all objects.
    Remove       /* "remove" */,

    // Applies to: tables.
    Mount        /* "mount" */,

    // Applies to: operations.
    Manage       /* "manage" */,
};

enum class ESecurityAction : int
{
    Allow /* "allow" */,
    Deny  /* "deny" */,
};

// https://yt.yandex-team.ru/docs/api/commands.html#check_permission
struct TCheckPermissionOptions
{
    using TSelf = TCheckPermissionOptions;

    // Columns to check permission to (for tables only).
    FLUENT_VECTOR_FIELD(TString, Column);
};

/// Options for @ref NYT::IClient::GetTableColumnarStatistics
struct TGetTableColumnarStatisticsOptions
{
    using TSelf = TGetTableColumnarStatisticsOptions;
};

/// Options for @ref NYT::IClient::GetTabletInfos
struct TGetTabletInfosOptions
{
    using TSelf = TGetTabletInfosOptions;
};

/// Options for @ref NYT::IClient::SkyShareTable
struct TSkyShareTableOptions
{
    using TSelf = TSkyShareTableOptions;

    ///
    /// @brief Key columns that are used to group files in a table into torrents.
    ///
    /// One torrent is created for each value of `KeyColumns` columns.
    /// If not specified, all files go into single torrent.
    FLUENT_FIELD_OPTION(TKeyColumns, KeyColumns)

    /// @brief Allow skynet manager to return fastbone links to skynet. See YT-11437
    FLUENT_FIELD_OPTION(bool, EnableFastbone)
};

} // namespace NYT
