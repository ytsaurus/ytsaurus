#pragma once

///
/// @file mapreduce/yt/interface/client_method_options.h
///
/// Header containing options for @ref NYT::IClient methods.

#include "common.h"
#include "format.h"
#include "retry_policy.h"

#include <util/datetime/base.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

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
    NT_LINK                 /* "link" */,
};

///
/// @brief Mode of composite type representation in yson.
///
/// @see https://yt.yandex-team.ru/docs/description/storage/data_types#yson
enum class EComplexTypeMode : int
{
    Named /* "named" */,
    Positional /* "positional" */,
};

///
/// @brief Options for @ref NYT::ICypressClient::Create
///
/// @see https://yt.yandex-team.ru/docs/api/commands.html#create
struct TCreateOptions
{
    /// @cond Doxygen_Suppress
    using TSelf = TCreateOptions;
    /// @endcond

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
    /// @cond Doxygen_Suppress
    using TSelf = TRemoveOptions;
    /// @endcond

    ///
    /// @brief Remove whole tree when removing composite cypress node (e.g. `map_node`).
    ///
    /// Without this option removing nonempty composite node will fail.
    FLUENT_FIELD_DEFAULT(bool, Recursive, false);

    /// @brief Do not fail if removing node doesn't exist.
    FLUENT_FIELD_DEFAULT(bool, Force, false);
};

template <typename TDerived>
struct TMasterReadOptions
{
    /// @cond Doxygen_Suppress
    using TSelf = TDerived;
    /// @endcond

    /// @brief Where to read from.
    FLUENT_FIELD_OPTION(EMasterReadKind, ReadFrom);
};

///
/// @brief Options for @ref NYT::ICypressClient::Exists
///
/// @see https://yt.yandex-team.ru/docs/api/commands.html#exists
struct TExistsOptions
    : public TMasterReadOptions<TExistsOptions>
{
};

///
/// @brief Options for @ref NYT::ICypressClient::Get
///
/// @see https://yt.yandex-team.ru/docs/api/commands.html#get
struct TGetOptions
    : public TMasterReadOptions<TGetOptions>
{
    /// @brief Attributes that should be fetched with each node.
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
    /// @cond Doxygen_Suppress
    using TSelf = TSetOptions;
    /// @endcond

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
    : public TMasterReadOptions<TListOptions>
{
    /// @cond Doxygen_Suppress
    using TSelf = TListOptions;
    /// @endcond

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
    /// @cond Doxygen_Suppress
    using TSelf = TCopyOptions;
    /// @endcond

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
    /// @cond Doxygen_Suppress
    using TSelf = TMoveOptions;
    /// @endcond

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
    /// @cond Doxygen_Suppress
    using TSelf = TLinkOptions;
    /// @endcond

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
    /// @cond Doxygen_Suppress
    using TSelf = TConcatenateOptions;
    /// @endcond

    /// Whether we should append to destination or rewrite it.
    FLUENT_FIELD_OPTION(bool, Append);
};

///
/// @brief Options for @ref NYT::IIOClient::CreateBlobTableReader
///
/// @see https://yt.yandex-team.ru/docs/api/commands.html#read_blob_table
struct TBlobTableReaderOptions
{
    /// @cond Doxygen_Suppress
    using TSelf = TBlobTableReaderOptions;
    /// @endcond

    /// Name of the part index column. By default it is "part_index".
    FLUENT_FIELD_OPTION(TString, PartIndexColumnName);

    /// Name of the data column. By default it is "data".
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

///
/// @brief Resource limits for operation (or pool)
///
/// @see https://yt.yandex-team.ru/docs/description/mr/scheduler/scheduler_and_pools#resursy
/// @see NYT::TUpdateOperationParametersOptions
struct TResourceLimits
{
    /// @cond Doxygen_Suppress
    using TSelf = TResourceLimits;
    /// @endcond

    /// Number of slots for user jobs.
    FLUENT_FIELD_OPTION(i64, UserSlots);

    /// Number of cpu cores.
    FLUENT_FIELD_OPTION(double, Cpu);

    /// Network usage. Doesn't have precise physical unit.
    FLUENT_FIELD_OPTION(i64, Network);

    /// Memory in bytes.
    FLUENT_FIELD_OPTION(i64, Memory);
};

///
/// @brief Scheduling options for single pool tree.
///
/// @see NYT::TUpdateOperationParametersOptions
struct TSchedulingOptions
{
    /// @cond Doxygen_Suppress
    using TSelf = TSchedulingOptions;
    /// @endcond

    ///
    /// @brief Pool to switch operation to.
    ///
    /// @note Switching is currently disabled on the server (will induce an exception).
    FLUENT_FIELD_OPTION(TString, Pool);

    /// @brief Operation weight.
    FLUENT_FIELD_OPTION(double, Weight);

    /// @brief Operation resource limits.
    FLUENT_FIELD_OPTION(TResourceLimits, ResourceLimits);
};

///
/// @brief Collection of scheduling options for multiple pool trees.
///
/// @see NYT::TUpdateOperationParametersOptions
struct TSchedulingOptionsPerPoolTree
{
    /// @cond Doxygen_Suppress
    using TSelf = TSchedulingOptionsPerPoolTree;
    /// @endcond

    TSchedulingOptionsPerPoolTree(const THashMap<TString, TSchedulingOptions>& options = {})
        : Options_(options)
    { }

    /// @breif Add scheduling options for pool tree.
    TSelf& Add(TStringBuf poolTreeName, const TSchedulingOptions& schedulingOptions)
    {
        Y_ENSURE(Options_.emplace(poolTreeName, schedulingOptions).second);
        return *this;
    }

    THashMap<TString, TSchedulingOptions> Options_;
};

///
/// @brief Options for @ref NYT::IOperation::SuspendOperation
///
/// @ref https://yt.yandex-team.ru/docs/api/commands.html#suspend_op
struct TSuspendOperationOptions
{
    /// @cond Doxygen_Suppress
    using TSelf = TSuspendOperationOptions;
    /// @endcond

    ///
    /// @brief Whether to abort already running jobs.
    ///
    /// By default running jobs are not aborted.
    FLUENT_FIELD_OPTION(bool, AbortRunningJobs);
};

///
/// @brief Options for @ref NYT::IOperation::ResumeOperation
///
/// @note They are empty for now but options might appear in the future.
///
/// @ref https://yt.yandex-team.ru/docs/api/commands.html#resume_op
struct TResumeOperationOptions
{
    /// @cond Doxygen_Suppress
    using TSelf = TResumeOperationOptions;
    /// @endcond
};

///
/// @brief Options for @ref NYT::IOperation::UpdateParameters
///
/// @ref https://yt.yandex-team.ru/docs/api/commands.html#update_op_parameters
struct TUpdateOperationParametersOptions
{
    /// @cond Doxygen_Suppress
    using TSelf = TUpdateOperationParametersOptions;
    /// @endcond

    /// @brief New owners of the operation.
    FLUENT_VECTOR_FIELD(TString, Owner);

    /// Pool to switch operation to (for all pool trees it is running in).
    FLUENT_FIELD_OPTION(TString, Pool);

    /// New operation weight (for all pool trees it is running in).
    FLUENT_FIELD_OPTION(double, Weight);

    /// Scheduling options for each pool tree the operation is running in.
    FLUENT_FIELD_OPTION(TSchedulingOptionsPerPoolTree, SchedulingOptionsPerPoolTree);
};

///
/// @brief Base class for many options related to IO.
///
/// @ref NYT::TFileWriterOptions
/// @ref NYT::TFileReaderOptions
/// @ref NYT::TTableReaderOptions
/// @ref NYT::TTableWriterOptions
template <class TDerived>
struct TIOOptions
{
    /// @cond Doxygen_Suppress
    using TSelf = TDerived;
    /// @endcond

    FLUENT_FIELD_OPTION(TNode, Config);

    ///
    /// @brief Whether to create internal client transaction for reading / writing table.
    ///
    /// This is advanced option.
    ///
    /// If `CreateTransaction` is set to `false`  reader/writer doesn't create internal transaction
    /// and doesn't lock table. This option is overriden (effectively `false`) for writers by
    /// @ref NYT::TWriterOptions::SingleHttpRequest
    ///
    /// WARNING: if `CreateTransaction` is `false`, read/write might become non-atomic.
    /// Change ONLY if you are sure what you are doing!
    FLUENT_FIELD_DEFAULT(bool, CreateTransaction, true);
};

/// @brief Options for reading file from YT.
struct TFileReaderOptions
    : public TIOOptions<TFileReaderOptions>
{
    /// @brief Offset to start reading from.
    ///
    /// By default reading is started from the beginning of the file.
    FLUENT_FIELD_OPTION(i64, Offset);

    /// @brief Maximum length to read.
    ///
    /// By default file is read until the end.
    FLUENT_FIELD_OPTION(i64, Length);
};

/// @brief Options that control how server side of YT stores data.
struct TWriterOptions
{
    /// @cond Doxygen_Suppress
    using TSelf = TWriterOptions;
    /// @endcond

    ///
    /// @brief Whether to wait all replicas to be written.
    ///
    /// When set to true upload will be considered successful as soon as
    /// @ref NYT::TWriterOptions::MinUploadReplicationFactor number of replicas are created.
    FLUENT_FIELD_OPTION(bool, EnableEarlyFinish);

    /// Number of replicas to be created.
    FLUENT_FIELD_OPTION(ui64, UploadReplicationFactor);

    ///
    /// Min number of created replicas needed to consider upload successful.
    ///
    /// @see NYT::TWriterOptions::EnableEarlyFinish
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
    /// @note Written chunks cannot be larger than size of this memory buffer.
    ///
    /// Since DesiredChunkSize is compared against data already compressed with compression codec
    /// it makes sense to set `RetryBlockSize = DesiredChunkSize / ExpectedCompressionRatio`
    ///
    /// @see @ref NYT::TWriterOptions::DesiredChunkSize
    /// @see @ref NYT::TWriterOptions::SingleHttpRequest
    FLUENT_FIELD_OPTION(size_t, RetryBlockSize);
};

///
/// @brief Options for writing file
///
/// @see NYT::IIOClient::CreateFileWriter
struct TFileWriterOptions
    : public TIOOptions<TFileWriterOptions>
{
    ///
    /// @brief Whether to compute MD5 sum of written file.
    ///
    /// If ComputeMD5 is set to `true` and we are appending to an existing file
    /// the `md5` attribute must be set (i.e. it was previously written only with `ComputeMD5 == true`).
    FLUENT_FIELD_OPTION(bool, ComputeMD5);

    ///
    /// @breif Options to control how YT server side writes data.
    ///
    /// @see NYT::TWriterOptions
    FLUENT_FIELD_OPTION(TWriterOptions, WriterOptions);
};

/// Options that control how C++ objects represent table rows when reading or writing a table.
class TFormatHints
{
public:
    /// @cond Doxygen_Suppress
    using TSelf = TFormatHints;
    /// @endcond

    ///
    /// @brief Whether to skip null values.
    ///
    /// When set to true TNode doesn't contain null column values
    /// (e.g. corresponding keys will be missing instead of containing null value).
    ///
    /// Only meaningful for TNode representation.
    ///
    /// Useful for sparse tables which have many columns in schema
    /// but only few columns are set in any row.
    FLUENT_FIELD_DEFAULT(bool, SkipNullValuesForTNode, false);

    ///
    /// @brief Whether to convert string to numeric and boolean types (e.g. "42u" -> 42u, "false" -> %false)
    /// when writing to schemaful table.
    FLUENT_FIELD_OPTION(bool, EnableStringToAllConversion);

    ///
    /// @brief Whether to convert numeric and boolean types to string (e.g., 3.14 -> "3.14", %true -> "true")
    /// when writing to schemaful table.
    FLUENT_FIELD_OPTION(bool, EnableAllToStringConversion);

    ///
    /// @brief Whether to convert uint64 <-> int64 when writting to schemaful table.
    ///
    /// On overflow the corresponding error with be raised.
    ///
    /// This options is enabled by default.
    FLUENT_FIELD_OPTION(bool, EnableIntegralTypeConversion);

    /// Whether to convert uint64 and int64 to double (e.g. 42 -> 42.0) when writing to schemaful table.
    FLUENT_FIELD_OPTION(bool, EnableIntegralToDoubleConversion);

    /// Shortcut for enabling all type conversions.
    FLUENT_FIELD_OPTION(bool, EnableTypeConversion);

    ///
    /// @brief Controls how complex types are represented in TNode or yson-strings.
    ///
    /// @see https://yt.yandex-team.ru/docs/description/storage/data_types#yson
    FLUENT_FIELD_OPTION(EComplexTypeMode, ComplexTypeMode);

    ///
    /// @brief Apply the patch to the fields.
    ///
    /// Non-default and non-empty values replace the default and empty ones.
    void Merge(const TFormatHints& patch);
};

/// Options for @ref NYT::IClient::CreateTableReader
struct TTableReaderOptions
    : public TIOOptions<TTableReaderOptions>
{
    /// @deprecated Size of internal client buffer.
    FLUENT_FIELD_DEFAULT(size_t, SizeLimit, 4 << 20);

    ///
    /// @brief Allows to fine tune format that is used for reading tables.
    ///
    /// Has no effect when used with raw-reader.
    FLUENT_FIELD_OPTION(TFormatHints, FormatHints);
};

/// Options for @ref NYT::IClient::CreateTableWriter
struct TTableWriterOptions
    : public TIOOptions<TTableWriterOptions>
{
    ///
    /// @brief Enable or disable retryful writing.
    ///
    /// If set to true no retry is made but we also make less requests to master.
    /// If set to false writer can make up to `TConfig::RetryCount` attempts to send each block of data.
    ///
    /// @note Writers' methods might throw strange exceptions that might look like network error
    /// when `SingleHttpRequest == true` and YT node encounters an error
    /// (due to limitations of HTTP protocol YT node have no chance to report error
    /// before it reads the whole input so it just drops the connection).
    FLUENT_FIELD_DEFAULT(bool, SingleHttpRequest, false);

    //
    // Allows to change the size of locally buffered rows before flushing to yt
    // Measured in bytes
    // Default value is 64Mb
    FLUENT_FIELD_DEFAULT(size_t, BufferSize, 64 << 20);

    /// @brief Allows to fine tune format that is used for writing tables.
    ///
    /// Has no effect when used with raw-writer.
    FLUENT_FIELD_OPTION(TFormatHints, FormatHints);

    // Try to infer schema of inexistent table from the type of written rows.
    //
    // NOTE: Default values for this option may differ depending on the row type.
    // For protobuf it's currently false by default.
    FLUENT_FIELD_OPTION(bool, InferSchema);

    FLUENT_FIELD_OPTION(TWriterOptions, WriterOptions);
};

///
/// @brief Options for @ref NYT::IClient::StartTransaction
///
/// @ref https://yt.yandex-team.ru/docs/api/commands.html#start_tx
struct TStartTransactionOptions
{
    /// @cond Doxygen_Suppress
    using TSelf = TStartTransactionOptions;
    /// @endcond

    FLUENT_FIELD_DEFAULT(bool, PingAncestors, false);

    ///
    /// @brief How long transaction lives after last ping.
    ///
    /// If server doesn't receive any pings for transaction for this time
    /// transaction will be aborted. By default timeout is 15 seconds.
    FLUENT_FIELD_OPTION(TDuration, Timeout);

    ///
    /// @brief Moment in the future when transaction is aborted.
    FLUENT_FIELD_OPTION(TInstant, Deadline);

    ///
    /// @brief Whether to ping created transaction automatically.
    ///
    /// When set to true library creates a thread that pings transaction.
    /// When set to false library doesn't ping transaction and it's user responsibility to ping it.
    FLUENT_FIELD_DEFAULT(bool, AutoPingable, true);

    ///
    /// @brief Set the title attribute of transaction.
    ///
    /// If title was not specified
    /// nither using this option nor using @ref NYT::TStartTransactionOptions::Attributes option
    /// library will generate default title for transaction.
    /// Such default title includes machine name, pid, user name and some other useful info.
    FLUENT_FIELD_OPTION(TString, Title);

    ///
    /// @brief Set custom transaction attributes
    ///
    /// @note @ref NYT::TStartTransactionOptions::Title option overrides `"title"` attribute.
    FLUENT_FIELD_OPTION(TNode, Attributes);
};

struct TAttachTransactionOptions
{
    /// @cond Doxygen_Suppress
    using TSelf = TAttachTransactionOptions;
    /// @endcond

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
    /// @cond Doxygen_Suppress
    using TSelf = TLockOptions;
    /// @endcond

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
    /// @cond Doxygen_Suppress
    using TSelf = TUnlockOptions;
    /// @endcond
};

template <class TDerived>
struct TTabletOptions
{
    /// @cond Doxygen_Suppress
    using TSelf = TDerived;
    /// @endcond

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
    /// @cond Doxygen_Suppress
    using TSelf = TAlterTableOptions;
    /// @endcond

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
    /// @cond Doxygen_Suppress
    using TSelf = TLookupRowsOptions;
    /// @endcond

    FLUENT_FIELD_OPTION(TDuration, Timeout);
    FLUENT_FIELD_OPTION(TColumnNames, Columns);
    FLUENT_FIELD_DEFAULT(bool, KeepMissingRows, false);
    FLUENT_FIELD_OPTION(bool, Versioned);
};

struct TSelectRowsOptions
{
    /// @cond Doxygen_Suppress
    using TSelf = TSelectRowsOptions;
    /// @endcond

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
    /// @cond Doxygen_Suppress
    using TSelf = TCreateClientOptions;
    /// @endcond

    FLUENT_FIELD(TString, Token);
    FLUENT_FIELD(TString, TokenPath);

    // RetryConfig provider allows to fine tune request retries.
    // E.g. set total timeout for all retries.
    FLUENT_FIELD_DEFAULT(IRetryConfigProviderPtr, RetryConfigProvider, nullptr);
};

struct TExecuteBatchOptions
{
    /// @cond Doxygen_Suppress
    using TSelf = TExecuteBatchOptions;
    /// @endcond

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
    /// @cond Doxygen_Suppress
    using TSelf = TDerived;
    /// @endcond

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
    /// @cond Doxygen_Suppress
    using TSelf = TAlterTableReplicaOptions;
    /// @endcond

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
    /// @cond Doxygen_Suppress
    using TSelf = TGetFileFromCacheOptions;
    /// @endcond
};

struct TPutFileToCacheOptions
{
    /// @cond Doxygen_Suppress
    using TSelf = TPutFileToCacheOptions;
    /// @endcond
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
    /// @cond Doxygen_Suppress
    using TSelf = TCheckPermissionOptions;
    /// @endcond

    // Columns to check permission to (for tables only).
    FLUENT_VECTOR_FIELD(TString, Column);
};

/// Options for @ref NYT::IClient::GetTableColumnarStatistics
struct TGetTableColumnarStatisticsOptions
{
    /// @cond Doxygen_Suppress
    using TSelf = TGetTableColumnarStatisticsOptions;
    /// @endcond
};

/// Options for @ref NYT::IClient::GetTabletInfos
struct TGetTabletInfosOptions
{
    /// @cond Doxygen_Suppress
    using TSelf = TGetTabletInfosOptions;
    /// @endcond
};

/// Options for @ref NYT::IClient::SkyShareTable
struct TSkyShareTableOptions
{
    /// @cond Doxygen_Suppress
    using TSelf = TSkyShareTableOptions;
    /// @endcond

    ///
    /// @brief Key columns that are used to group files in a table into torrents.
    ///
    /// One torrent is created for each value of `KeyColumns` columns.
    /// If not specified, all files go into single torrent.
    FLUENT_FIELD_OPTION(TColumnNames, KeyColumns)

    /// @brief Allow skynet manager to return fastbone links to skynet. See YT-11437
    FLUENT_FIELD_OPTION(bool, EnableFastbone)
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
