#pragma once

///
/// @file mapreduce/yt/interface/operation.h
///
/// Header containing interface to run operations in YT
/// and retrieve information about them.
/// @see [the doc](https://yt.yandex-team.ru/docs/description/mr/map_reduce_overview.html).

#include "client_method_options.h"
#include "errors.h"
#include "io.h"
#include "job_statistics.h"

#include <library/cpp/threading/future/future.h>
#include <library/cpp/type_info/type_info.h>

#include <util/datetime/base.h>
#include <util/generic/variant.h>
#include <util/generic/vector.h>
#include <util/generic/maybe.h>
#include <util/system/file.h>
#include <util/system/types.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

/// Tag class marking that the row type for table is not specified.
struct TUnspecifiedTableStructure
{ };

/// Tag class marking that table rows have protobuf type.
struct TProtobufTableStructure
{
    /// @brief Descriptor of the protobuf type of table rows.
    ///
    /// @note If table is tagged with @ref ::google::protobuf::Message instead of real proto class
    /// this descriptor might be null.
    const ::google::protobuf::Descriptor* Descriptor = nullptr;
};

/// Tag class marking that table rows have YDL type.
struct TYdlTableStructure
{
    NTi::TTypePtr Type = nullptr;
};

/// Tag class to specify table row type.
using TTableStructure = ::TVariant<
    TUnspecifiedTableStructure,
    TProtobufTableStructure,
    TYdlTableStructure
>;

bool operator==(const TUnspecifiedTableStructure&, const TUnspecifiedTableStructure&);
bool operator==(const TProtobufTableStructure& lhs, const TProtobufTableStructure& rhs);
bool operator==(const TYdlTableStructure& lhs, const TYdlTableStructure& rhs);

/// Table path marked with @ref NYT::TTableStructure tag.
struct TStructuredTablePath
{
    TStructuredTablePath(TRichYPath richYPath = TRichYPath(), TTableStructure description = TUnspecifiedTableStructure())
        : RichYPath(std::move(richYPath))
        , Description(std::move(description))
    { }

    TStructuredTablePath(TRichYPath richYPath, const ::google::protobuf::Descriptor* descriptor)
        : RichYPath(std::move(richYPath))
        , Description(TProtobufTableStructure({descriptor}))
    { }

    TStructuredTablePath(TYPath path)
        : RichYPath(std::move(path))
        , Description(TUnspecifiedTableStructure())
    { }

    TStructuredTablePath(const char* path)
        : RichYPath(path)
        , Description(TUnspecifiedTableStructure())
    { }

    TStructuredTablePath(TRichYPath richYPath, NTi::TTypePtr typePtr)
        : RichYPath(std::move(richYPath))
        , Description(TYdlTableStructure{std::move(typePtr)})
    { }

    TRichYPath RichYPath;
    TTableStructure Description;
};

/// Create marked table path from row type.
template <typename TRow>
TStructuredTablePath Structured(TRichYPath richYPath);

/// Create tag class from row type.
template <typename TRow>
TTableStructure StructuredTableDescription();

///////////////////////////////////////////////////////////////////////////////

/// Tag class marking that row stream is empty.
struct TVoidStructuredRowStream
{ };

/// Tag class marking that row stream consists of @ref NYT:TNode.
struct TTNodeStructuredRowStream
{ };

/// Tag class marking that row stream consists of @ref NYT::TYaMRRow.
struct TTYaMRRowStructuredRowStream
{ };

/// Tag class marking that row stream consists of YDL rows of given type.
struct TYdlStructuredRowStream
{
    NTi::TTypePtr Type = nullptr;
};

/// Tag class marking that row stream consists of protobuf rows of given type.
struct TProtobufStructuredRowStream
{
    /// @brief Descriptor of the protobuf type of table rows.
    ///
    /// @note If `Descriptor` is nullptr, then row stream consists of multiple message types.
    const ::google::protobuf::Descriptor* Descriptor = nullptr;
};

/// Tag class to specify type of rows in an operation row stream 
using TStructuredRowStreamDescription = ::TVariant<
    TVoidStructuredRowStream,
    TTNodeStructuredRowStream,
    TTYaMRRowStructuredRowStream,
    TYdlStructuredRowStream,
    TProtobufStructuredRowStream
>;

///////////////////////////////////////////////////////////////////////////////

/// Tag class marking that current binary should be used in operation.
struct TJobBinaryDefault
{ };

/// Tag class marking that binary from specified local path should be used in operation.
struct TJobBinaryLocalPath
{
    TString Path;
    TMaybe<TString> MD5CheckSum;
};

/// Tag class marking that binary from specified Cypress path should be used in operation.
struct TJobBinaryCypressPath
{
    TYPath Path;
    TMaybe<TTransactionId> TransactionId;
};

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {
    extern i64 OutputTableCount;
} // namespace NDetail

////////////////////////////////////////////////////////////////////////////////

/// Base for input format hints of a user job.
template <class TDerived>
class TUserJobInputFormatHintsBase
{
public:
    using TSelf = TDerived;

    FLUENT_FIELD_OPTION(TFormatHints, InputFormatHints);
};

/// Base for output format hints of a user job.
template <class TDerived>
class TUserJobOutputFormatHintsBase
{
public:
    using TSelf = TDerived;

    FLUENT_FIELD_OPTION(TFormatHints, OutputFormatHints);
};

/// Base for format hints of a user job.
template <class TDerived>
class TUserJobFormatHintsBase
    : public TUserJobInputFormatHintsBase<TDerived>
    , public TUserJobOutputFormatHintsBase<TDerived>
{
public:
    using TSelf = TDerived;
};

/// User job format hints.
class TUserJobFormatHints
    : public TUserJobFormatHintsBase<TUserJobFormatHints>
{ };

/// Spec of input and output tables of a raw operation.
template <class TDerived>
class TRawOperationIoTableSpec
{
public:
    using TSelf = TDerived;

    /// Add input table path to input path list.
    TDerived& AddInput(const TRichYPath& path);

    /// Set input table path no. `tableIndex`.
    TDerived& SetInput(size_t tableIndex, const TRichYPath& path);

    /// Add output table path to output path list.
    TDerived& AddOutput(const TRichYPath& path);

    /// Set output table path no. `tableIndex`.
    TDerived& SetOutput(size_t tableIndex, const TRichYPath& path);

    /// Get all input table paths.
    const TVector<TRichYPath>& GetInputs() const;

    /// Get all output table paths.
    const TVector<TRichYPath>& GetOutputs() const;

private:
    TVector<TRichYPath> Inputs_;
    TVector<TRichYPath> Outputs_;
};

/// Base spec for IO in "simple" raw operations (Map, Reduce etc.).
template <class TDerived>
struct TSimpleRawOperationIoSpec
    : public TRawOperationIoTableSpec<TDerived>
{
    using TSelf = TDerived;

    /// @brief Describes format for both input and output.
    ///
    /// @note `Format' is overriden by `InputFormat' and `OutputFormat'.
    FLUENT_FIELD_OPTION(TFormat, Format);

    /// Describes input format.
    FLUENT_FIELD_OPTION(TFormat, InputFormat);

    /// Describes output format.
    FLUENT_FIELD_OPTION(TFormat, OutputFormat);
};

/// Spec for IO in MapReduce operation.
template <class TDerived>
class TRawMapReduceOperationIoSpec
    : public TRawOperationIoTableSpec<TDerived>
{
public:
    using TSelf = TDerived;

    /// @brief Describes format for both input and output of mapper.
    ///
    /// @note `MapperFormat' is overriden by `MapperInputFormat' and `MapperOutputFormat'.
    FLUENT_FIELD_OPTION(TFormat, MapperFormat);

    /// Describes mapper input format.
    FLUENT_FIELD_OPTION(TFormat, MapperInputFormat);

    /// Describes mapper output format.
    FLUENT_FIELD_OPTION(TFormat, MapperOutputFormat);

    /// @brief Describes format for both input and output of reduce combiner.
    ///
    /// @note `ReduceCombinerFormat' is overriden by `ReduceCombinerInputFormat' and `ReduceCombinerOutputFormat'.
    FLUENT_FIELD_OPTION(TFormat, ReduceCombinerFormat);

    /// Describes reduce combiner input format.
    FLUENT_FIELD_OPTION(TFormat, ReduceCombinerInputFormat);

    /// Describes reduce combiner output format.
    FLUENT_FIELD_OPTION(TFormat, ReduceCombinerOutputFormat);

    /// @brief Describes format for both input and output of reducer.
    ///
    /// @note `ReducerFormat' is overriden by `ReducerInputFormat' and `ReducerOutputFormat'.
    FLUENT_FIELD_OPTION(TFormat, ReducerFormat);

    /// Describes reducer input format.
    FLUENT_FIELD_OPTION(TFormat, ReducerInputFormat);

    /// Describes reducer output format.
    FLUENT_FIELD_OPTION(TFormat, ReducerOutputFormat);

    /// Add direct map output table path.
    TDerived& AddMapOutput(const TRichYPath& path);

    /// Set direct map output table path no. `tableIndex`.
    TDerived& SetMapOutput(size_t tableIndex, const TRichYPath& path);

    /// Get all direct map output table paths 
    const TVector<TRichYPath>& GetMapOutputs() const;

private:
    TVector<TRichYPath> MapOutputs_;
};

/// Base spec of input tables 
class TOperationInputSpecBase
{
public:
    template <class T, class = void>
    struct TFormatAdder;

    /// Add input table path to input path list and specify type of rows.
    template <class T>
    void AddInput(const TRichYPath& path);

    void AddStructuredInput(TStructuredTablePath path);

    template <class T>
    void SetInput(size_t tableIndex, const TRichYPath& path);

    TVector<TRichYPath> Inputs_;

    const TVector<TStructuredTablePath>& GetStructuredInputs() const;

private:
    TVector<TStructuredTablePath> StructuredInputs_;
    friend struct TOperationIOSpecBase;
    template <class T>
    friend struct TOperationIOSpec;
};

class TOperationOutputSpecBase
{
public:
    template <class T, class = void>
    struct TFormatAdder;

    template <class T>
    void AddOutput(const TRichYPath& path);

    void AddStructuredOutput(TStructuredTablePath path);

    template <class T>
    void SetOutput(size_t tableIndex, const TRichYPath& path);

    TVector<TRichYPath> Outputs_;

    const TVector<TStructuredTablePath>& GetStructuredOutputs() const;

private:
    TVector<TStructuredTablePath> StructuredOutputs_;
    friend struct TOperationIOSpecBase;
    template <class T>
    friend struct TOperationIOSpec;
};

struct TOperationIOSpecBase
    : public TOperationInputSpecBase
    , public TOperationOutputSpecBase
{ };

template <class TDerived>
struct TOperationIOSpec
    : public TOperationIOSpecBase
{
    using TSelf = TDerived;

    template <class T>
    TDerived& AddInput(const TRichYPath& path);

    TDerived& AddStructuredInput(TStructuredTablePath path);

    template <class T>
    TDerived& SetInput(size_t tableIndex, const TRichYPath& path);

    template <class T>
    TDerived& AddOutput(const TRichYPath& path);

    TDerived& AddStructuredOutput(TStructuredTablePath path);

    template <class T>
    TDerived& SetOutput(size_t tableIndex, const TRichYPath& path);


    // DON'T USE THESE METHODS! They are left solely for backward compatibility.
    // These methods are the only way to do equivalent of (Add/Set)(Input/Output)<Message>
    // but please consider using (Add/Set)(Input/Output)<TConcreteMessage>
    // (where TConcreteMessage is some descendant of Message)
    // because they are faster and better (see https://st.yandex-team.ru/YT-6967)
    TDerived& AddProtobufInput_VerySlow_Deprecated(const TRichYPath& path);
    TDerived& AddProtobufOutput_VerySlow_Deprecated(const TRichYPath& path);
};

template <class TDerived>
struct TOperationSpecBase
{
    using TSelf = TDerived;

    ///
    /// @prief Limit on operation execution time.
    ///
    /// If operation doesn't finish in time it will be aborted.
    FLUENT_FIELD_OPTION(TDuration, TimeLimit);

    /// @brief Title to be shown in web interface.
    FLUENT_FIELD_OPTION(TString, Title);
};

template <class TDerived>
struct TUserOperationSpecBase
    : TOperationSpecBase<TDerived>
{
    using TSelf = TDerived;

    // How many jobs can fail before operation is failed.
    FLUENT_FIELD_OPTION(ui64, MaxFailedJobCount);

    // On any unsuccessful job completion (i.e. abortion or failure) force the whole operation to fail.
    FLUENT_FIELD_OPTION(bool, FailOnJobRestart);

    // Table to save whole stderr of operation
    // https://clubs.at.yandex-team.ru/yt/1045
    FLUENT_FIELD_OPTION(TYPath, StderrTablePath);

    // Table to save coredumps of operation
    // https://clubs.at.yandex-team.ru/yt/1045
    FLUENT_FIELD_OPTION(TYPath, CoreTablePath);

    // How long should the scheduler wait for the job to be started on a node.
    // When you run huge jobs that require preemption of all the other jobs on
    // a node, the default timeout might be insufficient and your job may be
    // aborted with 'waiting_timeout' reason. This is especially problematic
    // when you are setting 'FailOnJobRestart' option.
    // The value must be between 10 seconds and 10 minutes.
    FLUENT_FIELD_OPTION(TDuration, WaitingJobTimeout);
};

template <class TDerived>
struct TIntermediateTablesHintSpec
{
    // When using protobuf format it is important to know exact types of proto messages
    // that are used in input/output.
    //
    // Sometimes such messages cannot be derived from job class
    // i.e. when job class uses TTableReader<::google::protobuf::Message>
    // or TTableWriter<::google::protobuf::Message>
    //
    // When using such jobs user can provide exact message type using functions below.
    //
    // NOTE: only input/output that relate to intermediate tables can be hinted.
    // Input to map and output of reduce is derived from AddInput/AddOutput.
    template <class T>
    TDerived& HintMapOutput();

    template <class T>
    TDerived& HintReduceCombinerInput();
    template <class T>
    TDerived& HintReduceCombinerOutput();

    template <class T>
    TDerived& HintReduceInput();

    // Add output of map stage.
    // Mapper output table #0 is always intermediate table that is going to be reduced later.
    // Rows that mapper write to tables #1, #2, ... are saved in MapOutput tables.
    template <class T>
    TDerived& AddMapOutput(const TRichYPath& path);

    TVector<TRichYPath> MapOutputs_;

    const TVector<TStructuredTablePath>& GetStructuredMapOutputs() const;
    const TMaybe<TTableStructure>& GetIntermediateMapOutputDescription() const;
    const TMaybe<TTableStructure>& GetIntermediateReduceCombinerInputDescription() const;
    const TMaybe<TTableStructure>& GetIntermediateReduceCombinerOutputDescription() const;
    const TMaybe<TTableStructure>& GetIntermediateReducerInputDescription() const;

private:
    TVector<TStructuredTablePath> StructuredMapOutputs_;
    TMaybe<TTableStructure> IntermediateMapOutputDescription_;
    TMaybe<TTableStructure> IntermediateReduceCombinerInputDescription_;
    TMaybe<TTableStructure> IntermediateReduceCombinerOutputDescription_;
    TMaybe<TTableStructure> IntermediateReducerInputDescription_;
};

////////////////////////////////////////////////////////////////////////////////

struct TAddLocalFileOptions
{
    using TSelf = TAddLocalFileOptions;

    // Path by which job will see the uploaded file.
    // Defaults to basename of the local path.
    FLUENT_FIELD_OPTION(TString, PathInJob);

    // MD5 checksum
    // This library computes md5 checksum for all files that are uploaded to YT.
    // When md5 checksum is known user might provide it as `MD5CheckSum`
    // argument to save some cpu and disk IO.
    FLUENT_FIELD_OPTION(TString, MD5CheckSum);
};

struct TUserJobSpec
{
    using TSelf = TUserJobSpec;

    TSelf&  AddLocalFile(const TLocalFilePath& path, const TAddLocalFileOptions& options = TAddLocalFileOptions());
    TVector<std::tuple<TLocalFilePath, TAddLocalFileOptions>> GetLocalFiles() const;

    FLUENT_VECTOR_FIELD(TRichYPath, File);

    ///
    /// @brief MemoryLimit specifies how much memory job process can use.
    ///
    /// @note
    /// If job uses tmpfs (check @ref NYT::TOperationOptions::MountSandboxInTmpfs)
    /// YT computes its memory usage as total of:
    ///   - memory usage of job process itself (including mapped files);
    ///   - total size of tmpfs used by this job.
    ///
    /// @note
    /// When @ref NYT::TOperationOptions::MountSandboxInTmpfs is enabled library will compute
    /// total size of all files used by this job and add this total size to MemoryLimit.
    /// Thus you shouldn't include size of your files (e.g. binary file) into MemoryLimit.
    ///
    /// @note
    /// Final memory memory_limit passed to YT is calculated as follows:
    ///
    /// @note
    /// ```
    /// memory_limit = MemoryLimit + <total-size-of-used-files> + ExtraTmpfsSize
    /// ```
    ///
    /// @see NYT::TUserJobSpec::ExtraTmpfsSize
    FLUENT_FIELD_OPTION(i64, MemoryLimit);

    ///
    /// @brief Size of data that is going to be written to tmpfs.
    ///
    /// This option should be used if job writes data to tmpfs.
    ///
    /// ExtraTmpfsSize should not include size of files specified with
    /// @ref NYT::TUserJobSpec::AddLocalFile or @ref NYT::TUserJobSpec::File
    /// These files are copied to tmpfs automatically and their total size
    /// is computed automatically.
    ///
    /// @see NYT::TOperationOptions::MountSandboxInTmpfs
    /// @see NYT::TUserJobSpec::MemoryLimit
    FLUENT_FIELD_OPTION(i64, ExtraTmpfsSize);

    FLUENT_FIELD_OPTION(double, CpuLimit);

    //
    // https://yt.yandex-team.ru/docs/description/mr/operations_options#memory_reserve_factor
    //
    // Defines a fraction of MemoryLimit that job gets at start
    FLUENT_FIELD_OPTION(double, MemoryReserveFactor);

    //
    // JobBinary allows to specify path to executable that is to be used inside jobs.
    // Provided executable must use C++ YT API library (this library)
    // and implement job class that is going to be used.
    //
    // This option might be useful if we want to start operation from nonlinux machines
    // (in that case we use JobBinary to provide path to the same program compiled for linux).
    // Other example of using this option is uploading executable to cypress in advance
    // and save the time required to upload current executable to cache.
    // `md5` argument can be used to save cpu time and disk IO when binary md5 checksum is known.
    // When argument is not provided library will compute it itself.
    TUserJobSpec& JobBinaryLocalPath(TString path, TMaybe<TString> md5 = Nothing());
    TUserJobSpec& JobBinaryCypressPath(TString path, TMaybe<TTransactionId> transactionId = Nothing());
    const TJobBinaryConfig& GetJobBinary() const;

    //
    // Prefix and suffix for specific kind of job
    // Overrides common prefix and suffix in TOperationOptions
    FLUENT_FIELD(TString, JobCommandPrefix);
    FLUENT_FIELD(TString, JobCommandSuffix);

    //
    // Map of environment variables that will be set for jobs.
    FLUENT_MAP_FIELD(TString, TString, Environment);

    //
    // Limit for all files inside job sandbox.
    FLUENT_FIELD_OPTION(ui64, DiskSpaceLimit);

    //
    // Number of ports reserved for the job. They are passed through environment in YT_PORT_0, YT_PORT_1, ...
    FLUENT_FIELD_OPTION(ui16, PortCount);

    //
    // Limit on job execution time.
    // Jobs that exceed this limit will be considered failed.
    FLUENT_FIELD_OPTION(TDuration, JobTimeLimit);

private:
    TVector<std::tuple<TLocalFilePath, TAddLocalFileOptions>> LocalFiles_;
    TJobBinaryConfig JobBinary_;
};

////////////////////////////////////////////////////////////////////////////////

template <typename TDerived>
struct TMapOperationSpecBase
    : public TUserOperationSpecBase<TDerived>
{
    using TSelf = TDerived;

    FLUENT_FIELD(TUserJobSpec, MapperSpec);

    // When `Ordered' is false (by default), there is no guaranties about order of reading rows.
    // In this case mapper might work slightly faster because row delivered from fast node can be processed YT waits
    // response from slow nodes.
    // When `Ordered' is true, rows will come in order in which they are stored in input tables.
    FLUENT_FIELD_OPTION(bool, Ordered);

    // `JobCount' and `DataSizePerJob' options affect how many jobs will be launched.
    // These options only provide recommendations and YT might ignore them if they conflict with YT internal limits.
    // `JobCount' has higher priority than `DataSizePerJob'.
    FLUENT_FIELD_OPTION(ui32, JobCount);
    FLUENT_FIELD_OPTION(ui64, DataSizePerJob);
};

struct TMapOperationSpec
    : public TMapOperationSpecBase<TMapOperationSpec>
    , public TOperationIOSpec<TMapOperationSpec>
    , public TUserJobFormatHintsBase<TMapOperationSpec>
{ };

struct TRawMapOperationSpec
    : public TMapOperationSpecBase<TRawMapOperationSpec>
    , public TSimpleRawOperationIoSpec<TRawMapOperationSpec>
{ };

////////////////////////////////////////////////////////////////////////////////

template <typename TDerived>
struct TReduceOperationSpecBase
    : public TUserOperationSpecBase<TDerived>
{
    using TSelf = TDerived;

    FLUENT_FIELD(TUserJobSpec, ReducerSpec);
    FLUENT_FIELD(TKeyColumns, SortBy);
    FLUENT_FIELD(TKeyColumns, ReduceBy);
    FLUENT_FIELD_OPTION(TKeyColumns, JoinBy);
    //When set to true forces controller to put all rows with same ReduceBy columns in one job
    //When set to false controller can relax this demand (default true)
    FLUENT_FIELD_OPTION(bool, EnableKeyGuarantee);

    // Similar to corresponding options in `TMapOperationSpec'.
    FLUENT_FIELD_OPTION(ui32, JobCount);
    FLUENT_FIELD_OPTION(ui64, DataSizePerJob);
};

struct TReduceOperationSpec
    : public TReduceOperationSpecBase<TReduceOperationSpec>
    , public TOperationIOSpec<TReduceOperationSpec>
    , public TUserJobFormatHintsBase<TReduceOperationSpec>
{ };

struct TRawReduceOperationSpec
    : public TReduceOperationSpecBase<TRawReduceOperationSpec>
    , public TSimpleRawOperationIoSpec<TRawReduceOperationSpec>
{ };

////////////////////////////////////////////////////////////////////////////////

template <typename TDerived>
struct TJoinReduceOperationSpecBase
    : public TUserOperationSpecBase<TDerived>
{
    using TSelf = TDerived;

    FLUENT_FIELD(TUserJobSpec, ReducerSpec);
    FLUENT_FIELD(TKeyColumns, JoinBy);

    // Similar to corresponding options in `TMapOperationSpec'.
    FLUENT_FIELD_OPTION(ui32, JobCount);
    FLUENT_FIELD_OPTION(ui64, DataSizePerJob);
};

struct TJoinReduceOperationSpec
    : public TJoinReduceOperationSpecBase<TJoinReduceOperationSpec>
    , public TOperationIOSpec<TJoinReduceOperationSpec>
    , public TUserJobFormatHintsBase<TJoinReduceOperationSpec>
{ };

struct TRawJoinReduceOperationSpec
    : public TJoinReduceOperationSpecBase<TRawJoinReduceOperationSpec>
    , public TSimpleRawOperationIoSpec<TRawJoinReduceOperationSpec>
{ };

////////////////////////////////////////////////////////////////////////////////

template <typename TDerived>
struct TMapReduceOperationSpecBase
    : public TUserOperationSpecBase<TDerived>
{
    using TSelf = TDerived;

    FLUENT_FIELD(TUserJobSpec, MapperSpec);
    FLUENT_FIELD(TUserJobSpec, ReducerSpec);
    FLUENT_FIELD(TUserJobSpec, ReduceCombinerSpec);
    FLUENT_FIELD(TKeyColumns, SortBy);
    FLUENT_FIELD(TKeyColumns, ReduceBy);

    // Similar to `JobCount' / `DataSizePerJob'.
    FLUENT_FIELD_OPTION(ui64, MapJobCount);
    FLUENT_FIELD_OPTION(ui64, DataSizePerMapJob);

    FLUENT_FIELD_OPTION(ui64, PartitionCount);
    FLUENT_FIELD_OPTION(ui64, PartitionDataSize);

    // Replication factor for intermediate data (it's equal 1 by default).
    FLUENT_FIELD_OPTION(ui64,  IntermediateDataReplicationFactor);

    // Specifies how much data should be passed to single reduce-combiner job.
    FLUENT_FIELD_OPTION(ui64, DataSizePerSortJob);

    // Ordered mode for map stage.
    // Check `Ordered' option for Map operation for more info.
    FLUENT_FIELD_OPTION(bool, Ordered);

    // Always run reduce combiner before reducer.
    FLUENT_FIELD_OPTION(bool, ForceReduceCombiners);
};

struct TMapReduceOperationSpec
    : public TMapReduceOperationSpecBase<TMapReduceOperationSpec>
    , public TOperationIOSpec<TMapReduceOperationSpec>
    , public TIntermediateTablesHintSpec<TMapReduceOperationSpec>
{
    using TSelf = TMapReduceOperationSpec;

    FLUENT_FIELD_DEFAULT(TUserJobFormatHints, MapperFormatHints, TUserJobFormatHints());
    FLUENT_FIELD_DEFAULT(TUserJobFormatHints, ReducerFormatHints, TUserJobFormatHints());
    FLUENT_FIELD_DEFAULT(TUserJobFormatHints, ReduceCombinerFormatHints, TUserJobFormatHints());
};

struct TRawMapReduceOperationSpec
    : public TMapReduceOperationSpecBase<TRawMapReduceOperationSpec>
    , public TRawMapReduceOperationIoSpec<TRawMapReduceOperationSpec>
{ };

////////////////////////////////////////////////////////////////////////////////

// See https://yt.yandex-team.ru/docs/description/storage/static_schema.html#schema_inference
enum class ESchemaInferenceMode : int
{
    FromInput   /* "from_input" */,
    FromOutput  /* "from_output" */,
    Auto        /* "auto" */,
};

struct TSortOperationSpec
    : TOperationSpecBase<TSortOperationSpec>
{
    using TSelf = TSortOperationSpec;

    FLUENT_VECTOR_FIELD(TRichYPath, Input);
    FLUENT_FIELD(TRichYPath, Output);
    FLUENT_FIELD(TKeyColumns, SortBy);

    FLUENT_FIELD_OPTION(ui64, PartitionCount);
    FLUENT_FIELD_OPTION(ui64, PartitionDataSize);

    FLUENT_FIELD_OPTION(ui64, PartitionJobCount);
    FLUENT_FIELD_OPTION(ui64, DataSizePerPartitionJob);

    FLUENT_FIELD_OPTION(ESchemaInferenceMode, SchemaInferenceMode);

    // Replication factor for intermediate data (it's equal 1 by default).
    FLUENT_FIELD_OPTION(ui64, IntermediateDataReplicationFactor);
};

enum EMergeMode : int
{
    MM_UNORDERED    /* "unordered" */,
    MM_ORDERED      /* "ordered" */,
    MM_SORTED       /* "sorted" */,
};

struct TMergeOperationSpec
    : TOperationSpecBase<TMergeOperationSpec>
{
    using TSelf = TMergeOperationSpec;

    FLUENT_VECTOR_FIELD(TRichYPath, Input);
    FLUENT_FIELD(TRichYPath, Output);
    FLUENT_FIELD(TKeyColumns, MergeBy);
    FLUENT_FIELD_DEFAULT(EMergeMode, Mode, MM_UNORDERED);
    FLUENT_FIELD_DEFAULT(bool, CombineChunks, false);
    FLUENT_FIELD_DEFAULT(bool, ForceTransform, false);

    // Similar to `JobCount' / `DataSizePerJob'.
    FLUENT_FIELD_OPTION(ui64, JobCount);
    FLUENT_FIELD_OPTION(ui64, DataSizePerJob);

    FLUENT_FIELD_OPTION(ESchemaInferenceMode, SchemaInferenceMode);
};

struct TEraseOperationSpec
    : TOperationSpecBase<TEraseOperationSpec>
{
    using TSelf = TEraseOperationSpec;

    FLUENT_FIELD(TRichYPath, TablePath);
    FLUENT_FIELD_DEFAULT(bool, CombineChunks, false);

    FLUENT_FIELD_OPTION(ESchemaInferenceMode, SchemaInferenceMode);
};

// See https://yt.yandex-team.ru/docs/description/mr/remote_copy
struct TRemoteCopyOperationSpec
    : TOperationSpecBase<TRemoteCopyOperationSpec>
{
    using TSelf = TRemoteCopyOperationSpec;

    // Source cluster name.
    FLUENT_FIELD(TString, ClusterName);

    // Network to use for copy (all remote cluster nodes must have it configured).
    FLUENT_FIELD_OPTION(TString, NetworkName);

    FLUENT_VECTOR_FIELD(TRichYPath, Input);
    FLUENT_FIELD(TRichYPath, Output);

    FLUENT_FIELD_OPTION(ESchemaInferenceMode, SchemaInferenceMode);

    // Should user attributes be copied to the output table (allowed only for single output table).
    // 'AttributeKey's vector allows to choose what attributes to copy.
    FLUENT_FIELD_DEFAULT(bool, CopyAttributes, false);
    FLUENT_VECTOR_FIELD(TString, AttributeKey);

private:
    FLUENT_FIELD_OPTION(TNode, ClusterConnection);
};

class IVanillaJobBase;

struct TVanillaTask
    : public TOperationOutputSpecBase
    , public TUserJobOutputFormatHintsBase<TVanillaTask>
{
    using TSelf = TVanillaTask;

    template <class T>
    TSelf& AddOutput(const TRichYPath& path);

    TSelf& AddStructuredOutput(TStructuredTablePath path);

    template <class T>
    TSelf& SetOutput(size_t tableIndex, const TRichYPath& path);

    FLUENT_FIELD(TString, Name);
    FLUENT_FIELD(::TIntrusivePtr<IVanillaJobBase>, Job);
    FLUENT_FIELD(TUserJobSpec, Spec);
    FLUENT_FIELD(ui64, JobCount);
};

struct TVanillaOperationSpec
    : TUserOperationSpecBase<TVanillaOperationSpec>
{
    using TSelf = TVanillaOperationSpec;

    FLUENT_VECTOR_FIELD(TVanillaTask, Task);
};

////////////////////////////////////////////////////////////////////////////////

const TNode& GetJobSecureVault();

////////////////////////////////////////////////////////////////////////////////

class TRawJobContext
{
public:
    explicit TRawJobContext(size_t outputTableCount);

    const TFile& GetInputFile() const;
    const TVector<TFile>& GetOutputFileList() const;

private:
    TFile InputFile_;
    TVector<TFile> OutputFileList_;
};

////////////////////////////////////////////////////////////////////////////////

// Interface for classes that can be Saved/Loaded.
// Can be used with Y_SAVELOAD_JOB
class ISerializableForJob
{
public:
    virtual ~ISerializableForJob() = default;

    virtual void Save(IOutputStream& stream) const = 0;
    virtual void Load(IInputStream& stream) = 0;
};

////////////////////////////////////////////////////////////////////////////////

/// @brief Provider of information about operation inputs/outputs during @ref NYT::IJob::PrepareOperation.
class IOperationPreparationContext
{
public:
    virtual ~IOperationPreparationContext() = default;

    /// @brief Get the number of input tables.
    virtual int GetInputCount() const = 0;

    /// @brief Get the number of output tables.
    virtual int GetOutputCount() const = 0;

    /// @brief Get the schema of input table no. `index`.
    virtual const TTableSchema& GetInputSchema(int index) const = 0;

    /// @brief Get all the input table schemas.
    virtual const TVector<TTableSchema>& GetInputSchemas() const = 0;

    /// @brief Path to the input table if available (`Nothing()` for intermediate tables).
    virtual TMaybe<TYPath> GetInputPath(int index) const = 0;

    /// @brief Path to the output table if available (`Nothing()` for intermediate tables).
    virtual TMaybe<TYPath> GetOutputPath(int index) const = 0;
};

/// @brief Fluent builder class for @ref NYT::IJob::PrepareOperation.
///
/// @note Method calls are supposed to be chained.
class TJobOperationPreparer
{
public:

    /// @brief Group of input tables that allows to specify properties on all of them at once.
    ///
    /// The instances are created with @ref NYT::TJobOperationPreparer::StartInputGroup, not directly.
    class TInputGroup
    {
    public:
        TInputGroup(TJobOperationPreparer& preparer, TVector<int> indices);

        /// @brief Specify the type of input rows.
        template <typename TRow>
        TInputGroup& Description();

        /// @brief Specify renaming of input columns.
        TInputGroup& ColumnRenaming(const THashMap<TString, TString>& renaming);

        /// @brief Specify what input columns to send to job 
        ///
        /// @note Filter is applied before renaming, so it must specify original column names.
        TInputGroup& ColumnFilter(const TVector<TString>& columns);

        /// @brief Finish describing the input group.
        TJobOperationPreparer& EndInputGroup();

    private:
        TJobOperationPreparer& Preparer_;
        TVector<int> Indices_;
    };

    /// @brief Group of output tables that allows to specify properties on all of them at once.
    ///
    /// The instances are created with @ref NYT::TJobOperationPreparer::StartOutputGroup, not directly.
    class TOutputGroup
    {
    public:
        TOutputGroup(TJobOperationPreparer& preparer, TVector<int> indices);

        /// @brief Specify the type of output rows.
        ///
        /// @tparam TRow type of output rows from tables of this group.
        /// @param inferSchema Infer schema from `TRow` and specify it for these output tables.
        template <typename TRow>
        TOutputGroup& Description(bool inferSchema = true);

        /// @brief Specify schema for these tables.
        TOutputGroup& Schema(const TTableSchema& schema);

        /// @brief Specify that all the the tables in this group are unschematized.
        ///
        /// It is equivalent of `.Schema(TTableSchema().Strict(false)`.
        TOutputGroup& NoSchema();

        /// @brief Finish describing the output group.
        TJobOperationPreparer& EndOutputGroup();

    private:
        TJobOperationPreparer& Preparer_;
        TVector<int> Indices_;
    };

public:
    explicit TJobOperationPreparer(const IOperationPreparationContext& context);

    /// @brief Begin input group consisting of tables with indices `[begin, end)`.
    ///
    /// @param begin First index.
    /// @param end Index after the last one.
    TInputGroup BeginInputGroup(int begin, int end);

    /// @brief Begin input group consisting of tables with indices from `indices`.
    ///
    /// @tparam TCont Container with integers. Must suppport `std::begin` and `std::end` functions.
    /// @param indices Indices of tables to include in the group.
    template <typename TCont>
    TInputGroup BeginInputGroup(const TCont& indices);

    /// @brief Begin output group consisting of tables with indices `[begin, end)`.
    ///
    /// @param begin First index.
    /// @param end Index after the last one.
    TOutputGroup BeginOutputGroup(int begin, int end);

    /// @brief Begin input group consisting of tables with indices from `indices`.
    ///
    /// @tparam TCont Container with integers. Must suppport `std::begin` and `std::end` functions.
    /// @param indices Indices of tables to include in the group.
    template <typename TCont>
    TOutputGroup BeginOutputGroup(const TCont& indices);

    /// @brief Specify the schema for output table no `tableIndex`.
    ///
    /// @note All the output schemas must be specified either with this method, `NoOutputSchema` or `OutputDescription` with `inferSchema == true`
    TJobOperationPreparer& OutputSchema(int tableIndex, TTableSchema schema);

    /// @brief Mark the output table no. `tableIndex` as unschematized.
    TJobOperationPreparer& NoOutputSchema(int tableIndex);

    /// @brief Specify renaming of input columns for table no. `tableIndex`.
    TJobOperationPreparer& InputColumnRenaming(int tableIndex, const THashMap<TString, TString>& renaming);

    /// @brief Specify what input columns of table no. `tableIndex` to send to job 
    ///
    /// @note Filter is applied before renaming, so it must specify original column names.
    TJobOperationPreparer& InputColumnFilter(int tableIndex, const TVector<TString>& columns);

    /// @brief Specify the type of input rows for table no. `tableIndex`.
    ///
    /// @tparam TRow type of input rows.
    template <typename TRow>
    TJobOperationPreparer& InputDescription(int tableIndex);

    /// @brief Specify the type of output rows for table no. `tableIndex`.
    ///
    /// @tparam TRow type of output rows.
    /// @param inferSchema Infer schema from `TRow` and specify it for the output tables.
    template <typename TRow>
    TJobOperationPreparer& OutputDescription(int tableIndex, bool inferSchema = true);

    /// @brief Specify input format hints.
    ///
    /// These hints have lower priority than ones specified in spec.
    TJobOperationPreparer& InputFormatHints(TFormatHints hints);

    /// @brief Specify output format hints.
    ///
    /// These hints have lower priority than ones specified in spec.
    TJobOperationPreparer& OutputFormatHints(TFormatHints hints);

    /// @brief Specify format hints.
    ///
    /// These hints have lower priority than ones specified in spec.
    TJobOperationPreparer& FormatHints(TUserJobFormatHints newFormatHints);

    /// @name "Private" members
    /// The following methods should not be used by clients in @ref NYT::IJob::PrepareOperation
    ///@{

    /// @brief Finish the building process.
    void Finish();

    /// @brief Get output table schemas as specified by the user.
    TVector<TTableSchema> GetOutputSchemas();

    /// @brief Get input column renamings as specified by the user.
    const TVector<THashMap<TString, TString>>& GetInputColumnRenamings() const;

    /// @brief Get input column filters as specified by the user.
    const TVector<TMaybe<TVector<TString>>>& GetInputColumnFilters() const;

    /// @brief Get input column descrpiptions as specified by the user.
    const TVector<TMaybe<TTableStructure>>& GetInputDescriptions() const;

    /// @brief Get output column descriptions as specified by the user.
    const TVector<TMaybe<TTableStructure>>& GetOutputDescriptions() const;

    /// @brief Get format hints as specified by the user.
    const TUserJobFormatHints& GetFormatHints() const;

    ///@}
private:

    /// @brief Validate that schema for output table no. `tableIndex` has not been set yet.
    void ValidateMissingOutputSchema(int tableIndex) const;

    /// @brief Validate that description for input table no. `tableIndex` has not been set yet.
    void ValidateMissingInputDescription(int tableIndex) const;

    /// @brief Validate that description for output table no. `tableIndex` has not been set yet.
    void ValidateMissingOutputDescription(int tableIndex) const;

    /// @brief Validate that `tableIndex` is in correct range for input table indices.
    ///
    /// @param message Message to add to the exception in case of violation.
    void ValidateInputTableIndex(int tableIndex, TStringBuf message) const;

    /// @brief Validate that `tableIndex` is in correct range for output table indices.
    ///
    /// @param message Message to add to the exception in case of violation.
    void ValidateOutputTableIndex(int tableIndex, TStringBuf message) const;

    /// @brief Validate that all the output schemas has been set.
    void FinallyValidate() const;

    static TTableSchema EmptyNonstrictSchema();

private:
    const IOperationPreparationContext& Context_;

    TVector<TMaybe<TTableSchema>> OutputSchemas_;
    TVector<THashMap<TString, TString>> InputColumnRenamings_;
    TVector<TMaybe<TVector<TString>>> InputColumnFilters_;
    TVector<TMaybe<TTableStructure>> InputTableDescriptions_;
    TVector<TMaybe<TTableStructure>> OutputTableDescriptions_;
    TUserJobFormatHints FormatHints_ = {};
};

////////////////////////////////////////////////////////////////////////////////

class IJob
    : public TThrRefBase
{
public:
    enum EType
    {
        Mapper,
        Reducer,
        ReducerAggregator,
        RawJob,
        VanillaJob,
    };

    virtual void Save(IOutputStream& stream) const
    {
        Y_UNUSED(stream);
    }

    virtual void Load(IInputStream& stream)
    {
        Y_UNUSED(stream);
    }

    const TNode& SecureVault() const
    {
        return GetJobSecureVault();
    }

    i64 GetOutputTableCount() const
    {
        Y_VERIFY(NDetail::OutputTableCount > 0);

        return NDetail::OutputTableCount;
    }

    /// @brief Method allowing user to control some properties of input and output tables and formats.
    ///
    /// User can override this method in their job class to:
    ///   - specify output table schemas.
    ///     The most natural way is usually through @ref NYT::TJobOperationPreparer::OutputDescription (especially for protobuf),
    ///     but you can use @ref NYT::TJobOperationPreparer::OutputSchema directly
    ///   - specify output row type (@ref NYT::TJobOperationPreparer::OutputDescription)
    ///   - specify input row type (@ref NYT::TJobOperationPreparer::InputDescription)
    ///   - specify input column filter and renaming (@ref NYT::TJobOperationPreparer::InputColumnFilter and @ref NYT::TJobOperationPreparer::InputColumnRenaming)
    ///   - specify format hints (@ref NYT::TJobOperationPreparer::InputFormatHints,
    ///     NYT::TJobOperationPreparer::OutputFormatHints and @ref NYT::TJobOperationPreparer::FormatHints)
    ///   - maybe something more, cf. the methods of @ref NYT::TJobOperationPreparer.
    ///
    /// If one has several similar tables, groups can be used.
    /// Groups are delimited by @ref NYT::TJobOperationPreparer::BeginInputGroup /
    /// @ref NYT::TJobOperationPreparer::TInputGroup::EndInputGroup and
    /// @ref NYT::TJobOperationPreparer::BeginOutputGroup /
    /// @ref NYT::TJobOperationPreparer::TOutputGroup::EndOutputGroup.
    /// Example:
    /// @code{.cpp}
    ///   preparer
    ///     .BeginInputGroup({1,2,4,8})
    ///       .ColumnRenaming({{"a", "b"}, {"c", "d"}})
    ///       .ColumnFilter({"a", "c"})
    ///     .EndInputGroup();
    /// @endcode
    ///
    /// @note All the output table schemas must be set
    /// (possibly as empty nonstrict using @ref NYT::TJobOperationPreparer::NoOutputSchema or
    /// @ref NYT::TJobOperationPreparer::TOutputGroup::NoOutputSchema).
    /// By default all the output table schemas are marked as empty nonstrict.
    virtual void PrepareOperation(const IOperationPreparationContext& context, TJobOperationPreparer& preparer) const;
};

#define Y_SAVELOAD_JOB(...) \
    virtual void Save(IOutputStream& stream) const override { Save(&stream); } \
    virtual void Load(IInputStream& stream) override { Load(&stream); } \
    Y_PASS_VA_ARGS(Y_SAVELOAD_DEFINE(__VA_ARGS__));

////////////////////////////////////////////////////////////////////////////////

class IStructuredJob
    : public IJob
{
public:
    virtual TStructuredRowStreamDescription GetInputRowStreamDescription() const = 0;
    virtual TStructuredRowStreamDescription GetOutputRowStreamDescription() const = 0;
};

////////////////////////////////////////////////////////////////////////////////

class IMapperBase
    : public IStructuredJob
{ };

template <class TR, class TW>
class IMapper
    : public IMapperBase
{
public:
    static constexpr EType JobType = EType::Mapper;
    using TReader = TR;
    using TWriter = TW;

    virtual void Start(TWriter* writer)
    {
        Y_UNUSED(writer);
    }

    //
    // Each mapper job will call Do method only once.
    // Reader reader will read whole range of job input.
    virtual void Do(TReader* reader, TWriter* writer) = 0;

    virtual void Finish(TWriter* writer)
    {
        Y_UNUSED(writer);
    }

    virtual TStructuredRowStreamDescription GetInputRowStreamDescription() const override;
    virtual TStructuredRowStreamDescription GetOutputRowStreamDescription() const override;
};

////////////////////////////////////////////////////////////////////////////////

// Common base for IReducer and IAggregatorReducer
class IReducerBase
    : public IStructuredJob
{ };

template <class TR, class TW>
class IReducer
    : public IReducerBase
{
public:
    using TReader = TR;
    using TWriter = TW;

public:
    static constexpr EType JobType = EType::Reducer;

public:
    virtual void Start(TWriter* writer)
    {
        Y_UNUSED(writer);
    }

    //
    // Reduce jobs will call Do multiple times.
    // Each time Do is called reader will point to the range of records that have same ReduceBy or JoinBy key.
    virtual void Do(TReader* reader, TWriter* writer) = 0;

    virtual void Finish(TWriter* writer)
    {
        Y_UNUSED(writer);
    }

    void Break(); // do not process other keys

    virtual TStructuredRowStreamDescription GetInputRowStreamDescription() const override;
    virtual TStructuredRowStreamDescription GetOutputRowStreamDescription() const override;
};

////////////////////////////////////////////////////////////////////////////////

//
// IAggregatorReducer jobs are used inside reduce operations.
// Unlike IReduce jobs their `Do' method is called only once
// and takes whole range of records split by key boundaries.
//
// Template argument TR must be TTableRangesReader.
template <class TR, class TW>
class IAggregatorReducer
    : public IReducerBase
{
public:
    using TReader = TR;
    using TWriter = TW;

public:
    static constexpr EType JobType = EType::ReducerAggregator;

public:
    virtual void Start(TWriter* writer)
    {
        Y_UNUSED(writer);
    }

    virtual void Do(TReader* reader, TWriter* writer) = 0;

    virtual void Finish(TWriter* writer)
    {
        Y_UNUSED(writer);
    }

    virtual TStructuredRowStreamDescription GetInputRowStreamDescription() const override;
    virtual TStructuredRowStreamDescription GetOutputRowStreamDescription() const override;
};

////////////////////////////////////////////////////////////////////////////////

class IRawJob
    : public IJob
{
public:
    static constexpr EType JobType = EType::RawJob;

    virtual void Do(const TRawJobContext& jobContext) = 0;
};

class ICommandJob
    : public IJob
{
public:
    virtual const TString& GetCommand() const = 0;
};

/// @brief Raw job executing given bash command.
///
/// The binary will not be uploaded.
class TCommandRawJob
    : public IRawJob
    , public ICommandJob
{
public:
    explicit TCommandRawJob(TStringBuf command = {});

    const TString& GetCommand() const override;
    void Do(const TRawJobContext& jobContext) override;

private:
    TString Command_;
};

////////////////////////////////////////////////////////////////////////////////

class IVanillaJobBase
   : public virtual IStructuredJob
{
public:
    static constexpr EType JobType = EType::VanillaJob;
};

template <class TW = void>
class IVanillaJob;

template <>
class IVanillaJob<void>
    : public IVanillaJobBase
{
public:
    virtual void Do() = 0;

    virtual TStructuredRowStreamDescription GetInputRowStreamDescription() const override;
    virtual TStructuredRowStreamDescription GetOutputRowStreamDescription() const override;
};

/// @brief Vanilla job executing given bash command.
///
/// The binary will not be uploaded.
class TCommandVanillaJob
    : public IVanillaJob<>
    , public ICommandJob
{
public:
    explicit TCommandVanillaJob(TStringBuf command = {});

    const TString& GetCommand() const override;
    void Do() override;

private:
    TString Command_;
};

template <class TW>
class IVanillaJob
    : public IVanillaJobBase
{
public:
    using TWriter = TW;

    virtual void Start(TWriter* /* writer */)
    { }

    virtual void Do(TWriter* writer) = 0;

    virtual void Finish(TWriter* /* writer */)
    { }

    virtual TStructuredRowStreamDescription GetInputRowStreamDescription() const override;
    virtual TStructuredRowStreamDescription GetOutputRowStreamDescription() const override;
};

////////////////////////////////////////////////////////////////////////////////

enum class EOperationAttribute : int
{
    Id                /* "id" */,
    Type              /* "type" */,
    State             /* "state" */,
    AuthenticatedUser /* "authenticated_user" */,
    StartTime         /* "start_time" */,
    FinishTime        /* "finish_time" */,
    BriefProgress     /* "brief_progress" */,
    BriefSpec         /* "brief_spec" */,
    Suspended         /* "suspended" */,
    Result            /* "result" */,
    Progress          /* "progress" */,
    Events            /* "events" */,
    Spec              /* "spec" */,
    FullSpec          /* "full_spec" */,
    UnrecognizedSpec  /* "unrecognized_spec" */,
};

struct TOperationAttributeFilter
{
    using TSelf = TOperationAttributeFilter;

    TVector<EOperationAttribute> Attributes_;

    TSelf& Add(EOperationAttribute attribute)
    {
        Attributes_.push_back(attribute);
        return *this;
    }
};

struct TGetOperationOptions
{
    using TSelf = TGetOperationOptions;

    FLUENT_FIELD_OPTION(TOperationAttributeFilter, AttributeFilter);
};

enum class EOperationBriefState : int
{
    InProgress    /* "in_progress" */,
    Completed     /* "completed" */,
    Aborted       /* "aborted" */,
    Failed        /* "failed" */,
};

enum class EOperationType : int
{
    Map         /* "map" */,
    Merge       /* "merge" */,
    Erase       /* "erase" */,
    Sort        /* "sort" */,
    Reduce      /* "reduce" */,
    MapReduce   /* "map_reduce" */,
    RemoteCopy  /* "remote_copy" */,
    JoinReduce  /* "join_reduce" */,
    Vanilla     /* "vanilla" */,
};

struct TOperationProgress
{
    TJobStatistics JobStatistics;
};

struct TOperationBriefProgress
{
    ui64 Aborted = 0;
    ui64 Completed = 0;
    ui64 Failed = 0;
    ui64 Lost = 0;
    ui64 Pending = 0;
    ui64 Running = 0;
    ui64 Total = 0;
};

struct TOperationResult
{
    TMaybe<TYtError> Error;
};

struct TOperationEvent
{
    TString State;
    TInstant Time;
};

struct TOperationAttributes
{
    TMaybe<TOperationId> Id;
    TMaybe<EOperationType> Type;
    TMaybe<TString> State;
    TMaybe<EOperationBriefState> BriefState;
    TMaybe<TString> AuthenticatedUser;
    TMaybe<TInstant> StartTime;
    TMaybe<TInstant> FinishTime;
    TMaybe<TOperationBriefProgress> BriefProgress;
    TMaybe<TNode> BriefSpec;
    TMaybe<TNode> Spec;
    TMaybe<TNode> FullSpec;
    TMaybe<TNode> UnrecognizedSpec;
    TMaybe<bool> Suspended;
    TMaybe<TOperationResult> Result;
    TMaybe<TOperationProgress> Progress;
    TMaybe<TVector<TOperationEvent>> Events;
    TMaybe<THashMap<TString, TYtError>> Alerts;
};

enum class ECursorDirection
{
    Past /* "past" */,
    Future /* "future" */,
};

// https://yt.yandex-team.ru/docs/api/commands.html#list_operations
struct TListOperationsOptions
{
    using TSelf = TListOperationsOptions;

    // Search for operations with start time in half-closed interval
    // [CursorTime, ToTime) if CursorDirection == Future or
    // [FromTime, CursorTime) if CursorDirection == Past.
    FLUENT_FIELD_OPTION(TInstant, FromTime);
    FLUENT_FIELD_OPTION(TInstant, ToTime);
    FLUENT_FIELD_OPTION(TInstant, CursorTime);
    FLUENT_FIELD_OPTION(ECursorDirection, CursorDirection);

    // Choose operations satisfying given filters.
    //
    // Search for Filter as a substring in operation text factors
    // (e.g. title or input/output table paths).
    FLUENT_FIELD_OPTION(TString, Filter);
    // Choose operations whose pools include Pool.
    FLUENT_FIELD_OPTION(TString, Pool);
    // Choose operations with given user, state and type.
    FLUENT_FIELD_OPTION(TString, User);
    FLUENT_FIELD_OPTION(TString, State);
    FLUENT_FIELD_OPTION(EOperationType, Type);
    // Choose operations having (or not having) any failed jobs.
    FLUENT_FIELD_OPTION(bool, WithFailedJobs);

    // Search for operations in the archive in addition to Cypress.
    FLUENT_FIELD_OPTION(bool, IncludeArchive);

    // If set to true, include the number of operations for each pool, user, state and type
    // and the number of operations having failed jobs.
    FLUENT_FIELD_OPTION(bool, IncludeCounters);

    // Return no more than Limit operations (current default and maximum value is 100).
    FLUENT_FIELD_OPTION(i64, Limit);
};

struct TListOperationsResult
{
    TVector<TOperationAttributes> Operations;

    // If counters were requested (IncludeCounters == true)
    // the maps contain the number of operations found for each pool, user, state and type.
    // NOTE:
    //  1) Counters ignore CursorTime and CursorDirection,
    //     they always are collected in the whole [FromTime, ToTime) interval.
    //  2) Each next counter in the sequence [pool, user, state, type, with_failed_jobs]
    //     takes into account all the previous filters (i.e. if you set User filter to "some-user"
    //     type counts describe only operations with user "some-user").
    TMaybe<THashMap<TString, i64>> PoolCounts;
    TMaybe<THashMap<TString, i64>> UserCounts;
    TMaybe<THashMap<TString, i64>> StateCounts;
    TMaybe<THashMap<EOperationType, i64>> TypeCounts;
    // Number of operations having failed jobs (subject to all previous filters).
    TMaybe<i64> WithFailedJobsCount;

    // Incomplete == true means that not all operations satisisfying filters
    // were returned (limit exceeded) and you need to repeat the request with new StartTime
    // (e.g. StartTime == *Operations.back().StartTime, but don't forget to
    // remove the duplicates).
    bool Incomplete;
};

////////////////////////////////////////////////////////////////////////////////

enum class EJobSortField : int
{
    Type       /* "type" */,
    State      /* "state" */,
    StartTime  /* "start_time" */,
    FinishTime /* "finish_time" */,
    Address    /* "address" */,
    Duration   /* "duration" */,
    Progress   /* "progress" */,
    Id         /* "id" */,
};

enum class EListJobsDataSource : int
{
    Runtime  /* "runtime" */,
    Archive  /* "archive" */,
    Auto     /* "auto" */,
    Manual   /* "manual" */,
};

enum class EJobType : int
{
    SchedulerFirst    /* "scheduler_first" */,
    Map               /* "map" */,
    PartitionMap      /* "partition_map" */,
    SortedMerge       /* "sorted_merge" */,
    OrderedMerge      /* "ordered_merge" */,
    UnorderedMerge    /* "unordered_merge" */,
    Partition         /* "partition" */,
    SimpleSort        /* "simple_sort" */,
    FinalSort         /* "final_sort" */,
    SortedReduce      /* "sorted_reduce" */,
    PartitionReduce   /* "partition_reduce" */,
    ReduceCombiner    /* "reduce_combiner" */,
    RemoteCopy        /* "remote_copy" */,
    IntermediateSort  /* "intermediate_sort" */,
    OrderedMap        /* "ordered_map" */,
    JoinReduce        /* "join_reduce" */,
    Vanilla           /* "vanilla" */,
    SchedulerUnknown  /* "scheduler_unknown" */,
    SchedulerLast     /* "scheduler_last" */,
    ReplicatorFirst   /* "replicator_first" */,
    ReplicateChunk    /* "replicate_chunk" */,
    RemoveChunk       /* "remove_chunk" */,
    RepairChunk       /* "repair_chunk" */,
    SealChunk         /* "seal_chunk" */,
    ReplicatorLast    /* "replicator_last" */,
};

/// @brief Well-known task names.
enum class ETaskName : int
{
    Map               /* "map" */,
    PartitionMap0     /* "partition_map(0)" */,
    SortedMerge       /* "sorted_merge" */,
    OrderedMerge      /* "ordered_merge" */,
    UnorderedMerge    /* "unordered_merge" */,
    Partition0        /* "partition(0)" */,
    Partition1        /* "partition(1)" */,
    Partition2        /* "partition(2)" */,
    SimpleSort        /* "simple_sort" */,
    FinalSort         /* "final_sort" */,
    SortedReduce      /* "sorted_reduce" */,
    PartitionReduce   /* "partition_reduce" */,
    ReduceCombiner    /* "reduce_combiner" */,
    RemoteCopy        /* "remote_copy" */,
    IntermediateSort  /* "intermediate_sort" */,
    OrderedMap        /* "ordered_map" */,
    JoinReduce        /* "join_reduce" */,
};

class TTaskName
{
public:
    // Constructors are implicit by design.
    TTaskName(TString taskName);
    TTaskName(const char* taskName);
    TTaskName(ETaskName taskName);

    const TString& Get() const;

private:
    TString TaskName_;
};

enum class EJobState : int
{
    None       /* "none" */,
    Waiting    /* "waiting" */,
    Running    /* "running" */,
    Aborting   /* "aborting" */,
    Completed  /* "completed" */,
    Failed     /* "failed" */,
    Aborted    /* "aborted" */,
    Lost       /* "lost" */,
};

enum class EJobSortDirection : int
{
    Ascending /* "ascending" */,
    Descending /* "descending" */,
};

// https://yt.yandex-team.ru/docs/api/commands.html#list_jobs
struct TListJobsOptions
{
    using TSelf = TListJobsOptions;

    // Choose only jobs with given value of parameter (type, state, address and existence of stderr).
    // If a field is Nothing, choose jobs with all possible values of the corresponding parameter.
    FLUENT_FIELD_OPTION(EJobType, Type);
    FLUENT_FIELD_OPTION(EJobState, State);
    FLUENT_FIELD_OPTION(TString, Address);
    FLUENT_FIELD_OPTION(bool, WithStderr);
    FLUENT_FIELD_OPTION(bool, WithSpec);
    FLUENT_FIELD_OPTION(bool, WithFailContext);

    FLUENT_FIELD_OPTION(EJobSortField, SortField);
    FLUENT_FIELD_OPTION(ESortOrder, SortOrder);

    // Where to search for jobs: in scheduler and Cypress ('Runtime'), in archive ('Archive'),
    // automatically basing on operation presence in Cypress ('Auto') or choose manually (`Manual').
    FLUENT_FIELD_OPTION(EListJobsDataSource, DataSource);

    // These three options are taken into account only for `DataSource == Manual'.
    FLUENT_FIELD_OPTION(bool, IncludeCypress);
    FLUENT_FIELD_OPTION(bool, IncludeControllerAgent);
    FLUENT_FIELD_OPTION(bool, IncludeArchive);

    // Skip `Offset' first jobs and return not more than `Limit' of remaining.
    FLUENT_FIELD_OPTION(i64, Limit);
    FLUENT_FIELD_OPTION(i64, Offset);
};

struct TCoreInfo
{
    i64 ProcessId;
    TString ExecutableName;
    TMaybe<ui64> Size;
    TMaybe<TYtError> Error;
};

struct TJobAttributes
{
    TMaybe<TJobId> Id;
    TMaybe<EJobType> Type;
    TMaybe<EJobState> State;
    TMaybe<TString> Address;
    TMaybe<TInstant> StartTime;
    TMaybe<TInstant> FinishTime;
    TMaybe<double> Progress;
    TMaybe<i64> StderrSize;
    TMaybe<TYtError> Error;
    TMaybe<TNode> BriefStatistics;
    TMaybe<TVector<TRichYPath>> InputPaths;
    TMaybe<TVector<TCoreInfo>> CoreInfos;
};

struct TListJobsResult
{
    TVector<TJobAttributes> Jobs;
    TMaybe<i64> CypressJobCount;
    TMaybe<i64> ControllerAgentJobCount;
    TMaybe<i64> ArchiveJobCount;
};

////////////////////////////////////////////////////////////////////

struct TGetJobOptions
{
    using TSelf = TGetJobOptions;
};

struct TGetJobInputOptions
{
    using TSelf = TGetJobInputOptions;
};

struct TGetJobFailContextOptions
{
    using TSelf = TGetJobFailContextOptions;
};

struct TGetJobStderrOptions
{
    using TSelf = TGetJobStderrOptions;
};

////////////////////////////////////////////////////////////////////

struct TGetFailedJobInfoOptions
{
    using TSelf = TGetFailedJobInfoOptions;

    // How many jobs to download. Which jobs will be chosen is undefined.
    FLUENT_FIELD_DEFAULT(ui64, MaxJobCount, 10);

    // How much of stderr should be downloaded.
    FLUENT_FIELD_DEFAULT(ui64, StderrTailSize, 64 * 1024);
};

////////////////////////////////////////////////////////////////////////////////

struct IOperation
    : public TThrRefBase
{
    virtual ~IOperation() = default;

    //
    // Get operation id.
    virtual const TOperationId& GetId() const = 0;

    //
    // Get URL of the operation in YT Web UI.
    virtual TString GetWebInterfaceUrl() const = 0;

    //
    // Start watching operation. Return future that is set when operation is complete.
    //
    // NOTE: user should check value of returned future to ensure that operation completed successfully e.g.
    //     auto operationComplete = operation->Watch();
    //     operationComplete.Wait();
    //     operationComplete.GetValue(); // will throw if operation completed with errors
    //
    // If operation is completed successfully future contains void value.
    // If operation is completed with error future contains TOperationFailedException exception.
    // In rare cases when error occurred while waiting (e.g. YT become unavailable) future might contain other exception.
    virtual NThreading::TFuture<void> Watch() = 0;

    //
    // Retrieves information about failed jobs.
    // Can be called for operation in any stage.
    // Though user should keep in mind that this method always fetches info from cypress
    // and doesn't work when operation is archived. Successfully completed operations can be archived
    // quite quickly (in about ~30 seconds).
    virtual TVector<TFailedJobInfo> GetFailedJobInfo(const TGetFailedJobInfoOptions& options = TGetFailedJobInfoOptions()) = 0;

    // Return current operation brief state.
    virtual EOperationBriefState GetBriefState() = 0;

    //
    // Will return Nothing if operation is in 'Completed' or 'InProgress' state.
    // For failed / aborted operation will return nonempty error explaining operation fail / abort.
    virtual TMaybe<TYtError> GetError() = 0;

    //
    // Retrieve job statistics.
    virtual TJobStatistics GetJobStatistics() = 0;

    //
    // Retrieve operation progress.
    // Will return Nothing if operation has no running jobs yet, e.g. when it is materializing or has pending state.
    virtual TMaybe<TOperationBriefProgress> GetBriefProgress() = 0;

    //
    // Abort operation.
    // Operation will be finished immediately.
    // All results of completed/running jobs will be lost.
    virtual void AbortOperation() = 0;

    //
    // Complete operation.
    // Operation will be finished immediately.
    // All results of completed jobs will appear in output tables.
    // All results of running (not completed) jobs will be lost.
    virtual void CompleteOperation() = 0;

    //
    // Suspend operation.
    // Jobs will not be aborted by default, c.f. TSuspendOperationOptions.
    virtual void SuspendOperation(
        const TSuspendOperationOptions& options = TSuspendOperationOptions()) = 0;

    //
    // Resume previously suspended operation.
    virtual void ResumeOperation(
        const TResumeOperationOptions& options = TResumeOperationOptions()) = 0;

    //
    // Get operation attributes.
    virtual TOperationAttributes GetAttributes(
        const TGetOperationOptions& options = TGetOperationOptions()) = 0;

    //
    // Update operation runtime parameters.
    virtual void UpdateParameters(
        const TUpdateOperationParametersOptions& options = TUpdateOperationParametersOptions()) = 0;

    //
    // Get job attributes.
    virtual TJobAttributes GetJob(
        const TJobId& jobId,
        const TGetJobOptions& options = TGetJobOptions()) = 0;

    //
    // List jobs satisfying given filters.
    virtual TListJobsResult ListJobs(
        const TListJobsOptions& options = TListJobsOptions()) = 0;
};

struct TOperationOptions
{
    using TSelf = TOperationOptions;

    FLUENT_FIELD_OPTION(TNode, Spec);
    FLUENT_FIELD_DEFAULT(bool, Wait, true);
    FLUENT_FIELD_DEFAULT(bool, UseTableFormats, false);

    // Prefix and suffix for all kind of jobs (mapper,reducer,combiner)
    // Can be overridden for the specific job type in the TUserJobSpec
    FLUENT_FIELD(TString, JobCommandPrefix);
    FLUENT_FIELD(TString, JobCommandSuffix);

    ///
    /// @brief Put all files required by the job into tmpfs.
    ///
    /// This option can be set globaly using @ref NYT::TConfig::MountSandboxInTmpfs.
    /// @see https://yt.yandex-team.ru/docs/problems/woodpeckers
    FLUENT_FIELD_DEFAULT(bool, MountSandboxInTmpfs, false);
    FLUENT_FIELD_OPTION(TString, FileStorage);
    FLUENT_FIELD_OPTION(TNode, SecureVault);

    enum class EFileCacheMode : int
    {
        // Use YT API commands "get_file_from_cache" and "put_file_to_cache".
        ApiCommandBased,

        // Upload files to random paths inside 'FileStorage' without caching.
        CachelessRandomPathUpload,
    };

    FLUENT_FIELD_DEFAULT(EFileCacheMode, FileCacheMode, EFileCacheMode::ApiCommandBased);

    // Provides the transaction id, under which all
    // Cypress file storage entries will be checked/created.
    // By default, the global transaction is used.
    // Set a specific transaction only if you specify non-default file storage
    // path in 'FileStorage' option or in 'RemoteTempFilesDirectory' property of config.
    //
    // NOTE: this option can be set only for 'CachelessRandomPathUpload' caching mode.
    FLUENT_FIELD(TTransactionId, FileStorageTransactionId);

    // Ensure stderr, core tables exist before starting operation.
    // If set to false, it is caller's responsibility to ensure these tables exist.
    FLUENT_FIELD_DEFAULT(bool, CreateDebugOutputTables, true);

    // Ensure output tables exist before starting operation.
    // If set to false, it is caller's responsibility to ensure output tables exist.
    FLUENT_FIELD_DEFAULT(bool, CreateOutputTables, true);

    // Try to infer schema of inexistent table from the type of written rows.
    //
    // NOTE: Default values for this option may differ depending on the row type.
    // For protobuf it's currently false by default.
    FLUENT_FIELD_OPTION(bool, InferOutputSchema);
};

struct IOperationClient
{
    IOperationPtr Map(
        const TMapOperationSpec& spec,
        ::TIntrusivePtr<IMapperBase> mapper,
        const TOperationOptions& options = TOperationOptions());

    IOperationPtr Map(
        ::TIntrusivePtr<IMapperBase> mapper,
        const TOneOrMany<TStructuredTablePath>& input,
        const TOneOrMany<TStructuredTablePath>& output,
        const TMapOperationSpec& spec = TMapOperationSpec(),
        const TOperationOptions& options = TOperationOptions());

    virtual IOperationPtr RawMap(
        const TRawMapOperationSpec& spec,
        ::TIntrusivePtr<IRawJob> rawJob,
        const TOperationOptions& options = TOperationOptions()) = 0;

    IOperationPtr Reduce(
        const TReduceOperationSpec& spec,
        ::TIntrusivePtr<IReducerBase> reducer,
        const TOperationOptions& options = TOperationOptions());

    IOperationPtr Reduce(
        ::TIntrusivePtr<IReducerBase> reducer,
        const TOneOrMany<TStructuredTablePath>& input,
        const TOneOrMany<TStructuredTablePath>& output,
        const TKeyColumns& reduceBy,
        const TReduceOperationSpec& spec = TReduceOperationSpec(),
        const TOperationOptions& options = TOperationOptions());

    virtual IOperationPtr RawReduce(
        const TRawReduceOperationSpec& spec,
        ::TIntrusivePtr<IRawJob> rawJob,
        const TOperationOptions& options = TOperationOptions()) = 0;

    IOperationPtr JoinReduce(
        const TJoinReduceOperationSpec& spec,
        ::TIntrusivePtr<IReducerBase> reducer,
        const TOperationOptions& options = TOperationOptions());

    virtual IOperationPtr RawJoinReduce(
        const TRawJoinReduceOperationSpec& spec,
        ::TIntrusivePtr<IRawJob> rawJob,
        const TOperationOptions& options = TOperationOptions()) = 0;

    //
    // mapper might be nullptr in that case it's assumed to be identity mapper
    IOperationPtr MapReduce(
        const TMapReduceOperationSpec& spec,
        ::TIntrusivePtr<IMapperBase> mapper,
        ::TIntrusivePtr<IReducerBase> reducer,
        const TOperationOptions& options = TOperationOptions());

    // mapper, reduce combiner, reducer
    IOperationPtr MapReduce(
        const TMapReduceOperationSpec& spec,
        ::TIntrusivePtr<IMapperBase> mapper,
        ::TIntrusivePtr<IReducerBase> reduceCombiner,
        ::TIntrusivePtr<IReducerBase> reducer,
        const TOperationOptions& options = TOperationOptions());

    IOperationPtr MapReduce(
        ::TIntrusivePtr<IMapperBase> mapper,
        ::TIntrusivePtr<IReducerBase> reducer,
        const TOneOrMany<TStructuredTablePath>& input,
        const TOneOrMany<TStructuredTablePath>& output,
        const TKeyColumns& reduceBy,
        TMapReduceOperationSpec spec = TMapReduceOperationSpec(),
        const TOperationOptions& options = TOperationOptions());

    IOperationPtr MapReduce(
        ::TIntrusivePtr<IMapperBase> mapper,
        ::TIntrusivePtr<IReducerBase> reduceCombiner,
        ::TIntrusivePtr<IReducerBase> reducer,
        const TOneOrMany<TStructuredTablePath>& input,
        const TOneOrMany<TStructuredTablePath>& output,
        const TKeyColumns& reduceBy,
        TMapReduceOperationSpec spec = TMapReduceOperationSpec(),
        const TOperationOptions& options = TOperationOptions());

    // mapper and/or reduceCombiner may be nullptr
    virtual IOperationPtr RawMapReduce(
        const TRawMapReduceOperationSpec& spec,
        ::TIntrusivePtr<IRawJob> mapper,
        ::TIntrusivePtr<IRawJob> reduceCombiner,
        ::TIntrusivePtr<IRawJob> reducer,
        const TOperationOptions& options = TOperationOptions()) = 0;

    virtual IOperationPtr Sort(
        const TSortOperationSpec& spec,
        const TOperationOptions& options = TOperationOptions()) = 0;

    IOperationPtr Sort(
        const TOneOrMany<TRichYPath>& input,
        const TRichYPath& output,
        const TKeyColumns& sortBy,
        const TSortOperationSpec& spec = TSortOperationSpec(),
        const TOperationOptions& options = TOperationOptions());


    virtual IOperationPtr Merge(
        const TMergeOperationSpec& spec,
        const TOperationOptions& options = TOperationOptions()) = 0;

    virtual IOperationPtr Erase(
        const TEraseOperationSpec& spec,
        const TOperationOptions& options = TOperationOptions()) = 0;

    virtual IOperationPtr RemoteCopy(
        const TRemoteCopyOperationSpec& spec,
        const TOperationOptions& options = TOperationOptions()) = 0;

    virtual IOperationPtr RunVanilla(
        const TVanillaOperationSpec& spec,
        const TOperationOptions& options = TOperationOptions()) = 0;

    virtual void AbortOperation(
        const TOperationId& operationId) = 0;

    virtual void CompleteOperation(
        const TOperationId& operationId) = 0;

    virtual void WaitForOperation(
        const TOperationId& operationId) = 0;

    //
    // Checks and returns operation status.
    // NOTE: this function will never return EOperationBriefState::Failed or EOperationBriefState::Aborted status,
    // it will throw TOperationFailedError instead.
    virtual EOperationBriefState CheckOperation(
        const TOperationId& operationId) = 0;

    //
    // Creates operation object given operation id.
    // Will throw TErrorResponse exception if operation doesn't exist.
    virtual IOperationPtr AttachOperation(const TOperationId& operationId) = 0;

private:
    virtual IOperationPtr DoMap(
        const TMapOperationSpec& spec,
        const IStructuredJob& mapper,
        const TOperationOptions& options) = 0;

    virtual IOperationPtr DoReduce(
        const TReduceOperationSpec& spec,
        const IStructuredJob& reducer,
        const TOperationOptions& options) = 0;

    virtual IOperationPtr DoJoinReduce(
        const TJoinReduceOperationSpec& spec,
        const IStructuredJob& reducer,
        const TOperationOptions& options) = 0;

    virtual IOperationPtr DoMapReduce(
        const TMapReduceOperationSpec& spec,
        const IStructuredJob* mapper,
        const IStructuredJob* reduceCombiner,
        const IStructuredJob& reducer,
        const TOperationOptions& options) = 0;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

#define OPERATION_INL_H_
#include "operation-inl.h"
#undef OPERATION_INL_H_
