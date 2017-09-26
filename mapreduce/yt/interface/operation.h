#pragma once

#include "io.h"
#include "job_statistics.h"

#include <library/threading/future/future.h>
#include <util/generic/vector.h>

#ifdef __clang__
using std::nullptr_t;
#endif

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

struct TMultiFormatDesc
{
    enum EFormat {
        F_NONE,
        F_YSON,
        F_YAMR,
        F_PROTO
    };

    EFormat Format = F_NONE;
    yvector<const ::google::protobuf::Descriptor*> ProtoDescriptors;
};

struct TOperationIOSpecBase
{
    template <class T, class = void>
    struct TFormatAdder;

    yvector<TRichYPath> Inputs_;
    yvector<TRichYPath> Outputs_;

    TMultiFormatDesc InputDesc_;
    TMultiFormatDesc OutputDesc_;
};

template <class TDerived>
struct TOperationIOSpec
    : public TOperationIOSpecBase
{
    using TSelf = TDerived;

    template <class T>
    TDerived& AddInput(const TRichYPath& path);

    template <class T>
    TDerived& SetInput(size_t tableIndex, const TRichYPath& path);

    template <class T>
    TDerived& AddOutput(const TRichYPath& path);

    template <class T>
    TDerived& SetOutput(size_t tableIndex, const TRichYPath& path);

    // Ensure output tables exist before starting operation.
    // If set to false, it is caller's responsibility to ensure output tables exist.
    FLUENT_FIELD_DEFAULT(bool, CreateOutputTables, true);
};

template <class TDerived>
struct TUserOperationSpecBase
{
    using TSelf = TDerived;

    // How many jobs can fail before operation is failed.
    FLUENT_FIELD_OPTION(ui64, MaxFailedJobCount);

    // Table to save whole stderr of operation
    // https://clubs.at.yandex-team.ru/yt/1045
    FLUENT_FIELD_OPTION(TYPath, StderrTablePath);

    // Table to save coredumps of operation
    // https://clubs.at.yandex-team.ru/yt/1045
    FLUENT_FIELD_OPTION(TYPath, CoreTablePath);

    // Ensure stderr, core tables exist before starting operation.
    // If set to false, it is caller's responsibility to ensure these tables exist.
    FLUENT_FIELD_DEFAULT(bool, CreateDebugOutputTables, true);
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


    TMultiFormatDesc MapOutputHintDesc_;

    TMultiFormatDesc ReduceCombinerInputHintDesc_;
    TMultiFormatDesc ReduceCombinerOutputHintDesc_;

    TMultiFormatDesc ReduceInputHintDesc_;
};

////////////////////////////////////////////////////////////////////////////////

struct TUserJobSpec
{
    using TSelf = TUserJobSpec;

    FLUENT_VECTOR_FIELD(TLocalFilePath, LocalFile);
    FLUENT_VECTOR_FIELD(TRichYPath, File);

    //
    // MemoryLimit specifies how much memory each job can use.
    // Expected tmpfs size should NOT be included.
    //
    // ExtraTmpfsSize is meaningful if MountSandboxInTmpfs is set.
    // By default tmpfs size is set to the sum of sizes of all files that
    // are loaded into tmpfs before job started.
    // If job wants to save some data into tmpfs it can ask for extra tmpfs space using
    // ExtraTmpfsSize option.
    //
    // Final memory memory_limit and tmpfs_size that are passed to YT are calculated
    // as follows:
    //
    // tmpfs_size = size_of_binary + size_of_required_files + ExtraTmpfsSize
    // memory_limit = MemoryLimit + tmpfs_size
    FLUENT_FIELD_OPTION(i64, MemoryLimit);
    FLUENT_FIELD_OPTION(i64, ExtraTmpfsSize);

    FLUENT_FIELD_OPTION(TString, JobBinary);
};

////////////////////////////////////////////////////////////////////////////////

struct TMapOperationSpec
    : public TOperationIOSpec<TMapOperationSpec>
    , public TUserOperationSpecBase<TMapOperationSpec>
{
    using TSelf = TMapOperationSpec;

    FLUENT_FIELD(TUserJobSpec, MapperSpec);
    FLUENT_FIELD_OPTION(bool, Ordered);

    // `JobCount' and `DataSizePerJob' options affect how many jobs will be launched.
    // These options only provide recommendations and YT might ignore them if they conflict with YT internal limits.
    // `JobCount' has higher priority than `DataSizePerJob'.
    FLUENT_FIELD_OPTION(ui32, JobCount);
    FLUENT_FIELD_OPTION(ui64, DataSizePerJob);
};

struct TReduceOperationSpec
    : public TOperationIOSpec<TReduceOperationSpec>
    , public TUserOperationSpecBase<TReduceOperationSpec>
{
    using TSelf = TReduceOperationSpec;

    FLUENT_FIELD(TUserJobSpec, ReducerSpec);
    FLUENT_FIELD(TKeyColumns, SortBy);
    FLUENT_FIELD(TKeyColumns, ReduceBy);
    FLUENT_FIELD_OPTION(TKeyColumns, JoinBy);

    // Similar to corresponding options in `TMapOperationSpec'.
    FLUENT_FIELD_OPTION(ui32, JobCount);
    FLUENT_FIELD_OPTION(ui64, DataSizePerJob);
};

struct TMapReduceOperationSpec
    : public TOperationIOSpec<TMapReduceOperationSpec>
    , public TUserOperationSpecBase<TMapReduceOperationSpec>
    , public TIntermediateTablesHintSpec<TMapReduceOperationSpec>
{
    using TSelf = TMapReduceOperationSpec;

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
};

struct TJoinReduceOperationSpec
    : public TOperationIOSpec<TJoinReduceOperationSpec>
    , public TUserOperationSpecBase<TJoinReduceOperationSpec>
{
    using TSelf = TJoinReduceOperationSpec;

    FLUENT_FIELD(TUserJobSpec, ReducerSpec);
    FLUENT_FIELD(TKeyColumns, JoinBy);

    // Similar to corresponding options in `TMapOperationSpec'.
    FLUENT_FIELD_OPTION(ui32, JobCount);
    FLUENT_FIELD_OPTION(ui64, DataSizePerJob);
};

////////////////////////////////////////////////////////////////////////////////

struct TSortOperationSpec
{
    using TSelf = TSortOperationSpec;

    FLUENT_VECTOR_FIELD(TRichYPath, Input);
    FLUENT_FIELD(TRichYPath, Output);
    FLUENT_FIELD(TKeyColumns, SortBy);

    FLUENT_FIELD_OPTION(ui64, PartitionCount);
    FLUENT_FIELD_OPTION(ui64, PartitionDataSize);

    FLUENT_FIELD_OPTION(ui64, PartitionJobCount);
    FLUENT_FIELD_OPTION(ui64, DataSizePerPartitionJob);
};

enum EMergeMode : int
{
    MM_UNORDERED    /* "unordered" */,
    MM_ORDERED      /* "ordered" */,
    MM_SORTED       /* "sorted" */,
};

struct TMergeOperationSpec
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
};

struct TEraseOperationSpec
{
    using TSelf = TEraseOperationSpec;

    FLUENT_FIELD(TRichYPath, TablePath);
    FLUENT_FIELD_DEFAULT(bool, CombineChunks, false);
};

////////////////////////////////////////////////////////////////////////////////

const TNode& GetJobSecureVault();

////////////////////////////////////////////////////////////////////////////////

void Initialize(int argc, const char* argv[]);

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

class IJob
    : public TThrRefBase
{
private:
    friend struct IOperationClient;
    virtual void CheckInputFormat(const char* jobName, const TMultiFormatDesc& desc) = 0;
    virtual void CheckOutputFormat(const char* jobName, const TMultiFormatDesc& desc) = 0;
    virtual void AddInputFormatDescription(TMultiFormatDesc* desc) = 0;
    virtual void AddOutputFormatDescription(TMultiFormatDesc* desc) = 0;

public:
    enum EType {
        Mapper,
        Reducer,
        ReducerAggregator,
    };

    virtual void Save(IOutputStream& stream) const
    {
        Y_UNUSED(stream);
    }

    virtual void Load(IInputStream& stream)
    {
        Y_UNUSED(stream);
    }

    const TNode& SecureVault() const {
        return GetJobSecureVault();
    }
};

#define Y_SAVELOAD_JOB(...) \
    virtual void Save(IOutputStream& stream) const override { Save(&stream); } \
    virtual void Load(IInputStream& stream) override { Load(&stream); } \
    Y_PASS_VA_ARGS(Y_SAVELOAD_DEFINE(__VA_ARGS__));

class IMapperBase
    : public IJob
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

    virtual void Do(TReader* reader, TWriter* writer) = 0;

    virtual void Finish(TWriter* writer)
    {
        Y_UNUSED(writer);
    }

private:
    virtual void CheckInputFormat(const char* jobName, const TMultiFormatDesc& desc) override;
    virtual void CheckOutputFormat(const char* jobName, const TMultiFormatDesc& desc) override;
    virtual void AddInputFormatDescription(TMultiFormatDesc* desc) override;
    virtual void AddOutputFormatDescription(TMultiFormatDesc* desc) override;
};

// Common base for IReducer and IAggregatorReducer
class IReducerBase
    : public IJob
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

    virtual void Do(TReader* reader, TWriter* writer) = 0;

    virtual void Finish(TWriter* writer)
    {
        Y_UNUSED(writer);
    }

    void Break(); // do not process other keys

private:
    virtual void CheckInputFormat(const char* jobName, const TMultiFormatDesc& desc) override;
    virtual void CheckOutputFormat(const char* jobName, const TMultiFormatDesc& desc) override;
    virtual void AddInputFormatDescription(TMultiFormatDesc* desc) override;
    virtual void AddOutputFormatDescription(TMultiFormatDesc* desc) override;
};

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

private:
    virtual void CheckInputFormat(const char* jobName, const TMultiFormatDesc& desc) override;
    virtual void CheckOutputFormat(const char* jobName, const TMultiFormatDesc& desc) override;
    virtual void AddInputFormatDescription(TMultiFormatDesc* desc) override;
    virtual void AddOutputFormatDescription(TMultiFormatDesc* desc) override;
};

////////////////////////////////////////////////////////////////////////////////

enum EOperationStatus : int
{
    OS_IN_PROGRESS,
    OS_COMPLETED,
    OS_ABORTED,
    OS_FAILED,
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

////////////////////////////////////////////////////////////////////////////////

struct IOperation
    : public TThrRefBase
{
    virtual ~IOperation() = default;

    //
    // Get operation id.
    virtual const TOperationId& GetId() const = 0;

    //
    // Start watching operation. Return future that is set when operation is complete.
    //
    // NOTE: user should check value of returned future to ensure that operation completed successfuly e.g.
    //     auto operationComplete = operation->Watch();
    //     operationComplete.Wait();
    //     operationComplete.GetValue(); // will throw if operation completed with errors
    //
    // If operation is completed successfuly future contains void value.
    // If operation is completed with error future contains TOperationFailedException exeption.
    // In rare cases when error occurred while waiting (e.g. YT become unavailable) future might contain other exception.
    virtual NThreading::TFuture<void> Watch() = 0;

    //
    // Retrieves information about failed jobs.
    // Can be called for operation in any stage.
    // Though user should keep in mind that this method always fetches info from cypress
    // and doesn't work when operation is archived. Succesfully completed operations can be archived
    // quite quickly (in about ~30 seconds).
    virtual yvector<TFailedJobInfo> GetFailedJobInfo(const TGetFailedJobInfoOptions& options = TGetFailedJobInfoOptions()) = 0;

    //
    // Return current operation status.
    virtual EOperationStatus GetStatus() = 0;

    //
    // Will return Nothing if operation is in OS_COMPLETED or OS_IN_PROGREES state.
    // For failed / aborted operation will return nonempty error explaining operation fail / abort.
    virtual TMaybe<TYtError> GetError() = 0;

    //
    // Retrieve job statistics.
    virtual TJobStatistics GetJobStatistics() = 0;

    //
    // Retrieve operation progress.
    // Will return Nothing if operation has no running jobs yet, f.e. when it is materializing or has pending state
    virtual TMaybe<TOperationBriefProgress> GetBriefProgress() = 0;

    //
    // Abort operation.
    virtual void AbortOperation() = 0;
};

struct TOperationOptions
{
    using TSelf = TOperationOptions;

    FLUENT_FIELD_OPTION(TNode, Spec);
    FLUENT_FIELD_DEFAULT(bool, Wait, true);
    FLUENT_FIELD_DEFAULT(bool, UseTableFormats, false);
    FLUENT_FIELD(TString, JobCommandPrefix);
    FLUENT_FIELD(TString, JobCommandSuffix);

    //
    // If MountSandboxInTmpfs is set all files required by job will be put into tmpfs.
    // The same can be done with TConfig::MountSandboxInTmpfs option.
    FLUENT_FIELD_DEFAULT(bool, MountSandboxInTmpfs, false);
    FLUENT_FIELD_OPTION(TString, FileStorage);
    FLUENT_FIELD_OPTION(TNode, SecureVault);

    // Provides the transaction id, under which all
    // Cypress file storage entries will be checked/created.
    // By default, the global transaction is used.
    // Set a specific transaction only if you specify non-default file storage
    // path in 'FileStorage' option or in 'RemoteTempFilesDirectory'
    // property of config.
    FLUENT_FIELD(TTransactionId, FileStorageTransactionId);
};

struct IOperationClient
{
    IOperationPtr Map(
        const TMapOperationSpec& spec,
        ::TIntrusivePtr<IMapperBase> mapper,
        const TOperationOptions& options = TOperationOptions());

    IOperationPtr Reduce(
        const TReduceOperationSpec& spec,
        ::TIntrusivePtr<IReducerBase> reducer,
        const TOperationOptions& options = TOperationOptions());

    IOperationPtr JoinReduce(
        const TJoinReduceOperationSpec& spec,
        ::TIntrusivePtr<IReducerBase> reducer,
        const TOperationOptions& options = TOperationOptions());

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

    virtual IOperationPtr Sort(
        const TSortOperationSpec& spec,
        const TOperationOptions& options = TOperationOptions()) = 0;

    virtual IOperationPtr Merge(
        const TMergeOperationSpec& spec,
        const TOperationOptions& options = TOperationOptions()) = 0;

    virtual IOperationPtr Erase(
        const TEraseOperationSpec& spec,
        const TOperationOptions& options = TOperationOptions()) = 0;


    virtual void AbortOperation(
        const TOperationId& operationId) = 0;

    virtual void WaitForOperation(
        const TOperationId& operationId) = 0;

    //
    // Checks and returns operation status.
    // NOTE: this function will never return OS_FAILED or OS_ABORTED status,
    // it will throw TOperationFailedError instead.
    virtual EOperationStatus CheckOperation(
        const TOperationId& operationId) = 0;

private:
    virtual IOperationPtr DoMap(
        const TMapOperationSpec& spec,
        IJob* mapper,
        const TOperationOptions& options) = 0;

    virtual IOperationPtr DoReduce(
        const TReduceOperationSpec& spec,
        IJob* reducer,
        const TOperationOptions& options) = 0;

    virtual IOperationPtr DoJoinReduce(
        const TJoinReduceOperationSpec& spec,
        IJob* reducer,
        const TOperationOptions& options) = 0;

    virtual IOperationPtr DoMapReduce(
        const TMapReduceOperationSpec& spec,
        IJob* mapper,
        IJob* reduceCombiner,
        IJob* reducer,
        const TMultiFormatDesc& outputMapperDesc,
        const TMultiFormatDesc& inputReduceCombinerDesc,
        const TMultiFormatDesc& outputReduceCombinerDesc,
        const TMultiFormatDesc& inputReducerDesc,
        const TOperationOptions& options) = 0;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

#define OPERATION_INL_H_
#include "operation-inl.h"
#undef OPERATION_INL_H_
