#pragma once

#include "io.h"

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
    template <class T>
    TDerived& AddInput(const TRichYPath& path);

    template <class T>
    TDerived& SetInput(size_t tableIndex, const TRichYPath& path);

    template <class T>
    TDerived& AddOutput(const TRichYPath& path);

    template <class T>
    TDerived& SetOutput(size_t tableIndex, const TRichYPath& path);
};

////////////////////////////////////////////////////////////////////////////////

struct TUserJobSpec
{
    using TSelf = TUserJobSpec;

    FLUENT_VECTOR_FIELD(TLocalFilePath, LocalFile);
    FLUENT_VECTOR_FIELD(TRichYPath, File);
    FLUENT_FIELD_OPTION(i64, MemoryLimit);
    FLUENT_FIELD_OPTION(i64, ExtraTmpfsSize);
    FLUENT_FIELD_OPTION(Stroka, JobBinary);
};

struct TMapOperationSpec
    : public TOperationIOSpec<TMapOperationSpec>
{
    using TSelf = TMapOperationSpec;

    FLUENT_FIELD(TUserJobSpec, MapperSpec);
    FLUENT_FIELD_OPTION(bool, Ordered);
};

struct TReduceOperationSpec
    : public TOperationIOSpec<TReduceOperationSpec>
{
    using TSelf = TReduceOperationSpec;

    FLUENT_FIELD(TUserJobSpec, ReducerSpec);
    FLUENT_FIELD(TKeyColumns, SortBy);
    FLUENT_FIELD(TKeyColumns, ReduceBy);
    FLUENT_FIELD_OPTION(TKeyColumns, JoinBy);
};

struct TMapReduceOperationSpec
    : public TOperationIOSpec<TMapReduceOperationSpec>
{
    using TSelf = TMapReduceOperationSpec;

    FLUENT_FIELD(TUserJobSpec, MapperSpec);
    FLUENT_FIELD(TUserJobSpec, ReducerSpec);
    FLUENT_FIELD(TUserJobSpec, ReduceCombinerSpec);
    FLUENT_FIELD(TKeyColumns, SortBy);
    FLUENT_FIELD(TKeyColumns, ReduceBy);
};

struct TJoinReduceOperationSpec
    : public TOperationIOSpec<TJoinReduceOperationSpec>
{
    using TSelf = TJoinReduceOperationSpec;

    FLUENT_FIELD(TUserJobSpec, ReducerSpec);
    FLUENT_FIELD(TKeyColumns, JoinBy);
};

////////////////////////////////////////////////////////////////////////////////

struct TSortOperationSpec
{
    using TSelf = TSortOperationSpec;

    FLUENT_VECTOR_FIELD(TRichYPath, Input);
    FLUENT_FIELD(TRichYPath, Output);
    FLUENT_FIELD(TKeyColumns, SortBy);
};

enum EMergeMode
{
    MM_UNORDERED,
    MM_ORDERED,
    MM_SORTED
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

class IJob
    : public TThrRefBase
{
public:
    enum EType {
        Mapper,
        Reducer,
        ReducerAggregator,
    };

    virtual void Save(TOutputStream& stream) const
    {
        Y_UNUSED(stream);
    }

    virtual void Load(TInputStream& stream)
    {
        Y_UNUSED(stream);
    }

    const TNode& SecureVault() const {
        return GetJobSecureVault();
    }
};

#define Y_SAVELOAD_JOB(...) \
    virtual void Save(TOutputStream& stream) const override { Save(&stream); } \
    virtual void Load(TInputStream& stream) override { Load(&stream); } \
    Y_PASS_VA_ARGS(Y_SAVELOAD_DEFINE(__VA_ARGS__));

template <class TR, class TW>
class IMapper
    : public IJob
{
public:
    static const EType JobType = EType::Mapper;
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
};

template <class TR, class TW>
class IReducer
    : public IJob
{
public:
    static const EType JobType = EType::Reducer;
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

    void Break(); // do not process other keys
};

//
// IAggregatorReducer jobs are used inside reduce operations.
// Unlike IReduce jobs their `Do' method is called only once
// and takes whole range of records split by key boundaries.
//
// Template argument TR must be TTableRangesReader.
template <class TR, class TW>
class IAggregatorReducer
    : public IJob
{
public:
    static const EType JobType = EType::ReducerAggregator;

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
};

////////////////////////////////////////////////////////////////////////////////

enum EOperationStatus
{
    OS_RUNNING,
    OS_COMPLETED
};

struct TOperationOptions
{
    using TSelf = TOperationOptions;

    FLUENT_FIELD_OPTION(TNode, Spec);
    FLUENT_FIELD_DEFAULT(bool, Wait, true);
    FLUENT_FIELD_DEFAULT(bool, UseTableFormats, false);
    FLUENT_FIELD(Stroka, JobCommandPrefix);
    FLUENT_FIELD(Stroka, JobCommandSuffix);
    FLUENT_FIELD_DEFAULT(bool, MountSandboxInTmpfs, false);
    FLUENT_FIELD_OPTION(Stroka, FileStorage);
    FLUENT_FIELD_OPTION(TNode, SecureVault);
};

struct IOperationClient
{
    template <class TMapper>
    TOperationId Map(
        const TMapOperationSpec& spec,
        TMapper* mapper,
        const TOperationOptions& options = TOperationOptions());

    template <class TReducer>
    TOperationId Reduce(
        const TReduceOperationSpec& spec,
        TReducer* reducer,
        const TOperationOptions& options = TOperationOptions());

    template <class TReducer>
    TOperationId JoinReduce(
        const TJoinReduceOperationSpec& spec,
        TReducer* reducer,
        const TOperationOptions& options = TOperationOptions());

    // mapper, reducer
    template <class TMapper, class TReducer>
    TOperationId MapReduce(
        const TMapReduceOperationSpec& spec,
        TMapper* mapper,
        TReducer* reducer,
        const TOperationOptions& options = TOperationOptions());

    // identity mapper, reducer
    template <class TReducer>
    TOperationId MapReduce(
        const TMapReduceOperationSpec& spec,
        nullptr_t,
        TReducer* reducer,
        const TOperationOptions& options = TOperationOptions());

    // mapper, reduce combiner, reducer
    template <class TMapper, class TReduceCombiner, class TReducer>
    TOperationId MapReduce(
        const TMapReduceOperationSpec& spec,
        TMapper* mapper,
        TReduceCombiner* reduceCombiner,
        TReducer* reducer,
        const TOperationOptions& options = TOperationOptions());

    // identity mapper, reduce combiner, reducer
    template <class TReduceCombiner, class TReducer>
    TOperationId MapReduce(
        const TMapReduceOperationSpec& spec,
        nullptr_t,
        TReduceCombiner* reduceCombiner,
        TReducer* reducer,
        const TOperationOptions& options = TOperationOptions());


    virtual TOperationId Sort(
        const TSortOperationSpec& spec,
        const TOperationOptions& options = TOperationOptions()) = 0;

    virtual TOperationId Merge(
        const TMergeOperationSpec& spec,
        const TOperationOptions& options = TOperationOptions()) = 0;

    virtual TOperationId Erase(
        const TEraseOperationSpec& spec,
        const TOperationOptions& options = TOperationOptions()) = 0;


    virtual void AbortOperation(
        const TOperationId& operationId) = 0;

    virtual void WaitForOperation(
        const TOperationId& operationId) = 0;

    virtual EOperationStatus CheckOperation(
        const TOperationId& operationId) = 0;

private:
    virtual TOperationId DoMap(
        const TMapOperationSpec& spec,
        IJob* mapper,
        const TOperationOptions& options) = 0;

    virtual TOperationId DoReduce(
        const TReduceOperationSpec& spec,
        IJob* reducer,
        const TOperationOptions& options) = 0;

    virtual TOperationId DoJoinReduce(
        const TJoinReduceOperationSpec& spec,
        IJob* reducer,
        const TOperationOptions& options) = 0;

    virtual TOperationId DoMapReduce(
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
