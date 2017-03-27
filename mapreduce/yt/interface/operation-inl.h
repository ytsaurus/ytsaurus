#pragma once

#ifndef OPERATION_INL_H_
#error "Direct inclusion of this file is not allowed, use operation.h"
#endif
#undef OPERATION_INL_H_

#include <util/generic/bt_exception.h>
#include <util/generic/singleton.h>
#include <util/generic/type_name.h>

#include <util/stream/file.h>
#include <util/stream/buffer.h>
#include <util/string/subst.h>
#include <util/string/printf.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

::TIntrusivePtr<INodeReaderImpl> CreateJobNodeReader();
::TIntrusivePtr<IYaMRReaderImpl> CreateJobYaMRReader();
::TIntrusivePtr<IProtoReaderImpl> CreateJobProtoReader();

::TIntrusivePtr<INodeWriterImpl> CreateJobNodeWriter(size_t outputTableCount);
::TIntrusivePtr<IYaMRWriterImpl> CreateJobYaMRWriter(size_t outputTableCount);
::TIntrusivePtr<IProtoWriterImpl> CreateJobProtoWriter(size_t outputTableCount);

////////////////////////////////////////////////////////////////////////////////

template <class T>
inline ::TIntrusivePtr<typename TRowTraits<T>::IReaderImpl> CreateJobReaderImpl();

template <>
inline ::TIntrusivePtr<INodeReaderImpl> CreateJobReaderImpl<TNode>()
{
    return CreateJobNodeReader();
}

template <>
inline ::TIntrusivePtr<IYaMRReaderImpl> CreateJobReaderImpl<TYaMRRow>()
{
    return CreateJobYaMRReader();
}

template <>
inline ::TIntrusivePtr<IProtoReaderImpl> CreateJobReaderImpl<Message>()
{
    return CreateJobProtoReader();
}

template <class T>
inline ::TIntrusivePtr<typename TRowTraits<T>::IReaderImpl> CreateJobReaderImpl()
{
    return CreateJobProtoReader();
}

template <class T>
inline TTableReaderPtr<T> CreateJobReader()
{
    return new TTableReader<T>(CreateJobReaderImpl<T>());
}

////////////////////////////////////////////////////////////////////////////////

template <class T>
TTableWriterPtr<T> CreateJobWriter(size_t outputTableCount);

template <>
inline TTableWriterPtr<TNode> CreateJobWriter<TNode>(size_t outputTableCount)
{
    return new TTableWriter<TNode>(CreateJobNodeWriter(outputTableCount));
}

template <>
inline TTableWriterPtr<TYaMRRow> CreateJobWriter<TYaMRRow>(size_t outputTableCount)
{
    return new TTableWriter<TYaMRRow>(CreateJobYaMRWriter(outputTableCount));
}

template <>
inline TTableWriterPtr<Message> CreateJobWriter<Message>(size_t outputTableCount)
{
    return new TTableWriter<Message>(CreateJobProtoWriter(outputTableCount));
}

template <class T, class = void>
struct TProtoWriterCreator;

template <class T>
struct TProtoWriterCreator<T, std::enable_if_t<TIsBaseOf<Message, T>::Value>>
{
    static TTableWriterPtr<T> Create(::TIntrusivePtr<IProtoWriterImpl> writer)
    {
        return new TTableWriter<T>(writer);
    }
};

template <class T>
inline TTableWriterPtr<T> CreateJobWriter(size_t outputTableCount)
{
    return TProtoWriterCreator<T>::Create(CreateJobProtoWriter(outputTableCount));
}

////////////////////////////////////////////////////////////////////////////////

template <class T, class = void>
struct TFormatDescTraits;

template <>
struct TFormatDescTraits<TNode>
{
    static const TMultiFormatDesc::EFormat Format = TMultiFormatDesc::F_YSON;
};

template <>
struct TFormatDescTraits<TYaMRRow>
{
    static const TMultiFormatDesc::EFormat Format = TMultiFormatDesc::F_YAMR;
};

template <>
struct TFormatDescTraits<Message>
{
    static const TMultiFormatDesc::EFormat Format = TMultiFormatDesc::F_PROTO;
};

template <class T>
struct TFormatDescTraits<T, std::enable_if_t<TIsBaseOf<Message, T>::Value>>
{
    static const TMultiFormatDesc::EFormat Format = TMultiFormatDesc::F_PROTO;
};


template <class T, class TEnable>
struct TOperationIOSpecBase::TFormatAdder
{
    static void Add(TMultiFormatDesc& desc)
    {
        if (desc.Format != TMultiFormatDesc::F_NONE &&
            desc.Format != TFormatDescTraits<T>::Format) {
            ythrow yexception() << "Invalid format"; // TODO: more info
        }
        desc.Format = TFormatDescTraits<T>::Format;
    }
};

template <class T>
struct TOperationIOSpecBase::TFormatAdder<T, std::enable_if_t<TIsBaseOf<Message, T>::Value>>
{
    static void Add(TMultiFormatDesc& desc)
    {
        if (desc.Format != TMultiFormatDesc::F_NONE &&
            desc.Format != TMultiFormatDesc::F_PROTO) {
            ythrow yexception() << "Invalid format"; // TODO: more info
        }
        desc.Format = TMultiFormatDesc::F_PROTO;
        desc.ProtoDescriptors.push_back(T::descriptor());
    }
};

template <class TDerived>
template <class T>
TDerived& TOperationIOSpec<TDerived>::AddInput(const TRichYPath& path)
{
    TOperationIOSpecBase::TFormatAdder<T>::Add(InputDesc_);
    Inputs_.push_back(path);
    return *static_cast<TDerived*>(this);
}

template <class TDerived>
template <class T>
TDerived& TOperationIOSpec<TDerived>::SetInput(size_t tableIndex, const TRichYPath& path)
{
    TOperationIOSpecBase::TFormatAdder<T>::Add(InputDesc_);
    Inputs_.resize(std::max(Inputs_.size(), tableIndex + 1));
    Inputs_[tableIndex] = path;
    return *static_cast<TDerived*>(this);
}


template <class TDerived>
template <class T>
TDerived& TOperationIOSpec<TDerived>::AddOutput(const TRichYPath& path)
{
    TOperationIOSpecBase::TFormatAdder<T>::Add(OutputDesc_);
    Outputs_.push_back(path);
    return *static_cast<TDerived*>(this);
}

template <class TDerived>
template <class T>
TDerived& TOperationIOSpec<TDerived>::SetOutput(size_t tableIndex, const TRichYPath& path)
{
    TOperationIOSpecBase::TFormatAdder<T>::Add(OutputDesc_);
    Outputs_.resize(std::max(Outputs_.size(), tableIndex + 1));
    Outputs_[tableIndex] = path;
    return *static_cast<TDerived*>(this);
}

////////////////////////////////////////////////////////////////////////////////

struct TReducerContext
{
    bool Break = false;
    static TReducerContext* Get() { return Singleton<TReducerContext>(); }
};

template <class TR, class TW>
inline void IReducer<TR, TW>::Break()
{
    TReducerContext::Get()->Break = true;
}

template <typename TReader, typename TWriter>
void FeedJobInput(
    IMapper<TReader, TWriter>* mapper,
    typename TRowTraits<typename TReader::TRowType>::IReaderImpl* readerImpl,
    TWriter* writer)
{
    using TInputRow = typename TReader::TRowType;

    auto reader = MakeIntrusive<TTableReader<TInputRow>>(readerImpl);
    mapper->Do(reader.Get(), writer);
}

template <typename TReader, typename TWriter>
void FeedJobInput(
    IReducer<TReader, TWriter>* reducer,
    typename TRowTraits<typename TReader::TRowType>::IReaderImpl* readerImpl,
    TWriter* writer)
{
    using TInputRow = typename TReader::TRowType;

    auto rangesReader = MakeIntrusive<TTableRangesReader<TInputRow>>(readerImpl);
    for (; rangesReader->IsValid(); rangesReader->Next()) {
        reducer->Do(&rangesReader->GetRange(), writer);
        if (TReducerContext::Get()->Break) {
            break;
        }
    }
}

template <typename TReader, typename TWriter>
void FeedJobInput(
    IAggregatorReducer<TReader, TWriter>* reducer,
    typename TRowTraits<typename TReader::TRowType>::IReaderImpl* readerImpl,
    TWriter* writer)
{
    using TInputRow = typename TReader::TRowType;

    auto rangesReader = MakeIntrusive<TTableRangesReader<TInputRow>>(readerImpl);
    reducer->Do(rangesReader.Get(), writer);
}

template <class TJob>
int RunJob(size_t outputTableCount, TInputStream& jobStateStream)
{
    using TInputRow = typename TJob::TReader::TRowType;
    using TOutputRow = typename TJob::TWriter::TRowType;

    auto readerImpl = CreateJobReaderImpl<TInputRow>();

    // Many users don't expect to have jobs with empty input so we skip such jobs.
    if (!readerImpl->IsValid()) {
        return 0;
    }

    auto writer = CreateJobWriter<TOutputRow>(outputTableCount);

    auto job = MakeIntrusive<TJob>();
    job->Load(jobStateStream);

    job->Start(writer.Get());
    FeedJobInput(job.Get(), readerImpl.Get(), writer.Get());
    job->Finish(writer.Get());

    writer->Finish();

    return 0;
}

//
// We leave RunMapJob/RunReduceJob/RunAggregatorReducer for backward compatibility,
// some user use them already. :(

template <class TMapper>
int RunMapJob(size_t outputTableCount, TInputStream& jobStateStream)
{
    return RunJob<TMapper>(outputTableCount, jobStateStream);
}

template <class TReducer>
int RunReduceJob(size_t outputTableCount, TInputStream& jobStateStream)
{
    return RunJob<TReducer>(outputTableCount, jobStateStream);
}

template <class TReducer>
int RunAggregatorReducer(size_t outputTableCount, TInputStream& jobStateStream)
{
    return RunJob<TReducer>(outputTableCount, jobStateStream);
}

////////////////////////////////////////////////////////////////////////////////

using TJobFunction = int (*)(size_t, TInputStream&);

class TJobFactory
{
public:
    static TJobFactory* Get()
    {
        return Singleton<TJobFactory>();
    }

    template <class TJob>
    void RegisterJob(const char* name)
    {
        const auto* typeInfoPtr = &typeid(TJob);
        CheckNotRegistered(typeInfoPtr, name);
        JobNames[typeInfoPtr] = name;
        JobFunctions[name] = RunJob<TJob>;
    }

    Stroka GetJobName(IJob* job)
    {
        const auto* typeInfoPtr = &typeid(*job);
        CheckJobRegistered(typeInfoPtr);
        return JobNames[typeInfoPtr];
    }

    TJobFunction GetJobFunction(const char* name)
    {
        CheckNameRegistered(name);
        return JobFunctions[name];
    }

private:
    yhash_map<const std::type_info*, Stroka> JobNames;
    yhash_map<Stroka, TJobFunction> JobFunctions;

    void CheckNotRegistered(const std::type_info* typeInfoPtr, const char* name)
    {
        if (JobNames.find(typeInfoPtr) != JobNames.end()) {
            ythrow yexception() <<
                Sprintf("type_info '%s' is already registered under name '%s'",
                    typeInfoPtr->name(), ~JobNames[typeInfoPtr]);
        }
        if (JobFunctions.find(name) != JobFunctions.end()) {
            ythrow yexception() <<
                Sprintf("job with name '%s' is already registered",
                    name);
        }
    }

    void CheckJobRegistered(const std::type_info* typeInfoPtr)
    {
        if (JobNames.find(typeInfoPtr) == JobNames.end()) {
            ythrow yexception() <<
                Sprintf("type_info '%s' is not registered, use REGISTER_* macros",
                    typeInfoPtr->name());
        }
    }

    void CheckNameRegistered(const char* name)
    {
        if (JobFunctions.find(name) == JobFunctions.end()) {
            ythrow yexception() <<
                Sprintf("job with name '%s' is not registered, use REGISTER_* macros",
                    name);
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

template <class TMapper>
struct TMapperRegistrator
{
    TMapperRegistrator(const char* name)
    {
        static_assert(TMapper::JobType == IJob::EType::Mapper,
            "REGISTER_MAPPER is not compatible with this job class");

        NYT::TJobFactory::Get()->RegisterJob<TMapper>(name);
    }
};

template <class TReducer>
struct TReducerRegistrator
{
    TReducerRegistrator(const char* name)
    {
        static_assert(TReducer::JobType == IJob::EType::Reducer ||
            TReducer::JobType == IJob::EType::ReducerAggregator,
            "REGISTER_REDUCER is not compatible with this job class");

        NYT::TJobFactory::Get()->RegisterJob<TReducer>(name);
    }
};

inline Stroka YtRegistryTypeName(const Stroka& name) {
    Stroka res = name;
#ifdef _win_
    SubstGlobal(res, "class ", "");
#endif
    return res;
}

#define REGISTER_MAPPER(...) \
static NYT::TMapperRegistrator<__VA_ARGS__> \
Y_GENERATE_UNIQUE_ID(TJobRegistrator)(~NYT::YtRegistryTypeName(TypeName<__VA_ARGS__>()));

#define REGISTER_NAMED_MAPPER(name, ...) \
static NYT::TMapperRegistrator<__VA_ARGS__> \
Y_GENERATE_UNIQUE_ID(TJobRegistrator)(name);

#define REGISTER_REDUCER(...) \
static NYT::TReducerRegistrator<__VA_ARGS__> \
Y_GENERATE_UNIQUE_ID(TJobRegistrator)(~NYT::YtRegistryTypeName(TypeName<__VA_ARGS__>()));

#define REGISTER_NAMED_REDUCER(name, ...) \
static NYT::TReducerRegistrator<__VA_ARGS__> \
Y_GENERATE_UNIQUE_ID(TJobRegistrator)(name);

////////////////////////////////////////////////////////////////////////////////

template <class TRow>
void CheckFormats(const char *jobName, const char* direction, const TMultiFormatDesc& desc)
{
    if (desc.Format != TMultiFormatDesc::F_NONE &&
        TFormatDescTraits<TRow>::Format != desc.Format)
    {
        ythrow yexception() <<
            Sprintf("cannot match %s type and %s descriptor", jobName, direction);
    }
}

template <class TMapper>
TOperationId IOperationClient::Map(
    const TMapOperationSpec& spec,
    TMapper* mapper,
    const TOperationOptions& options)
{
    static_assert(TMapper::JobType == IJob::EType::Mapper,
        "This class cannot be used as a mapper");

    using TMapInputRow = typename TMapper::TReader::TRowType;
    using TMapOutputRow = typename TMapper::TWriter::TRowType;

    CheckFormats<TMapInputRow>("mapper", "input", spec.InputDesc_);
    CheckFormats<TMapOutputRow>("mapper", "output", spec.OutputDesc_);

    ::TIntrusivePtr<TMapper> mapperPtr(mapper);

    return DoMap(
        spec,
        mapper,
        options);
}

template <class TReducer>
TOperationId IOperationClient::Reduce(
    const TReduceOperationSpec& spec,
    TReducer* reducer,
    const TOperationOptions& options)
{
    static_assert(
        TReducer::JobType == IJob::EType::Reducer
        || TReducer::JobType == IJob::EType::ReducerAggregator,
        "This class cannot be used as a reducer");

    using TReduceInputRow = typename TReducer::TReader::TRowType;
    using TReduceOutputRow = typename TReducer::TWriter::TRowType;

    CheckFormats<TReduceInputRow>("reducer", "input", spec.InputDesc_);
    CheckFormats<TReduceOutputRow>("reducer", "output", spec.OutputDesc_);

    ::TIntrusivePtr<TReducer> reducerPtr(reducer);

    return DoReduce(
        spec,
        reducer,
        options);
}

template <class TReducer>
TOperationId IOperationClient::JoinReduce(
    const TJoinReduceOperationSpec& spec,
    TReducer* reducer,
    const TOperationOptions& options)
{
    static_assert(TReducer::JobType == IJob::EType::Reducer
        || TReducer::JobType == IJob::EType::ReducerAggregator,
        "This class cannot be used as a reducer");

    using TReduceInputRow = typename TReducer::TReader::TRowType;
    using TReduceOutputRow = typename TReducer::TWriter::TRowType;

    CheckFormats<TReduceInputRow>("reducer", "input", spec.InputDesc_);
    CheckFormats<TReduceOutputRow>("reducer", "output", spec.OutputDesc_);

    ::TIntrusivePtr<TReducer> reducerPtr(reducer);

    return DoJoinReduce(
        spec,
        reducer,
        options);
}

template <class TMapper, class TReducer>
TOperationId IOperationClient::MapReduce(
    const TMapReduceOperationSpec& spec,
    TMapper* mapper,
    TReducer* reducer,
    const TOperationOptions& options)
{
    static_assert(TMapper::JobType == IJob::EType::Mapper,
        "This class cannot be used as a mapper");
    static_assert(TReducer::JobType == IJob::EType::Reducer
        || TReducer::JobType == IJob::EType::ReducerAggregator,
        "This class cannot be used as a reducer");

    using TMapInputRow = typename TMapper::TReader::TRowType;
    using TMapOutputRow = typename TMapper::TWriter::TRowType;
    using TReduceInputRow = typename TReducer::TReader::TRowType;
    using TReduceOutputRow = typename TReducer::TWriter::TRowType;

    CheckFormats<TMapInputRow>("mapper", "input", spec.InputDesc_);
    CheckFormats<TReduceOutputRow>("reducer", "output", spec.OutputDesc_);

    TMultiFormatDesc dummy, outputMapperDesc, inputReducerDesc;
    TOperationIOSpecBase::TFormatAdder<TMapOutputRow>::Add(outputMapperDesc);
    TOperationIOSpecBase::TFormatAdder<TReduceInputRow>::Add(inputReducerDesc);

    ::TIntrusivePtr<TMapper> mapperPtr(mapper);
    ::TIntrusivePtr<TReducer> reducerPtr(reducer);

    return DoMapReduce(
        spec,
        mapper,
        nullptr,
        reducer,
        outputMapperDesc,
        dummy,
        dummy,
        inputReducerDesc,
        options);
}

template <class TReducer>
TOperationId IOperationClient::MapReduce(
    const TMapReduceOperationSpec& spec,
    nullptr_t,
    TReducer* reducer,
    const TOperationOptions& options)
{
    static_assert(TReducer::JobType == IJob::EType::Reducer
        || TReducer::JobType == IJob::EType::ReducerAggregator,
        "This class cannot be used as a reducer");

    using TReduceInputRow = typename TReducer::TReader::TRowType;
    using TReduceOutputRow = typename TReducer::TWriter::TRowType;

    CheckFormats<TReduceInputRow>("reducer", "input", spec.InputDesc_);
    CheckFormats<TReduceOutputRow>("reducer", "output", spec.OutputDesc_);

    TMultiFormatDesc dummy, inputReducerDesc;
    TOperationIOSpecBase::TFormatAdder<TReduceInputRow>::Add(inputReducerDesc);

    ::TIntrusivePtr<TReducer> reducerPtr(reducer);

    return DoMapReduce(
        spec,
        nullptr,
        nullptr,
        reducer,
        dummy,
        dummy,
        dummy,
        inputReducerDesc,
        options);
}

template <class TMapper, class TReduceCombiner, class TReducer>
TOperationId IOperationClient::MapReduce(
    const TMapReduceOperationSpec& spec,
    TMapper* mapper,
    TReduceCombiner* reduceCombiner,
    TReducer* reducer,
    const TOperationOptions& options)
{
    static_assert(TMapper::JobType == IJob::EType::Mapper,
        "This job cannot be used as a mapper");
    static_assert(TReduceCombiner::JobType == IJob::EType::Reducer
        || TReduceCombiner::JobType == IJob::EType::ReducerAggregator,
        "This class cannot be used as a reducer");
    static_assert(TReducer::JobType == IJob::EType::Reducer
        || TReducer::JobType == IJob::EType::ReducerAggregator,
        "This class cannot be used as a reducer");

    using TMapInputRow = typename TMapper::TReader::TRowType;
    using TMapOutputRow = typename TMapper::TWriter::TRowType;
    using TReduceCombinerInputRow = typename TReduceCombiner::TReader::TRowType;
    using TReduceCombinerOutputRow = typename TReduceCombiner::TWriter::TRowType;
    using TReduceInputRow = typename TReducer::TReader::TRowType;
    using TReduceOutputRow = typename TReducer::TWriter::TRowType;

    CheckFormats<TMapInputRow>("mapper", "input", spec.InputDesc_);
    CheckFormats<TReduceOutputRow>("reducer", "output", spec.OutputDesc_);

    TMultiFormatDesc outputMapperDesc, inputReducerDesc,
        inputReduceCombinerDesc, outputReduceCombinerDesc;
    TOperationIOSpecBase::TFormatAdder<TMapOutputRow>::Add(outputMapperDesc);
    TOperationIOSpecBase::TFormatAdder<TReduceInputRow>::Add(inputReducerDesc);
    TOperationIOSpecBase::TFormatAdder<TReduceCombinerInputRow>::Add(inputReduceCombinerDesc);
    TOperationIOSpecBase::TFormatAdder<TReduceCombinerOutputRow>::Add(outputReduceCombinerDesc);

    ::TIntrusivePtr<TMapper> mapperPtr(mapper);
    ::TIntrusivePtr<TReduceCombiner> reduceCombinerPtr(reduceCombiner);
    ::TIntrusivePtr<TReducer> reducerPtr(reducer);

    return DoMapReduce(
        spec,
        mapper,
        reduceCombiner,
        reducer,
        outputMapperDesc,
        inputReduceCombinerDesc,
        outputReduceCombinerDesc,
        inputReducerDesc,
        options);
}

template <class TReduceCombiner, class TReducer>
TOperationId IOperationClient::MapReduce(
    const TMapReduceOperationSpec& spec,
    nullptr_t,
    TReduceCombiner* reduceCombiner,
    TReducer* reducer,
    const TOperationOptions& options)
{
    static_assert(TReduceCombiner::JobType == IJob::EType::Reducer
        || TReduceCombiner::JobType == IJob::EType::ReducerAggregator,
        "This class cannot be used as a reducer");
    static_assert(TReducer::JobType == IJob::EType::Reducer
        || TReducer::JobType == IJob::EType::ReducerAggregator,
        "This class cannot be used as a reducer");

    using TReduceCombinerInputRow = typename TReduceCombiner::TReader::TRowType;
    using TReduceCombinerOutputRow = typename TReduceCombiner::TWriter::TRowType;
    using TReduceInputRow = typename TReducer::TReader::TRowType;
    using TReduceOutputRow = typename TReducer::TWriter::TRowType;

    CheckFormats<TReduceInputRow>("reducer", "input", spec.InputDesc_);
    CheckFormats<TReduceOutputRow>("reducer", "output", spec.OutputDesc_);

    TMultiFormatDesc dummy, inputReducerDesc,
        inputReduceCombinerDesc, outputReduceCombinerDesc;
    TOperationIOSpecBase::TFormatAdder<TReduceInputRow>::Add(inputReducerDesc);
    TOperationIOSpecBase::TFormatAdder<TReduceCombinerInputRow>::Add(inputReduceCombinerDesc);
    TOperationIOSpecBase::TFormatAdder<TReduceCombinerOutputRow>::Add(outputReduceCombinerDesc);

    ::TIntrusivePtr<TReduceCombiner> reduceCombinerPtr(reduceCombiner);
    ::TIntrusivePtr<TReducer> reducerPtr(reducer);

    return DoMapReduce(
        spec,
        nullptr,
        reduceCombiner,
        reducer,
        dummy,
        inputReduceCombinerDesc,
        outputReduceCombinerDesc,
        inputReducerDesc,
        options);
}

////////////////////////////////////////////////////////////////////////////////

}
