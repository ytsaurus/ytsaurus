#pragma once

#ifndef OPERATION_INL_H_
#error "Direct inclusion of this file is not allowed, use operation.h"
#endif
#undef OPERATION_INL_H_

#include <util/generic/type_name.h>

#include <contrib/libs/protobuf/message.h>

#include <util/stream/file.h>
#include <util/stream/buffer.h>

#include <util/generic/bt_exception.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

TIntrusivePtr<INodeReaderImpl> CreateJobNodeReader();
TIntrusivePtr<IYaMRReaderImpl> CreateJobYaMRReader();
TIntrusivePtr<IProtoReaderImpl> CreateJobProtoReader();

TIntrusivePtr<INodeWriterImpl> CreateJobNodeWriter(size_t outputTableCount);
TIntrusivePtr<IYaMRWriterImpl> CreateJobYaMRWriter(size_t outputTableCount);
TIntrusivePtr<IProtoWriterImpl> CreateJobProtoWriter(size_t outputTableCount);

////////////////////////////////////////////////////////////////////////////////

template <class T>
inline TIntrusivePtr<typename TRowTraits<T>::IReaderImpl> CreateJobReaderImpl();

template <>
inline TIntrusivePtr<INodeReaderImpl> CreateJobReaderImpl<TNode>()
{
    return CreateJobNodeReader();
}

template <>
inline TIntrusivePtr<IYaMRReaderImpl> CreateJobReaderImpl<TYaMRRow>()
{
    return CreateJobYaMRReader();
}

template <>
inline TIntrusivePtr<IProtoReaderImpl> CreateJobReaderImpl<Message>()
{
    return CreateJobProtoReader();
}

template <class T>
inline TIntrusivePtr<typename TRowTraits<T>::IReaderImpl> CreateJobReaderImpl()
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
struct TProtoWriterCreator<T, TEnableIf<TIsBaseOf<Message, T>::Value>>
{
    static TTableWriterPtr<T> Create(TIntrusivePtr<IProtoWriterImpl> writer)
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
struct TFormatDescTraits<T, TEnableIf<TIsBaseOf<Message, T>::Value>>
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
struct TOperationIOSpecBase::TFormatAdder<T, TEnableIf<TIsBaseOf<Message, T>::Value>>
{
    static void Add(TMultiFormatDesc& desc)
    {
        if (desc.Format != TMultiFormatDesc::F_NONE &&
            desc.Format != TMultiFormatDesc::F_PROTO) {
            ythrow yexception() << "Invalid format"; // TODO: more info
        }
        desc.Format = TMultiFormatDesc::F_PROTO;
        desc.ProtoFormats.push_back(TProtoFormat());
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
TDerived& TOperationIOSpec<TDerived>::AddOutput(const TRichYPath& path)
{
    TOperationIOSpecBase::TFormatAdder<T>::Add(OutputDesc_);
    Outputs_.push_back(path);
    return *static_cast<TDerived*>(this);
}

////////////////////////////////////////////////////////////////////////////////

template <class TMapper>
int RunMapJob(size_t outputTableCount, TInputStream& jobStateStream)
{
    using TInputRow = typename TMapper::TReader::TRowType;
    using TOutputRow = typename TMapper::TWriter::TRowType;

    try {
        auto reader = CreateJobReader<TInputRow>();
        auto writer = CreateJobWriter<TOutputRow>(outputTableCount);

        auto mapper = MakeIntrusive<TMapper>();
        mapper->Load(jobStateStream);

        mapper->Start(writer.Get());
        mapper->Do(reader.Get(), writer.Get());
        mapper->Finish(writer.Get());
        writer->Finish();

    } catch (const TWithBackTrace<yexception>& e) {
        Cerr << "Exception caught: " << e.what() << Endl;
        e.BackTrace()->PrintTo(Cerr);
        return 1;

    } catch (const std::exception& e) {
        Cerr << "Exception caught: " << e.what() << Endl;
        return 1;
    }

    return 0;
}

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

template <class TReducer>
int RunReduceJob(size_t outputTableCount, TInputStream& jobStateStream)
{
    using TInputRow = typename TReducer::TReader::TRowType;
    using TOutputRow = typename TReducer::TWriter::TRowType;

    try {
        auto readerImpl = CreateJobReaderImpl<TInputRow>();
        auto reader = MakeIntrusive<TTableReader<TInputRow>>(readerImpl);
        auto writer = CreateJobWriter<TOutputRow>(outputTableCount);

        auto reducer = MakeIntrusive<TReducer>();
        reducer->Load(jobStateStream);

        reducer->Start(writer.Get());
        while (reader->IsValid()) {
            reducer->Do(reader.Get(), writer.Get());
            if (TReducerContext::Get()->Break) {
                break;
            }
            readerImpl->NextKey();
            if (reader->IsValid()) {
                reader->Next();
            }
        }
        reducer->Finish(writer.Get());
        writer->Finish();

    } catch (const TWithBackTrace<yexception>& e) {
        Cerr << "Exception caught: " << e.what() << Endl;
        e.BackTrace()->PrintTo(Cerr);
        return 1;

    } catch (std::exception& e) {
        Cerr << "Exception caught: " << e.what() << Endl;
        return 1;
    }

    return 0;
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

    template <class TMapper>
    void RegisterMapper(const char* name)
    {
        if (TMapper::JobType != IJob::EType::Mapper) {
            ythrow yexception() <<
                "REGISTER_MAPPER is not compatible with job class " << name;
        }

        const auto* typeInfoPtr = &typeid(TMapper);
        CheckNotRegistered(typeInfoPtr, name);
        JobNames[typeInfoPtr] = name;
        JobFunctions[name] = RunMapJob<TMapper>;
    }

    template <class TReducer>
    void RegisterReducer(const char* name)
    {
        if (TReducer::JobType != IJob::EType::Reducer) {
            ythrow yexception() <<
                "REGISTER_REDUCER is not compatible with job class " << name;
        }

        const auto* typeInfoPtr = &typeid(TReducer);
        CheckNotRegistered(typeInfoPtr, name);
        JobNames[typeInfoPtr] = name;
        JobFunctions[name] = RunReduceJob<TReducer>;
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
        NYT::TJobFactory::Get()->RegisterMapper<TMapper>(name);
    }
};

template <class TReducer>
struct TReducerRegistrator
{
    TReducerRegistrator(const char* name)
    {
        NYT::TJobFactory::Get()->RegisterReducer<TReducer>(name);
    }
};

#define REGISTER_MAPPER(...) \
static NYT::TMapperRegistrator<__VA_ARGS__> \
Y_GENERATE_UNIQUE_ID(TJobRegistrator)(~TypeName<__VA_ARGS__>());

#define REGISTER_NAMED_MAPPER(name, ...) \
static NYT::TMapperRegistrator<__VA_ARGS__> \
Y_GENERATE_UNIQUE_ID(TJobRegistrator)(name);

#define REGISTER_REDUCER(...) \
static NYT::TReducerRegistrator<__VA_ARGS__> \
Y_GENERATE_UNIQUE_ID(TJobRegistrator)(~TypeName<__VA_ARGS__>());

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
    using TMapInputRow = typename TMapper::TReader::TRowType;
    using TMapOutputRow = typename TMapper::TWriter::TRowType;

    CheckFormats<TMapInputRow>("mapper", "input", spec.InputDesc_);
    CheckFormats<TMapOutputRow>("mapper", "output", spec.OutputDesc_);

    TIntrusivePtr<TMapper> mapperPtr(mapper);

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
    using TReduceInputRow = typename TReducer::TReader::TRowType;
    using TReduceOutputRow = typename TReducer::TWriter::TRowType;

    CheckFormats<TReduceInputRow>("reducer", "input", spec.InputDesc_);
    CheckFormats<TReduceOutputRow>("reducer", "output", spec.OutputDesc_);

    TIntrusivePtr<TReducer> reducerPtr(reducer);

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
    using TReduceInputRow = typename TReducer::TReader::TRowType;
    using TReduceOutputRow = typename TReducer::TWriter::TRowType;

    CheckFormats<TReduceInputRow>("reducer", "input", spec.InputDesc_);
    CheckFormats<TReduceOutputRow>("reducer", "output", spec.OutputDesc_);

    TIntrusivePtr<TReducer> reducerPtr(reducer);

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
    using TMapInputRow = typename TMapper::TReader::TRowType;
    using TMapOutputRow = typename TMapper::TWriter::TRowType;
    using TReduceInputRow = typename TReducer::TReader::TRowType;
    using TReduceOutputRow = typename TReducer::TWriter::TRowType;

    CheckFormats<TMapInputRow>("mapper", "input", spec.InputDesc_);
    CheckFormats<TReduceOutputRow>("reducer", "output", spec.OutputDesc_);

    TMultiFormatDesc dummy, outputMapperDesc, inputReducerDesc;
    outputMapperDesc.Format = TFormatDescTraits<TMapOutputRow>::Format;
    inputReducerDesc.Format = TFormatDescTraits<TReduceInputRow>::Format;

    TIntrusivePtr<TMapper> mapperPtr(mapper);
    TIntrusivePtr<TReducer> reducerPtr(reducer);

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
    using TReduceInputRow = typename TReducer::TReader::TRowType;
    using TReduceOutputRow = typename TReducer::TWriter::TRowType;

    CheckFormats<TReduceInputRow>("reducer", "input", spec.InputDesc_);
    CheckFormats<TReduceOutputRow>("reducer", "output", spec.OutputDesc_);

    TMultiFormatDesc dummy, inputReducerDesc;
    inputReducerDesc.Format = TFormatDescTraits<TReduceInputRow>::Format;

    TIntrusivePtr<TReducer> reducerPtr(reducer);

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
    outputMapperDesc.Format = TFormatDescTraits<TMapOutputRow>::Format;
    inputReducerDesc.Format = TFormatDescTraits<TReduceInputRow>::Format;
    inputReduceCombinerDesc.Format = TFormatDescTraits<TReduceCombinerInputRow>::Format;
    outputReduceCombinerDesc.Format = TFormatDescTraits<TReduceCombinerOutputRow>::Format;

    TIntrusivePtr<TMapper> mapperPtr(mapper);
    TIntrusivePtr<TReduceCombiner> reduceCombinerPtr(reduceCombiner);
    TIntrusivePtr<TReducer> reducerPtr(reducer);

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
    using TReduceCombinerInputRow = typename TReduceCombiner::TReader::TRowType;
    using TReduceCombinerOutputRow = typename TReduceCombiner::TWriter::TRowType;
    using TReduceInputRow = typename TReducer::TReader::TRowType;
    using TReduceOutputRow = typename TReducer::TWriter::TRowType;

    CheckFormats<TReduceInputRow>("reducer", "input", spec.InputDesc_);
    CheckFormats<TReduceOutputRow>("reducer", "output", spec.OutputDesc_);

    TMultiFormatDesc dummy, inputReducerDesc,
        inputReduceCombinerDesc, outputReduceCombinerDesc;
    inputReducerDesc.Format = TFormatDescTraits<TReduceInputRow>::Format;
    inputReduceCombinerDesc.Format = TFormatDescTraits<TReduceCombinerInputRow>::Format;
    outputReduceCombinerDesc.Format = TFormatDescTraits<TReduceCombinerOutputRow>::Format;

    TIntrusivePtr<TReduceCombiner> reduceCombinerPtr(reduceCombiner);
    TIntrusivePtr<TReducer> reducerPtr(reducer);

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
