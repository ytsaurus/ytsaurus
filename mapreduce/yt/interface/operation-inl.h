#pragma once

#ifndef OPERATION_INL_H_
#error "Direct inclusion of this file is not allowed, use operation.h"
#endif
#undef OPERATION_INL_H_

#include <util/generic/type_name.h>

#include <contrib/libs/protobuf/message.h>

#include <util/stream/file.h>

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

inline void LoadJobState(IJob* job)
{
    TFileInput stream("jobstate");
    job->Load(stream);
}

////////////////////////////////////////////////////////////////////////////////

template <class TMapper>
int RunMapJob(size_t outputTableCount, bool hasState)
{
    using TInputRow = typename TMapper::TReader::TRowType;
    using TOutputRow = typename TMapper::TWriter::TRowType;

    auto reader = CreateJobReader<TInputRow>();
    auto writer = CreateJobWriter<TOutputRow>(outputTableCount);

    auto mapper = MakeIntrusive<TMapper>();
    if (hasState) {
        LoadJobState(mapper.Get());
    }

    try {
        mapper->Start(writer.Get());
        mapper->Do(reader.Get(), writer.Get());
        mapper->Finish(writer.Get());
        writer->Finish();

    } catch (NStl::exception& e) {
        Cerr << "Exception caught: " << e.what() << Endl;
        return 1;
    }

    return 0;
}

template <class TReducer>
int RunReduceJob(size_t outputTableCount, bool hasState)
{
    using TInputRow = typename TReducer::TReader::TRowType;
    using TOutputRow = typename TReducer::TWriter::TRowType;

    auto readerImpl = CreateJobReaderImpl<TInputRow>();
    auto reader = MakeIntrusive<TTableReader<TInputRow>>(readerImpl);
    auto writer = CreateJobWriter<TOutputRow>(outputTableCount);

    auto reducer = MakeIntrusive<TReducer>();
    if (hasState) {
        LoadJobState(reducer.Get());
    }

    try {
        reducer->Start(writer.Get());
        while (reader->IsValid()) {
            reducer->Do(reader.Get(), writer.Get());
            readerImpl->NextKey();
        }
        reducer->Finish(writer.Get());
        writer->Finish();

    } catch (NStl::exception& e) {
        Cerr << "Exception caught: " << e.what() << Endl;
        return 1;
    }

    return 0;
}

////////////////////////////////////////////////////////////////////////////////

using TJobFunction = int (*)(size_t, bool);

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
        const auto* typeInfoPtr = &typeid(TMapper);
        CheckNotRegistered(typeInfoPtr, name);
        JobNames[typeInfoPtr] = name;
        JobFunctions[name] = RunMapJob<TMapper>;
    }

    template <class TReducer>
    void RegisterReducer(const char* name)
    {
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
                Sprintf("type_info for %s is already registered",
                    typeInfoPtr->name());
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
                Sprintf("type_info for %s is not registered, use REGISTER_* macros",
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
GENERATE_UNIQUE_ID(TJobRegistrator)(~TypeName<__VA_ARGS__>()); \

#define REGISTER_NAMED_MAPPER(name, ...) \
static NYT::TMapperRegistrator<__VA_ARGS__> \
GENERATE_UNIQUE_ID(TJobRegistrator)(name);

#define REGISTER_REDUCER(...) \
static NYT::TReducerRegistrator<__VA_ARGS__> \
GENERATE_UNIQUE_ID(TJobRegistrator)(~TypeName<__VA_ARGS__>());

#define REGISTER_NAMED_REDUCER(name, ...) \
static NYT::TReducerRegistrator<__VA_ARGS__> \
GENERATE_UNIQUE_ID(TJobRegistrator)(name);

////////////////////////////////////////////////////////////////////////////////

template <class TMapper>
TOperationId IOperationClient::Map(
    const TMapOperationSpec& spec,
    TMapper* mapper,
    const TOperationOptions& options)
{
    using TInputRow = typename TMapper::TReader::TRowType;
    using TOutputRow = typename TMapper::TWriter::TRowType;

    if (TFormatDescTraits<TInputRow>::Format != spec.InputDesc_.Format) {
        ythrow yexception() << "cannot match mapper type and input descriptor";
    }
    if (TFormatDescTraits<TOutputRow>::Format != spec.OutputDesc_.Format) {
        ythrow yexception() << "cannot match mapper type and output descriptor";
    }

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
    using TInputRow = typename TReducer::TReader::TRowType;
    using TOutputRow = typename TReducer::TWriter::TRowType;

    if (TFormatDescTraits<TInputRow>::Format != spec.InputDesc_.Format) {
        ythrow yexception() << "cannot match reducer type and input descriptor";
    }
    if (TFormatDescTraits<TOutputRow>::Format != spec.OutputDesc_.Format) {
        ythrow yexception() << "cannot match reducer type and output descriptor";
    }

    TIntrusivePtr<TReducer> reducerPtr(reducer);

    return DoReduce(
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

    if (TFormatDescTraits<TMapInputRow>::Format != spec.InputDesc_.Format) {
        ythrow yexception() << "cannot match mapper type and input descriptor";
    }
    if (TFormatDescTraits<TReduceOutputRow>::Format != spec.OutputDesc_.Format) {
        ythrow yexception() << "cannot match reducer type and output descriptor";
    }

    TMultiFormatDesc outputMapperDesc, inputReducerDesc;
    outputMapperDesc.Format = TFormatDescTraits<TMapOutputRow>::Format;
    inputReducerDesc.Format = TFormatDescTraits<TReduceInputRow>::Format;

    TIntrusivePtr<TMapper> mapperPtr(mapper);
    TIntrusivePtr<TReducer> reducerPtr(reducer);

    return DoMapReduce(
        spec,
        mapper,
        reducer,
        outputMapperDesc,
        inputReducerDesc,
        options);
}

////////////////////////////////////////////////////////////////////////////////

}
