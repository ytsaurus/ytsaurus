#pragma once

#ifndef OPERATION_INL_H_
#error "Direct inclusion of this file is not allowed, use operation.h"
#endif
#undef OPERATION_INL_H_

#include "errors.h"

#include <util/generic/bt_exception.h>
#include <util/generic/singleton.h>
#include <util/generic/type_name.h>

#include <util/stream/file.h>
#include <util/stream/buffer.h>
#include <util/string/subst.h>

namespace NYT {

namespace NDetail {

////////////////////////////////////////////////////////////////////////////////

template<class T>
void Assign(TVector<T>& array, size_t idx, const T& value) {
    array.resize(std::max(array.size(), idx + 1));
    array[idx] = value;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NDetail

////////////////////////////////////////////////////////////////////////////////

template <typename TDerived>
TDerived& TRawOperationIoTableSpec<TDerived>::AddInput(const TRichYPath& path)
{
    Inputs_.push_back(path);
    return static_cast<TDerived&>(*this);
}

template <typename TDerived>
TDerived& TRawOperationIoTableSpec<TDerived>::SetInput(size_t tableIndex, const TRichYPath& path)
{
    NDetail::Assign(Inputs_, tableIndex, path);
}

template <typename TDerived>
TDerived& TRawOperationIoTableSpec<TDerived>::AddOutput(const TRichYPath& path)
{
    Outputs_.push_back(path);
    return static_cast<TDerived&>(*this);
}

template <typename TDerived>
TDerived& TRawOperationIoTableSpec<TDerived>::SetOutput(size_t tableIndex, const TRichYPath& path)
{
    NDetail::Assign(Outputs_, tableIndex, path);
}

template <typename TDerived>
const TVector<TRichYPath>& TRawOperationIoTableSpec<TDerived>::GetInputs() const
{
    return Inputs_;
}

template <typename TDerived>
const TVector<TRichYPath>& TRawOperationIoTableSpec<TDerived>::GetOutputs() const
{
    return Outputs_;
}

////////////////////////////////////////////////////////////////////////////////

template <typename TDerived>
TDerived& TRawMapReduceOperationIoSpec<TDerived>::AddMapOutput(const TRichYPath& path)
{
    MapOutputs_.push_back(path);
    return static_cast<TDerived&>(*this);
}

template <typename TDerived>
TDerived& TRawMapReduceOperationIoSpec<TDerived>::SetMapOutput(size_t tableIndex, const TRichYPath& path)
{
    NDetail::Assign(MapOutputs_, tableIndex, path);
}

template <typename TDerived>
const TVector<TRichYPath>& TRawMapReduceOperationIoSpec<TDerived>::GetMapOutputs() const
{
    return MapOutputs_;
}

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
    static const TMultiFormatDesc::EFormat Format = TMultiFormatDesc::F_NODE;
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

template<class T>
void SetFormat(TMultiFormatDesc& desc)
{
    const auto newFmt = TFormatDescTraits<T>::Format;
    if (desc.Format != TMultiFormatDesc::F_NONE && desc.Format != newFmt)
    {
        ythrow yexception() << "Invalid format"; // TODO: more info
    }
    desc.Format = newFmt;
}

template <class T, class TEnable>
struct TOperationIOSpecBase::TFormatAdder
{
    static void Add(TMultiFormatDesc& desc)
    {
        SetFormat<T>(desc);
    }

    static void Set(size_t /*idx*/, TMultiFormatDesc& desc)
    {
        SetFormat<T>(desc);
    }
};

//TODO: enable when all the clients will not use AddInput<Message>/AddOutput<Message>
//see REVIEW: 270137
//
//template<>
//struct TOperationIOSpecBase::TFormatAdder<Message>;

template <class T>
struct TOperationIOSpecBase::TFormatAdder<T, std::enable_if_t<TIsBaseOf<Message, T>::Value>>
{
    static void Add(TMultiFormatDesc& desc)
    {
        SetFormat<T>(desc);
        desc.ProtoDescriptors.push_back(T::descriptor());
    }

    static void Set(size_t idx, TMultiFormatDesc& desc)
    {
        SetFormat<T>(desc);
        NDetail::Assign(desc.ProtoDescriptors, idx, T::descriptor());
    }
};

template <class T>
void TOperationIOSpecBase::AddInput(const TRichYPath& path)
{
    TOperationIOSpecBase::TFormatAdder<T>::Add(InputDesc_);
    Inputs_.push_back(path);
}

template <class T>
void TOperationIOSpecBase::SetInput(size_t tableIndex, const TRichYPath& path)
{
    TOperationIOSpecBase::TFormatAdder<T>::Set(tableIndex, InputDesc_);
    NDetail::Assign(Inputs_, tableIndex, path);
}


template <class T>
void TOperationIOSpecBase::AddOutput(const TRichYPath& path)
{
    TOperationIOSpecBase::TFormatAdder<T>::Add(OutputDesc_);
    Outputs_.push_back(path);
}

template <class T>
void TOperationIOSpecBase::SetOutput(size_t tableIndex, const TRichYPath& path)
{
    TOperationIOSpecBase::TFormatAdder<T>::Set(tableIndex, OutputDesc_);
    NDetail::Assign(Outputs_, tableIndex, path);
}

template <class TDerived>
template <class T>
TDerived& TOperationIOSpec<TDerived>::AddInput(const TRichYPath& path)
{
    static_assert(!std::is_same<T, Message>::value, "input type can't be Message, it can only be its strict subtype (see st.yandex-team.ru/YT-7609)");
    TOperationIOSpecBase::AddInput<T>(path);
    return *static_cast<TDerived*>(this);
}

template <class TDerived>
template <class T>
TDerived& TOperationIOSpec<TDerived>::SetInput(size_t tableIndex, const TRichYPath& path)
{
    static_assert(!std::is_same<T, Message>::value, "input type can't be Message, it can only be its strict subtype (see st.yandex-team.ru/YT-7609)");
    TOperationIOSpecBase::SetInput<T>(tableIndex, path);
    return *static_cast<TDerived*>(this);
}


template <class TDerived>
template <class T>
TDerived& TOperationIOSpec<TDerived>::AddOutput(const TRichYPath& path)
{
    static_assert(!std::is_same<T, Message>::value, "output type can't be Message, it can only be its strict subtype (see st.yandex-team.ru/YT-7609)");
    TOperationIOSpecBase::AddOutput<T>(path);
    return *static_cast<TDerived*>(this);
}

template <class TDerived>
template <class T>
TDerived& TOperationIOSpec<TDerived>::SetOutput(size_t tableIndex, const TRichYPath& path)
{
    static_assert(!std::is_same<T, Message>::value, "output type can't be Message, it can only be its strict subtype (see st.yandex-team.ru/YT-7609)");
    TOperationIOSpecBase::SetOutput<T>(tableIndex, path);
    return *static_cast<TDerived*>(this);
}

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

void ResetUseClientProtobuf(const char* methodName);

} // namespace NDetail

template <class TDerived>
TDerived& TOperationIOSpec<TDerived>::AddProtobufInput_VerySlow_Deprecated(const TRichYPath& path)
{
    NDetail::ResetUseClientProtobuf("AddProtobufInput_VerySlow_Deprecated");
    TOperationIOSpecBase::TFormatAdder<Message>::Add(InputDesc_);
    Inputs_.push_back(path);
    return *static_cast<TDerived*>(this);
}

template <class TDerived>
TDerived& TOperationIOSpec<TDerived>::SetProtobufInput_VerySlow_Deprecated(size_t tableIndex, const TRichYPath& path)
{
    NDetail::ResetUseClientProtobuf("SetProtobufInput_VerySlow_Deprecated");
    TOperationIOSpecBase::TFormatAdder<Message>::Set(tableIndex, InputDesc_);
    NDetail::Assign(Inputs_, tableIndex, path);
    return *static_cast<TDerived*>(this);
}

template <class TDerived>
TDerived& TOperationIOSpec<TDerived>::AddProtobufOutput_VerySlow_Deprecated(const TRichYPath& path)
{
    NDetail::ResetUseClientProtobuf("AddProtobufOutput_VerySlow_Deprecated");
    TOperationIOSpecBase::TFormatAdder<Message>::Add(OutputDesc_);
    Outputs_.push_back(path);
    return *static_cast<TDerived*>(this);
}

template <class TDerived>
TDerived& TOperationIOSpec<TDerived>::SetProtobufOutput_VerySlow_Deprecated(size_t tableIndex, const TRichYPath& path)
{
    NDetail::ResetUseClientProtobuf("SetProtobufOutput_VerySlow_Deprecated");
    TOperationIOSpecBase::TFormatAdder<Message>::Set(tableIndex, OutputDesc_);
    NDetail::Assign(Outputs_, tableIndex, path);
    return *static_cast<TDerived*>(this);
}

////////////////////////////////////////////////////////////////////////////////

template <class TDerived>
template <class TRow>
TDerived& TIntermediateTablesHintSpec<TDerived>::HintMapOutput()
{
    TOperationIOSpecBase::TFormatAdder<TRow>::Set(0, MapOutputDesc_);
    return *static_cast<TDerived*>(this);
}

template <class TDerived>
template <class TRow>
TDerived& TIntermediateTablesHintSpec<TDerived>::AddMapOutput(const TRichYPath& path)
{
    if (MapOutputs_.empty()) {
        TOperationIOSpecBase::TFormatAdder<TRow>::Set(1, MapOutputDesc_);
    } else {
        TOperationIOSpecBase::TFormatAdder<TRow>::Add(MapOutputDesc_);
    }
    MapOutputs_.push_back(path);
    return *static_cast<TDerived*>(this);
}

template <class TDerived>
template <class TRow>
TDerived& TIntermediateTablesHintSpec<TDerived>::HintReduceCombinerInput()
{
    if (!ReduceCombinerInputHintDesc_.ProtoDescriptors.empty()) {
        ythrow TApiUsageError() << "HintReduceCombinerInput cannot be called multiple times";
    }
    TOperationIOSpecBase::TFormatAdder<TRow>::Add(ReduceCombinerInputHintDesc_);
    return *static_cast<TDerived*>(this);
}

template <class TDerived>
template <class TRow>
TDerived& TIntermediateTablesHintSpec<TDerived>::HintReduceCombinerOutput()
{
    if (!ReduceCombinerOutputHintDesc_.ProtoDescriptors.empty()) {
        ythrow TApiUsageError() << "HintReduceCombinerOutput cannot be called multiple times";
    }
    TOperationIOSpecBase::TFormatAdder<TRow>::Add(ReduceCombinerOutputHintDesc_);
    return *static_cast<TDerived*>(this);
}

template <class TDerived>
template <class TRow>
TDerived& TIntermediateTablesHintSpec<TDerived>::HintReduceInput()
{
    if (!ReduceInputHintDesc_.ProtoDescriptors.empty()) {
        ythrow TApiUsageError() << "HintReduceInput cannot be called multiple times";
    }
    TOperationIOSpecBase::TFormatAdder<TRow>::Add(ReduceInputHintDesc_);
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

template <class TRawJob>
int RunRawJob(size_t outputTableCount, IInputStream& jobStateStream)
{
    TRawJobContext context(outputTableCount);

    TRawJob job;
    job.Load(jobStateStream);
    job.Do(context);
    return 0;
}

template <class TJob>
int RunJob(size_t outputTableCount, IInputStream& jobStateStream)
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
int RunMapJob(size_t outputTableCount, IInputStream& jobStateStream)
{
    return RunJob<TMapper>(outputTableCount, jobStateStream);
}

template <class TReducer>
int RunReduceJob(size_t outputTableCount, IInputStream& jobStateStream)
{
    return RunJob<TReducer>(outputTableCount, jobStateStream);
}

template <class TReducer>
int RunAggregatorReducer(size_t outputTableCount, IInputStream& jobStateStream)
{
    return RunJob<TReducer>(outputTableCount, jobStateStream);
}

////////////////////////////////////////////////////////////////////////////////

using TJobFunction = int (*)(size_t, IInputStream&);

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

    template <class TRawJob>
    void RegisterRawJob(const char* name)
    {
        const auto* typeInfoPtr = &typeid(TRawJob);
        CheckNotRegistered(typeInfoPtr, name);
        JobNames[typeInfoPtr] = name;
        JobFunctions[name] = RunRawJob<TRawJob>;
    }

    TString GetJobName(IJob* job)
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
    THashMap<const std::type_info*, TString> JobNames;
    THashMap<TString, TJobFunction> JobFunctions;

    void CheckNotRegistered(const std::type_info* typeInfoPtr, const char* name)
    {
        Y_ENSURE(!JobNames.has(typeInfoPtr),
            "type_info '" << typeInfoPtr->name() << "'"
            "is already registered under name '" << JobNames[typeInfoPtr] << "'");
        Y_ENSURE(!JobFunctions.has(name),
            "job with name '" << name << "' is already registered");
    }

    void CheckJobRegistered(const std::type_info* typeInfoPtr)
    {
        Y_ENSURE(JobNames.has(typeInfoPtr),
            "type_info '" << typeInfoPtr->name() << "' is not registered, use REGISTER_* macros");
    }

    void CheckNameRegistered(const char* name)
    {
        Y_ENSURE(JobFunctions.has(name),
            "job with name '" << name << "' is not registered, use REGISTER_* macros");
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

template <class TRawJob>
struct TRawJobRegistrator
{
    TRawJobRegistrator(const char* name)
    {
        static_assert(TRawJob::JobType == IJob::EType::RawJob,
            "REGISTER_RAW_JOB is not compatible with this job class");
        NYT::TJobFactory::Get()->RegisterRawJob<TRawJob>(name);
    }
};

////////////////////////////////////////////////////////////////////////////////

inline TString YtRegistryTypeName(const TString& name) {
    TString res = name;
#ifdef _win_
    SubstGlobal(res, "class ", "");
#endif
    return res;
}

////////////////////////////////////////////////////////////////////////////////

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

#define REGISTER_NAMED_RAW_JOB(name, className) \
static NYT::TRawJobRegistrator<className> \
Y_GENERATE_UNIQUE_ID(TJobRegistrator)(name);

#define REGISTER_RAW_JOB(className) \
REGISTER_NAMED_RAW_JOB((~NYT::YtRegistryTypeName(TypeName<className>())), className)

////////////////////////////////////////////////////////////////////////////////

template <class TRow>
void CheckFormats(const char *jobName, const char* direction, const TMultiFormatDesc& desc)
{
    if (desc.Format != TMultiFormatDesc::F_NONE &&
        TFormatDescTraits<TRow>::Format != desc.Format)
    {
        ythrow TApiUsageError()
            << "cannot match " << jobName << " type and " << direction << " descriptor";
    }
}

////////////////////////////////////////////////////////////////////////////////

template <class TReader, class TWriter>
void IMapper<TReader, TWriter>::CheckInputFormat(const char* jobName, const TMultiFormatDesc& desc)
{
    NYT::CheckFormats<typename TReader::TRowType>(jobName, "input", desc);
}

template <class TReader, class TWriter>
void IMapper<TReader, TWriter>::CheckOutputFormat(const char* jobName, const TMultiFormatDesc& desc)
{
    NYT::CheckFormats<typename TWriter::TRowType>(jobName, "output", desc);
}

template <class TReader, class TWriter>
void IMapper<TReader, TWriter>::AddInputFormatDescription(TMultiFormatDesc* desc)
{
    TOperationIOSpecBase::TFormatAdder<typename TReader::TRowType>::Add(*desc);
}

template <class TReader, class TWriter>
void IMapper<TReader, TWriter>::AddOutputFormatDescription(TMultiFormatDesc* desc)
{
    TOperationIOSpecBase::TFormatAdder<typename TWriter::TRowType>::Add(*desc);
}

////////////////////////////////////////////////////////////////////////////////

template <class TReader, class TWriter>
void IReducer<TReader, TWriter>::CheckInputFormat(const char* jobName, const TMultiFormatDesc& desc)
{
    NYT::CheckFormats<typename TReader::TRowType>(jobName, "input", desc);
}

template <class TReader, class TWriter>
void IReducer<TReader, TWriter>::CheckOutputFormat(const char* jobName, const TMultiFormatDesc& desc)
{
    NYT::CheckFormats<typename TWriter::TRowType>(jobName, "output", desc);
}

template <class TReader, class TWriter>
void IReducer<TReader, TWriter>::AddInputFormatDescription(TMultiFormatDesc* desc)
{
    TOperationIOSpecBase::TFormatAdder<typename TReader::TRowType>::Add(*desc);
}

template <class TReader, class TWriter>
void IReducer<TReader, TWriter>::AddOutputFormatDescription(TMultiFormatDesc* desc)
{
    TOperationIOSpecBase::TFormatAdder<typename TWriter::TRowType>::Add(*desc);
}

////////////////////////////////////////////////////////////////////////////////

template <class TReader, class TWriter>
void IAggregatorReducer<TReader, TWriter>::CheckInputFormat(const char* jobName, const TMultiFormatDesc& desc)
{
    NYT::CheckFormats<typename TReader::TRowType>(jobName, "input", desc);
}

template <class TReader, class TWriter>
void IAggregatorReducer<TReader, TWriter>::CheckOutputFormat(const char* jobName, const TMultiFormatDesc& desc)
{
    NYT::CheckFormats<typename TWriter::TRowType>(jobName, "output", desc);
}

template <class TReader, class TWriter>
void IAggregatorReducer<TReader, TWriter>::AddInputFormatDescription(TMultiFormatDesc* desc)
{
    TOperationIOSpecBase::TFormatAdder<typename TReader::TRowType>::Add(*desc);
}

template <class TReader, class TWriter>
void IAggregatorReducer<TReader, TWriter>::AddOutputFormatDescription(TMultiFormatDesc* desc)
{
    TOperationIOSpecBase::TFormatAdder<typename TWriter::TRowType>::Add(*desc);
}

////////////////////////////////////////////////////////////////////////////////

}
