#include "public.h"
#include "helpers.h"
#include "serialize.h"
#include "shutdown.h"
#include "stream.h"
#include "lazy_list_fragment_parser.h"
#include "lazy_parser.h"
#include "lazy_yson_consumer.h"
#include "yson_lazy_map.h"
#include "object_builder.h"
#include "protobuf_descriptor_pool.h"
#include "list_fragment_parser.h"

#include <yt/core/ytree/convert.h>

#include <yt/core/yson/protobuf_interop.h>

#include <yt/core/misc/crash_handler.h>
#include <yt/core/misc/blob.h>

#include <contrib/libs/pycxx/Extensions.hxx>
#include <contrib/libs/pycxx/Objects.hxx>

#include <contrib/libs/protobuf/io/coded_stream.h>
#include <contrib/libs/protobuf/io/zero_copy_stream_impl_lite.h>

#include <contrib/libs/protobuf/google/protobuf/descriptor.pb.h>

namespace NYT {
namespace NPython {

using namespace NYTree;
using namespace NYson;

static const int BufferSize = 1024 * 1024;

////////////////////////////////////////////////////////////////////////////////

Py::Exception CreateYsonError(const TString& message, const NYT::TError& error = TError())
{
    auto ysonModule = Py::Module(PyImport_ImportModule("yt.yson.common"), true);
    auto ysonErrorClass = Py::Callable(GetAttr(ysonModule, "YsonError"));

    std::vector<TError> innerErrors({error});
    Py::Dict options;
    options.setItem("message", ConvertTo<Py::Object>(message));
    options.setItem("code", ConvertTo<Py::Object>(1));
    options.setItem("inner_errors", ConvertTo<Py::Object>(innerErrors));
    auto ysonError = ysonErrorClass.apply(Py::Tuple(), options);
    return Py::Exception(*ysonError.type(), ysonError);
}

#define CATCH(message) \
    catch (const NYT::TErrorException& error) { \
        throw CreateYsonError(message, error.Error()); \
    } catch (const std::exception& ex) { \
        if (PyErr_ExceptionMatches(PyExc_BaseException)) { \
            throw; \
        } else { \
            throw CreateYsonError(message, TError(ex)); \
        } \
    }

////////////////////////////////////////////////////////////////////////////////

class TYsonIterator
    : public Py::PythonClass<TYsonIterator>
{
public:
    TYsonIterator(Py::PythonClassInstance *self, Py::Tuple& args, Py::Dict& kwargs)
        : Py::PythonClass<TYsonIterator>::PythonClass(self, args, kwargs)
    { }

    void Init(IInputStream* inputStream, std::unique_ptr<IInputStream> inputStreamOwner,
              bool alwaysCreateAttributes, const TNullable<TString>& encoding)
    {
        YCHECK(!inputStreamOwner || inputStreamOwner.get() == inputStream);
        InputStream_ = inputStream;
        InputStreamOwner_ = std::move(inputStreamOwner);
        Consumer_.reset(new NYTree::TPythonObjectBuilder(alwaysCreateAttributes, encoding));
        Parser_.reset(new NYson::TYsonParser(Consumer_.get(), NYson::EYsonType::ListFragment));
        IsStreamRead_ = false;
    }

    Py::Object iter()
    {
        return self();
    }

    PyObject* iternext()
    {
        try {
            // Read unless we have whole row
            while (!Consumer_->HasObject() && !IsStreamRead_) {
                int length = InputStream_->Read(Buffer_, BufferSize);
                if (length != 0) {
                    Parser_->Read(TStringBuf(Buffer_, length));
                }
                if (BufferSize != length) {
                    IsStreamRead_ = true;
                    Parser_->Finish();
                }
            }

            // Stop iteration if we done
            if (!Consumer_->HasObject()) {
                PyErr_SetNone(PyExc_StopIteration);
                return 0;
            }

            auto result = Consumer_->ExtractObject();
            // We should return pointer to alive object
            result.increment_reference_count();
            return result.ptr();
        } CATCH("Yson load failed");
    }

    virtual ~TYsonIterator()
    { }

    static void InitType()
    {
        behaviors().name("Yson iterator");
        behaviors().doc("Iterates over stream with YSON rows");
        behaviors().supportGetattro();
        behaviors().supportSetattro();
        behaviors().supportIter();

        behaviors().readyType();
    }

private:
    IInputStream* InputStream_;
    std::unique_ptr<IInputStream> InputStreamOwner_;

    bool IsStreamRead_;

    std::unique_ptr<NYTree::TPythonObjectBuilder> Consumer_;
    std::unique_ptr<NYson::TYsonParser> Parser_;

    char Buffer_[BufferSize];
};

////////////////////////////////////////////////////////////////////////////////

class TRawYsonIterator
    : public Py::PythonClass<TRawYsonIterator>
{
public:
    TRawYsonIterator(Py::PythonClassInstance *self, Py::Tuple& args, Py::Dict& kwargs)
        : Py::PythonClass<TRawYsonIterator>::PythonClass(self, args, kwargs)
    { }

    void Init(IInputStream* inputStream, std::unique_ptr<IInputStream> inputStreamOwner)
    {
        InputStreamOwner_ = std::move(inputStreamOwner);
        Parser_ = TListFragmentParser(inputStream);
    }

    Py::Object iter()
    {
        return self();
    }

    PyObject* iternext()
    {
        try {
            TSharedRef item;

            {
                TReleaseAcquireGilGuard guard;
                item = Parser_.NextItem();
            }

            if (!item) {
                PyErr_SetNone(PyExc_StopIteration);
                return 0;
            }
            auto result = Py::Bytes(item.Begin(), item.Size());
            result.increment_reference_count();
            return result.ptr();
        } CATCH("Yson load failed");
    }

    virtual ~TRawYsonIterator()
    { }

    static void InitType()
    {
        behaviors().name("Yson iterator");
        behaviors().doc("Iterates over stream with YSON rows");
        behaviors().supportGetattro();
        behaviors().supportSetattro();
        behaviors().supportIter();

        behaviors().readyType();
    }

private:
    std::unique_ptr<IInputStream> InputStreamOwner_;
    TListFragmentParser Parser_;
};

////////////////////////////////////////////////////////////////////////////////

class TLazyYsonIterator
    : public Py::PythonClass<TLazyYsonIterator>
{
public:
    TLazyYsonIterator(Py::PythonClassInstance *self, Py::Tuple& args, Py::Dict& kwargs)
        : Py::PythonClass<TLazyYsonIterator>::PythonClass(self, args, kwargs)
    { }

    void Init(
        IInputStream* inputStream,
        std::unique_ptr<IInputStream> inputStreamOwner,
        Py::Tuple& loadsParams,
        const TNullable<TString>& encoding,
        bool alwaysCreateAttributes)
    {
        InputStreamOwner_ = std::move(inputStreamOwner);
        KeyCache_ = TPythonStringCache(true, encoding);
        Parser_.reset(new TLazyListFragmentParser(inputStream, encoding, alwaysCreateAttributes, &KeyCache_));
    }

    Py::Object iter()
    {
        return self();
    }

    PyObject* iternext()
    {
        try {
            auto item = Parser_->NextItem();
            if (!item) {
                PyErr_SetNone(PyExc_StopIteration);
                return nullptr;
            }
            return item;
        } CATCH("Yson load failed");
    }

    virtual ~TLazyYsonIterator()
    { }

    static void InitType()
    {
        behaviors().name("Yson iterator");
        behaviors().doc("Iterates over stream with YSON rows");
        behaviors().supportGetattro();
        behaviors().supportSetattro();
        behaviors().supportIter();

        behaviors().readyType();
    }

private:
    std::unique_ptr<IInputStream> InputStreamOwner_;
    std::unique_ptr<TLazyListFragmentParser> Parser_;
    TPythonStringCache KeyCache_;
};

////////////////////////////////////////////////////////////////////////////////

class TYsonModule
    : public Py::ExtensionModule<TYsonModule>
{
public:
    TYsonModule()
        // This should match .so file name.
        : Py::ExtensionModule<TYsonModule>("yson_lib")
    {
        PyEval_InitThreads();

        RegisterShutdown();
        InstallCrashSignalHandler(std::set<int>({SIGSEGV}));

        TYsonIterator::InitType();
        TRawYsonIterator::InitType();
        TLazyYsonIterator::InitType();
        PyType_Ready(TLazyYsonMapBaseType);
        PyType_Ready(TLazyYsonMapType);

        YsonLazyMapBaseClass = TPythonClassObject(TLazyYsonMapBaseType);
        YsonLazyMapClass = TPythonClassObject(TLazyYsonMapType);

        add_keyword_method("load", &TYsonModule::Load, "Loads YSON from stream");
        add_keyword_method("loads", &TYsonModule::Loads, "Loads YSON from string");

        add_keyword_method("dump", &TYsonModule::Dump, "Dumps YSON to stream");
        add_keyword_method("dumps", &TYsonModule::Dumps, "Dumps YSON to string");

        add_keyword_method("loads_proto", &TYsonModule::LoadsProto, "Loads proto message from yson string");
        add_keyword_method("dumps_proto", &TYsonModule::DumpsProto, "Dumps proto message to yson string");

        initialize("Python bindings for YSON");
    }

    Py::Object Load(const Py::Tuple& args_, const Py::Dict& kwargs_)
    {
        auto args = args_;
        auto kwargs = kwargs_;

        try {
            return LoadImpl(args, kwargs, nullptr);
        } CATCH("Yson load failed");
    }

    Py::Object Loads(const Py::Tuple& args_, const Py::Dict& kwargs_)
    {
        auto args = args_;
        auto kwargs = kwargs_;

        auto stringArgument = ExtractArgument(args, kwargs, "string");
#if PY_MAJOR_VERSION >= 3
        if (PyUnicode_Check(stringArgument.ptr())) {
            throw Py::TypeError("Only binary strings parsing is supported, got unicode");
        }
#endif
        auto string = ConvertStringObjectToString(stringArgument);
        std::unique_ptr<IInputStream> stringStream(new TOwningStringInput(string));

        try {
            return LoadImpl(args, kwargs, std::move(stringStream));
        } CATCH("Yson load failed");
    }

    Py::Object Dump(const Py::Tuple& args_, const Py::Dict& kwargs_)
    {
        auto args = args_;
        auto kwargs = kwargs_;

        try {
            DumpImpl(args, kwargs, nullptr);
        } CATCH("Yson dumps failed");

        return Py::None();
    }

    Py::Object Dumps(const Py::Tuple& args_, const Py::Dict& kwargs_)
    {
        auto args = args_;
        auto kwargs = kwargs_;

        TString result;
        TStringOutput stringOutput(result);

        try {
            DumpImpl(args, kwargs, &stringOutput);
        } CATCH("Yson dumps failed");

        return Py::ConvertToPythonString(result);
    }

    Py::Object DumpsProto(const Py::Tuple& args_, const Py::Dict& kwargs_)
    {
        auto args = args_;
        auto kwargs = kwargs_;

        auto protoObject = ExtractArgument(args, kwargs, "proto");
        ValidateArgumentsEmpty(args, kwargs);

        try {
            return DumpsProtoImpl(protoObject);
        } CATCH("Yson dumps_proto failed");
    }

    Py::Object LoadsProto(const Py::Tuple& args_, const Py::Dict& kwargs_)
    {
        auto args = args_;
        auto kwargs = kwargs_;

        auto stringObject = Py::Bytes(ExtractArgument(args, kwargs, "string"));
        auto protoClassObject = Py::Callable(ExtractArgument(args, kwargs, "proto_class"));
        ValidateArgumentsEmpty(args, kwargs);

        try {
            return LoadsProtoImpl(stringObject, protoClassObject);
        } CATCH("Yson loads_proto failed");
    }

    virtual ~TYsonModule()
    { }

private:
    Py::Object LoadImpl(
        Py::Tuple& args,
        Py::Dict& kwargs,
        std::unique_ptr<IInputStream> inputStream)
    {
        // Holds inputStreamWrap if passed non-trivial stream argument
        IInputStream* inputStreamPtr;
        if (!inputStream) {
            auto streamArg = ExtractArgument(args, kwargs, "stream");
            bool wrapStream = true;
#if PY_MAJOR_VERSION < 3
            if (PyFile_Check(streamArg.ptr())) {
                FILE* file = PyFile_AsFile(streamArg.ptr());
                inputStream.reset(new TFileInput(Duplicate(file)));
                wrapStream = false;
            }
#endif
            if (wrapStream) {
                inputStream.reset(new TInputStreamWrap(streamArg));
            }
        }
        inputStreamPtr = inputStream.get();

        auto ysonType = NYson::EYsonType::Node;
        if (HasArgument(args, kwargs, "yson_type")) {
            auto arg = ExtractArgument(args, kwargs, "yson_type");
            ysonType = ParseEnum<NYson::EYsonType>(ConvertStringObjectToString(arg));
        }

        bool alwaysCreateAttributes = true;
        if (HasArgument(args, kwargs, "always_create_attributes")) {
            auto arg = ExtractArgument(args, kwargs, "always_create_attributes");
            alwaysCreateAttributes = Py::Boolean(arg);
        }

        bool raw = false;
        if (HasArgument(args, kwargs, "raw")) {
            auto arg = ExtractArgument(args, kwargs, "raw");
            raw = Py::Boolean(arg);
        }

        TNullable<TString> encoding;
        if (HasArgument(args, kwargs, "encoding")) {
            auto arg = ExtractArgument(args, kwargs, "encoding");
            if (!arg.isNone()) {
#if PY_MAJOR_VERSION < 3
                throw CreateYsonError("Encoding parameter is not supported for Python 2");
#else
                encoding = ConvertStringObjectToString(arg);
#endif
            }
#if PY_MAJOR_VERSION >= 3
        } else {
            encoding = "utf-8";
#endif
        }

        bool lazy = false;
        if (HasArgument(args, kwargs, "lazy")) {
            auto arg = ExtractArgument(args, kwargs, "lazy");
            lazy = Py::Boolean(arg);
        }

        ValidateArgumentsEmpty(args, kwargs);

        if (lazy) {
            if (raw) {
                throw CreateYsonError("Raw mode is not supported in lazy mode");
            }

            Py::Object encodingParam;
            if (encoding) {
                encodingParam = Py::String(encoding.Get());
            } else {
                encodingParam = Py::None();
            }
            Py::TupleN params(encodingParam, Py::Boolean(alwaysCreateAttributes));
            if (ysonType == NYson::EYsonType::ListFragment) {
                Py::Callable classType(TLazyYsonIterator::type());
                Py::PythonClassObject<TLazyYsonIterator> pythonIter(classType.apply(Py::Tuple(), Py::Dict()));

                auto* iter = pythonIter.getCxxObject();
                iter->Init(inputStreamPtr, std::move(inputStream), params, encoding, alwaysCreateAttributes);
                return pythonIter;
            }

            return ParseLazyYson(inputStreamPtr, encoding, alwaysCreateAttributes, ysonType);
        }

        if (ysonType == NYson::EYsonType::ListFragment) {
            if (raw) {
                Py::Callable classType(TRawYsonIterator::type());
                Py::PythonClassObject<TRawYsonIterator> pythonIter(classType.apply(Py::Tuple(), Py::Dict()));

                auto* iter = pythonIter.getCxxObject();
                iter->Init(inputStreamPtr, std::move(inputStream));
                return pythonIter;
            } else {
                Py::Callable classType(TYsonIterator::type());
                Py::PythonClassObject<TYsonIterator> pythonIter(classType.apply(Py::Tuple(), Py::Dict()));

                auto* iter = pythonIter.getCxxObject();
                iter->Init(inputStreamPtr, std::move(inputStream), alwaysCreateAttributes, encoding);
                return pythonIter;
            }

        } else {
            if (raw) {
                throw CreateYsonError("Raw mode is only supported for list fragments");
            }
            NYTree::TPythonObjectBuilder consumer(alwaysCreateAttributes, encoding);
            NYson::TYsonParser parser(&consumer, ysonType);

            TBlob buffer(TDefaultBlobTag(), BufferSize, /*initiailizeStorage*/ false);

            if (ysonType == NYson::EYsonType::MapFragment) {
                consumer.OnBeginMap();
            }
            while (int length = inputStreamPtr->Read(buffer.Begin(), BufferSize))
            {
                parser.Read(TStringBuf(buffer.Begin(), length));
                if (BufferSize != length) {
                    break;
                }
            }
            parser.Finish();
            if (ysonType == NYson::EYsonType::MapFragment) {
                consumer.OnEndMap();
            }

            return consumer.ExtractObject();
        }
    }

    void DumpImpl(Py::Tuple& args, Py::Dict& kwargs, IOutputStream* outputStream)
    {
        auto obj = ExtractArgument(args, kwargs, "object");

        // Holds outputStreamWrap if passed non-trivial stream argument
        std::unique_ptr<TOutputStreamWrap> outputStreamWrap;
        std::unique_ptr<TFileOutput> fileOutput;
        std::unique_ptr<TBufferedOutput> bufferedOutputStream;

        if (!outputStream) {
            auto streamArg = ExtractArgument(args, kwargs, "stream");
            bool wrapStream = true;
#if PY_MAJOR_VERSION < 3
            if (PyFile_Check(streamArg.ptr())) {
                FILE* file = PyFile_AsFile(streamArg.ptr());
                fileOutput.reset(new TFileOutput(Duplicate(file)));
                outputStream = fileOutput.get();
                wrapStream = false;
            }
#endif
            if (wrapStream) {
                outputStreamWrap.reset(new TOutputStreamWrap(streamArg));
                outputStream = outputStreamWrap.get();
            }
#if PY_MAJOR_VERSION < 3
            // Python 3 has "io" module with fine-grained buffering control, no need in
            // additional buferring here.
            bufferedOutputStream.reset(new TBufferedOutput(outputStream, 1024 * 1024));
            outputStream = bufferedOutputStream.get();
#endif
        }

        auto ysonFormat = NYson::EYsonFormat::Text;
        if (HasArgument(args, kwargs, "yson_format")) {
            auto arg = ExtractArgument(args, kwargs, "yson_format");
            ysonFormat = ParseEnum<NYson::EYsonFormat>(ConvertStringObjectToString(arg));
        }

        NYson::EYsonType ysonType = NYson::EYsonType::Node;
        if (HasArgument(args, kwargs, "yson_type")) {
            auto arg = ExtractArgument(args, kwargs, "yson_type");
            ysonType = ParseEnum<NYson::EYsonType>(ConvertStringObjectToString(arg));
        }

        int indent = NYson::TYsonWriter::DefaultIndent;
        const int maxIndentValue = 128;

        if (HasArgument(args, kwargs, "indent")) {
            auto arg = Py::Int(ExtractArgument(args, kwargs, "indent"));
            if (arg > maxIndentValue) {
                throw CreateYsonError(Format("Indent value exceeds indentation limit (%d)", maxIndentValue));
            }
            indent = static_cast<int>(Py::Long(arg).as_long());
        }

        bool booleanAsString = false;
        if (HasArgument(args, kwargs, "boolean_as_string")) {
            auto arg = ExtractArgument(args, kwargs, "boolean_as_string");
            booleanAsString = Py::Boolean(arg);
        }

        bool ignoreInnerAttributes = false;
        if (HasArgument(args, kwargs, "ignore_inner_attributes")) {
            auto arg = ExtractArgument(args, kwargs, "ignore_inner_attributes");
            ignoreInnerAttributes = Py::Boolean(arg);
        }

        TNullable<TString> encoding("utf-8");
        if (HasArgument(args, kwargs, "encoding")) {
            auto arg = ExtractArgument(args, kwargs, "encoding");
            if (arg.isNone()) {
                encoding = Null;
            } else {
                encoding = ConvertStringObjectToString(arg);
            }
        }

        ValidateArgumentsEmpty(args, kwargs);

        auto writer = NYson::CreateYsonWriter(
            outputStream,
            ysonFormat,
            ysonType,
            false,
            booleanAsString,
            indent);

        switch (ysonType) {
            case NYson::EYsonType::Node:
            case NYson::EYsonType::MapFragment:
                Serialize(obj, writer.get(), encoding, ignoreInnerAttributes, ysonType);
                break;

            case NYson::EYsonType::ListFragment: {
                auto iterator = Py::Object(PyObject_GetIter(obj.ptr()), true);
                size_t rowIndex = 0;
                TContext context;
                while (auto* item = PyIter_Next(*iterator)) {
                    context.RowIndex = rowIndex;
                    Serialize(Py::Object(item, true), writer.get(), encoding, ignoreInnerAttributes, NYson::EYsonType::Node, 0, &context);
                    ++rowIndex;
                }
                if (PyErr_Occurred()) {
                    throw Py::Exception();
                }
                break;
            }

            default:
                throw CreateYsonError("YSON type " + ToString(ysonType) + " is not supported");
        }

        writer->Flush();

        if (bufferedOutputStream) {
            bufferedOutputStream->Flush();
        }
    }

    void RegisterFileDescriptor(Py::Object fileDescriptor)
    {
        auto name = ConvertStringObjectToString(GetAttr(fileDescriptor, "name"));
        if (GetDescriptorPool()->FindFileByName(name)) {
            return;
        }

        auto dependencies = GetAttr(fileDescriptor, "dependencies");
        auto iterator = Py::Object(PyObject_GetIter(dependencies.ptr()), true);
        while (auto* item = PyIter_Next(*iterator)) {
            RegisterFileDescriptor(Py::Object(item, true));
        }
        if (PyErr_Occurred()) {
            throw Py::Exception();
        }

        auto serializedFileDescriptor = ConvertStringObjectToString(GetAttr(fileDescriptor, "serialized_pb"));
        ::google::protobuf::FileDescriptorProto fileDescriptorProto;
        fileDescriptorProto.ParseFromArray(serializedFileDescriptor.begin(), serializedFileDescriptor.size());

        auto result = GetDescriptorPool()->BuildFile(fileDescriptorProto);
        YCHECK(result);
    }

    Py::Object DumpsProtoImpl(Py::Object protoObject)
    {
        auto serializeToString = Py::Callable(GetAttr(protoObject, "SerializeToString"));
        auto serializedProto = Py::String(serializeToString.apply(Py::Tuple(), Py::Dict()));
        auto serializedStringBuf = ConvertToStringBuf(serializedProto);

        auto descriptorObject = GetAttr(protoObject, "DESCRIPTOR");
        RegisterFileDescriptor(GetAttr(descriptorObject, "file"));

        auto fullName = ConvertStringObjectToString(GetAttr(descriptorObject, "full_name"));
        auto* descriptor = GetDescriptorPool()->FindMessageTypeByName(fullName);
        auto* messageType = ReflectProtobufMessageType(descriptor);

        ::google::protobuf::io::ArrayInputStream inputStream(serializedStringBuf.begin(), serializedStringBuf.size());

        TString result;
		TStringOutput outputStream(result);
		TYsonWriter writer(&outputStream);
		ParseProtobuf(&writer, &inputStream, messageType);

        return Py::ConvertToPythonString(result);
    }

    Py::Object LoadsProtoImpl(Py::Object stringObject, Py::Object protoClassObject)
    {
        auto descriptorObject = GetAttr(protoClassObject, "DESCRIPTOR");
        RegisterFileDescriptor(GetAttr(descriptorObject, "file"));

        auto fullName = ConvertStringObjectToString(GetAttr(descriptorObject, "full_name"));
        auto* descriptor = GetDescriptorPool()->FindMessageTypeByName(fullName);
        auto* messageType = ReflectProtobufMessageType(descriptor);

        TString result;
        ::google::protobuf::io::StringOutputStream outputStream(&result);

        auto writer = CreateProtobufWriter(&outputStream, messageType);

        ParseYsonStringBuffer(ConvertToStringBuf(stringObject), EYsonType::Node, writer.get());

        return Py::Callable(GetAttr(protoClassObject, "FromString")).apply(
            Py::TupleN(Py::ConvertToPythonString(result)),
            Py::Dict());
    }

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NPython
} // namespace NYT

////////////////////////////////////////////////////////////////////////////////

#if defined( _WIN32 )
#define EXPORT_SYMBOL __declspec( dllexport )
#else
#define EXPORT_SYMBOL
#endif

static PyObject* init_module()
{
    static NYT::NPython::TYsonModule* yson = new NYT::NPython::TYsonModule;
    return yson->module().ptr();
}

#if PY_MAJOR_VERSION < 3
extern "C" EXPORT_SYMBOL void inityson_lib() { Y_UNUSED(init_module()); }
extern "C" EXPORT_SYMBOL void inityson_lib_d() { inityson_lib(); }
#else
extern "C" EXPORT_SYMBOL PyObject* PyInit_yson_lib() { return init_module(); }
extern "C" EXPORT_SYMBOL PyObject* PyInit_yson_lib_d() { return PyInit_yson_lib(); }
#endif
