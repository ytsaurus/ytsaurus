#include "object_builder.h"
#include "serialize.h"
#include "lazy_parser.h"
#include "lazy_yson_consumer.h"
#include "lazy_list_fragment_parser.h"
#include "yson_lazy_map.h"
#include "protobuf_descriptor_pool.h"
#include "list_fragment_parser.h"
#include "error.h"
#include "helpers.h"

#include "skiff/schema.h"
#include "skiff/record.h"
#include "skiff/parser.h"
#include "skiff/serialize.h"
#include "skiff/switch.h"
#include "skiff/raw_iterator.h"

#include <yt/python/common/shutdown.h>
#include <yt/python/common/helpers.h>

#include <yt/core/ytree/convert.h>

#include <yt/core/yson/protobuf_interop.h>

#include <yt/core/misc/crash_handler.h>

#include <contrib/libs/protobuf/io/coded_stream.h>
#include <contrib/libs/protobuf/io/zero_copy_stream_impl_lite.h>

#include <contrib/libs/protobuf/google/protobuf/descriptor.pb.h>

namespace NYT {
namespace NPython {

using namespace NYTree;
using namespace NYson;

static constexpr int BufferSize = 1024 * 1024;

////////////////////////////////////////////////////////////////////////////////

class TYsonIterator
    : public TRowsIteratorBase<TYsonIterator, NYTree::TPythonObjectBuilder, NYson::TYsonParser>
{
public:
    TYsonIterator(Py::PythonClassInstance *self, Py::Tuple& args, Py::Dict& kwargs)
        : TBase::TRowsIteratorBase(self, args, kwargs, FormatName)
    { }

    void Init(
        IInputStream* inputStream,
        std::unique_ptr<IInputStream> inputStreamHolder,
        bool alwaysCreateAttributes,
        const TNullable<TString>& encoding)
    {
        YCHECK(!inputStreamHolder || inputStreamHolder.get() == inputStream);

        InputStream_ = inputStream;
        InputStreamHolder_ = std::move(inputStreamHolder);
        Consumer_.reset(new TPythonObjectBuilder(alwaysCreateAttributes, encoding));
        Parser_.reset(new TYsonParser(Consumer_.get(), EYsonType::ListFragment));
    }

    static void InitType()
    {
        TBase::InitType(FormatName);
    }

private:
    using TBase = TRowsIteratorBase<TYsonIterator, NYTree::TPythonObjectBuilder, NYson::TYsonParser>;

    static constexpr const char FormatName[] = "Yson";

    std::unique_ptr<IInputStream> InputStreamHolder_;
};

constexpr const char TYsonIterator::FormatName[];

////////////////////////////////////////////////////////////////////////////////

class TRawYsonIterator
    : public Py::PythonClass<TRawYsonIterator>
{
public:
    TRawYsonIterator(Py::PythonClassInstance *self, Py::Tuple& args, Py::Dict& kwargs)
        : Py::PythonClass<TRawYsonIterator>::PythonClass(self, args, kwargs)
    { }

    void Init(IInputStream* inputStream, std::unique_ptr<IInputStream> inputStreamHolder)
    {
        InputStreamHolder_ = std::move(inputStreamHolder);
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
        } CATCH_AND_CREATE_YSON_ERROR("Yson load failed");
    }

    virtual ~TRawYsonIterator() = default;

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
    std::unique_ptr<IInputStream> InputStreamHolder_;
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
        std::unique_ptr<IInputStream> inputStreamHolder,
        Py::Tuple& loadsParams,
        const TNullable<TString>& encoding,
        bool alwaysCreateAttributes)
    {
        YCHECK(!inputStreamHolder || inputStreamHolder.get() == inputStream);

        InputStreamHolder_ = std::move(inputStreamHolder);
        KeyCache_ = TPythonStringCache(/* enable */ true, encoding);
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
        } CATCH_AND_CREATE_YSON_ERROR("Yson load failed");
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
    std::unique_ptr<IInputStream> InputStreamHolder_;
    std::unique_ptr<TLazyListFragmentParser> Parser_;
    TPythonStringCache KeyCache_;
};

////////////////////////////////////////////////////////////////////////////////

class TYsonModule
    : public Py::ExtensionModule<TYsonModule>
{
public:
    TYsonModule()
        // This name should match .so file name.
        : Py::ExtensionModule<TYsonModule>("yson_lib")
    {
        PyEval_InitThreads();

        RegisterShutdown();
        InstallCrashSignalHandler(std::set<int>({SIGSEGV}));

        TYsonIterator::InitType();
        TRawYsonIterator::InitType();
        TLazyYsonIterator::InitType();
        TSkiffRecordPython::InitType();
        TSkiffRecordItemsIterator::InitType();
        TSkiffSchemaPython::InitType();
        TSkiffTableSwitchPython::InitType();
        TSkiffIterator::InitType();
        TSkiffRawIterator::InitType();

        PyType_Ready(TLazyYsonMapBaseType);
        PyType_Ready(TLazyYsonMapType);

        YsonLazyMapBaseClass = TPythonClassObject(TLazyYsonMapBaseType);
        YsonLazyMapClass = TPythonClassObject(TLazyYsonMapType);

        add_keyword_method("is_debug_build", &TYsonModule::IsDebugBuild, "Check if module was built in debug mode");

        add_keyword_method("load", &TYsonModule::Load, "Loads YSON from stream");
        add_keyword_method("loads", &TYsonModule::Loads, "Loads YSON from string");

        add_keyword_method("dump", &TYsonModule::Dump, "Dumps YSON to stream");
        add_keyword_method("dumps", &TYsonModule::Dumps, "Dumps YSON to string");

        add_keyword_method("loads_proto", &TYsonModule::LoadsProto, "Loads proto message from yson string");
        add_keyword_method("dumps_proto", &TYsonModule::DumpsProto, "Dumps proto message to yson string");

        add_keyword_method("load_skiff", &TYsonModule::LoadSkiff, "Loads Skiff from stream");
        add_keyword_method("dump_skiff", &TYsonModule::DumpSkiff, "Dumps Skiff to stream");

        initialize("Python bindings for YSON");

        Py::Dict moduleDict(moduleDictionary());
        Py::Object skiffRecordClass(TSkiffRecordPython::type());
        Py::Object skiffSchemaClass(TSkiffSchemaPython::type());
        Py::Object skiffTableSwitchClass(TSkiffTableSwitchPython::type());
        moduleDict["SkiffRecord"] = skiffRecordClass;
        moduleDict["SkiffSchema"] = skiffSchemaClass;
        moduleDict["SkiffTableSwitch"] = skiffTableSwitchClass;
    }

    Py::Object IsDebugBuild(const Py::Tuple& /*args_*/, const Py::Dict& /*kwargs_*/)
    {
#if defined(NDEBUG)
        return Py::Boolean(false);
#else
        return Py::Boolean(true);
#endif
    }

    Py::Object Load(const Py::Tuple& args_, const Py::Dict& kwargs_)
    {
        auto args = args_;
        auto kwargs = kwargs_;

        try {
            auto streamArg = ExtractArgument(args, kwargs, "stream");
            return LoadImpl(args, kwargs, CreateInputStreamWrapper(streamArg));
        } CATCH_AND_CREATE_YSON_ERROR("Yson load failed");
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
        auto stringStream = CreateOwningStringInput(std::move(string));

        try {
            return LoadImpl(args, kwargs, std::move(stringStream));
        } CATCH_AND_CREATE_YSON_ERROR("Yson load failed");
    }

    Py::Object Dump(const Py::Tuple& args_, const Py::Dict& kwargs_)
    {
        auto args = args_;
        auto kwargs = kwargs_;

        try {
            DumpImpl(args, kwargs, nullptr);
        } CATCH_AND_CREATE_YSON_ERROR("Yson dump failed");

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
        } CATCH_AND_CREATE_YSON_ERROR("Yson dumps failed");

        return Py::ConvertToPythonString(result);
    }

    Py::Object DumpsProto(const Py::Tuple& args_, const Py::Dict& kwargs_)
    {
        auto args = args_;
        auto kwargs = kwargs_;

        auto protoObject = ExtractArgument(args, kwargs, "proto");

        TNullable<bool> skipUnknownFields;
        if (HasArgument(args, kwargs, "skip_unknown_fields")) {
            auto arg = ExtractArgument(args, kwargs, "skip_unknown_fields");
            skipUnknownFields = Py::Boolean(arg);
        }

        ValidateArgumentsEmpty(args, kwargs);

        try {
            return DumpsProtoImpl(protoObject, skipUnknownFields);
        } CATCH_AND_CREATE_YSON_ERROR("Yson dumps_proto failed");
    }

    Py::Object LoadsProto(const Py::Tuple& args_, const Py::Dict& kwargs_)
    {
        auto args = args_;
        auto kwargs = kwargs_;

        auto stringObject = Py::Bytes(ExtractArgument(args, kwargs, "string"));
        auto protoClassObject = Py::Callable(ExtractArgument(args, kwargs, "proto_class"));

        TNullable<bool> skipUnknownFields;
        if (HasArgument(args, kwargs, "skip_unknown_fields")) {
            auto arg = ExtractArgument(args, kwargs, "skip_unknown_fields");
            skipUnknownFields = Py::Boolean(arg);
        }

        ValidateArgumentsEmpty(args, kwargs);

        try {
            return LoadsProtoImpl(stringObject, protoClassObject, skipUnknownFields);
        } CATCH_AND_CREATE_YSON_ERROR("Yson loads_proto failed");
    }

    Py::Object LoadSkiff(const Py::Tuple& args_, const Py::Dict& kwargs_)
    {
        auto args = args_;
        auto kwargs = kwargs_;
        return NPython::LoadSkiff(args, kwargs);
    }

    Py::Object DumpSkiff(const Py::Tuple& args_, const Py::Dict& kwargs_)
    {
        auto args = args_;
        auto kwargs = kwargs_;
        return NPython::DumpSkiff(args, kwargs);
    }

    virtual ~TYsonModule()
    { }

private:
    Py::Object LoadImpl(
        Py::Tuple& args,
        Py::Dict& kwargs,
        std::unique_ptr<IInputStream> inputStreamHolder)
    {
        IInputStream* inputStream = inputStreamHolder.get();

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

        auto encoding = ParseEncodingArgument(args, kwargs);

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
                iter->Init(inputStream, std::move(inputStreamHolder), params, encoding, alwaysCreateAttributes);
                return pythonIter;
            } else {
                return ParseLazyYson(inputStream, encoding, alwaysCreateAttributes, ysonType);
            }
        }

        if (ysonType == NYson::EYsonType::ListFragment) {
            if (raw) {
                Py::Callable classType(TRawYsonIterator::type());
                Py::PythonClassObject<TRawYsonIterator> pythonIter(classType.apply(Py::Tuple(), Py::Dict()));

                auto* iter = pythonIter.getCxxObject();
                iter->Init(inputStream, std::move(inputStreamHolder));
                return pythonIter;
            } else {
                Py::Callable classType(TYsonIterator::type());
                Py::PythonClassObject<TYsonIterator> pythonIter(classType.apply(Py::Tuple(), Py::Dict()));

                auto* iter = pythonIter.getCxxObject();
                iter->Init(inputStream, std::move(inputStreamHolder), alwaysCreateAttributes, encoding);
                return pythonIter;
            }
        } else {
            if (raw) {
                throw CreateYsonError("Raw mode is only supported for list fragments");
            }
            NYTree::TPythonObjectBuilder consumer(alwaysCreateAttributes, encoding);

            auto parse = [&] {ParseYson(TYsonInput(inputStream, ysonType), &consumer);};

            if (ysonType == NYson::EYsonType::MapFragment) {
                consumer.OnBeginMap();
                parse();
                consumer.OnEndMap();
            } else {
                parse();
            }

            return consumer.ExtractObject();
        }
    }

    void DumpImpl(Py::Tuple& args, Py::Dict& kwargs, IOutputStream* outputStream)
    {
        auto obj = ExtractArgument(args, kwargs, "object");

        std::unique_ptr<IOutputStream> outputStreamHolder;
        if (!outputStream) {
            auto streamArg = ExtractArgument(args, kwargs, "stream");
            outputStreamHolder = CreateOutputStreamWrapper(streamArg, /* addBuffering */ true);
            outputStream = outputStreamHolder.get();
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
                auto iterator = CreateIterator(obj);
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
    }

    void RegisterFileDescriptor(Py::Object fileDescriptor)
    {
        auto name = ConvertStringObjectToString(GetAttr(fileDescriptor, "name"));
        if (GetDescriptorPool()->FindFileByName(name)) {
            return;
        }

        auto dependencies = GetAttr(fileDescriptor, "dependencies");
        auto iterator = CreateIterator(dependencies);
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

    Py::Object DumpsProtoImpl(Py::Object protoObject, TNullable<bool> skipUnknownFields)
    {
        auto serializeToString = Py::Callable(GetAttr(protoObject, "SerializeToString"));
        auto serializedProto = Py::Bytes(serializeToString.apply(Py::Tuple(), Py::Dict()));
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

        TProtobufParserOptions options;
        if (skipUnknownFields) {
            options.SkipUnknownFields = *skipUnknownFields;
        } else {
            options.SkipUnknownFields = true;
        }

        ParseProtobuf(&writer, &inputStream, messageType, options);

        return Py::ConvertToPythonString(result);
    }

    Py::Object LoadsProtoImpl(Py::Object stringObject, Py::Object protoClassObject, TNullable<bool> skipUnknownFields)
    {
        auto descriptorObject = GetAttr(protoClassObject, "DESCRIPTOR");
        RegisterFileDescriptor(GetAttr(descriptorObject, "file"));

        auto fullName = ConvertStringObjectToString(GetAttr(descriptorObject, "full_name"));
        auto* descriptor = GetDescriptorPool()->FindMessageTypeByName(fullName);
        auto* messageType = ReflectProtobufMessageType(descriptor);

        TString result;
        ::google::protobuf::io::StringOutputStream outputStream(&result);

        TProtobufWriterOptions options;
        if (skipUnknownFields) {
            options.SkipUnknownFields = *skipUnknownFields;
        } else {
            options.SkipUnknownFields = true;
        }
        auto writer = CreateProtobufWriter(&outputStream, messageType, options);

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
