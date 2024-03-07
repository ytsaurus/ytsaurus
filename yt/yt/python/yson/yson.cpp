#include "serialize.h"
#include "yson_lazy_map.h"
#include "protobuf_descriptor_pool.h"
#include "list_fragment_parser.h"
#include "error.h"
#include "helpers.h"
#include "limited_yson_writer.h"
#include "pull_object_builder.h"

#include "arrow.h"

#include "skiff/schema.h"
#include "skiff/record.h"
#include "skiff/parser.h"
#include "skiff/other_columns.h"
#include "skiff/serialize.h"
#include "skiff/switch.h"
#include "skiff/raw_iterator.h"
#include "skiff/structured_iterator.h"

#include <yt/yt/python/common/shutdown.h>
#include <yt/yt/python/common/helpers.h>
#include <yt/yt/python/common/stream.h>

#include <yt/yt/core/ytree/convert.h>

#include <yt/yt/core/yson/protobuf_interop.h>
#include <yt/yt/core/yson/null_consumer.h>
#include <yt/yt/core/yson/pull_parser.h>

#include <yt/yt/core/misc/crash_handler.h>
#include <yt/yt/core/misc/signal_registry.h>

#include <google/protobuf/io/coded_stream.h>
#include <google/protobuf/io/zero_copy_stream_impl_lite.h>

#include <google/protobuf/descriptor.pb.h>

#include <util/stream/zerocopy.h>

namespace NYT::NPython {

using namespace NYTree;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

static constexpr int YsonParserNestingLevelLimit = 256;

////////////////////////////////////////////////////////////////////////////////

class TYsonIterator
    : public Py::PythonClass<TYsonIterator>
{
public:
    TYsonIterator(Py::PythonClassInstance *self, Py::Tuple& args, Py::Dict& kwargs)
        : TBase::PythonClass(self, args, kwargs)
    { }

    void Init(
        IZeroCopyInput* inputStream,
        std::unique_ptr<IZeroCopyInput> inputStreamHolder,
        bool alwaysCreateAttributes,
        const std::optional<TString>& encoding)
    {
        YT_VERIFY(!inputStreamHolder || inputStreamHolder.get() == inputStream);

        InputStreamHolder_ = std::move(inputStreamHolder);
        Parser_.reset(new TYsonPullParser(inputStream, EYsonType::ListFragment, YsonParserNestingLevelLimit));
        ObjectBuilder_.reset(new TPullObjectBuilder(Parser_.get(), alwaysCreateAttributes, encoding));
    }

    static void InitType()
    {
        Name_ = TString(FormatName) + "Iterator";
        Doc_ = "Iterates over stream with " + TString(FormatName) + " rows";
        TypeName_ = "yt_yson_bindings.yson_lib." + Name_;
        TBase::behaviors().name(TypeName_.c_str());
        TBase::behaviors().doc(Doc_.c_str());
        TBase::behaviors().supportGetattro();
        TBase::behaviors().supportSetattro();
        TBase::behaviors().supportIter();

        TBase::behaviors().readyType();
    }

    Py::Object iter() override
    {
        return TBase::self();
    }

    PyObject* iternext() override
    {
        YT_VERIFY(InputStreamHolder_);
        YT_VERIFY(Parser_);
        YT_VERIFY(ObjectBuilder_);

        try {
            auto result = ObjectBuilder_->ParseObject();
            return result.release();
        } CATCH_AND_CREATE_YSON_ERROR(TString(FormatName) + " load failed");
    }

    using TBase = Py::PythonClass<TYsonIterator>;

protected:
    static void InitType(const TString& formatName);

    static TString Name_;
    static TString Doc_;
    static TString TypeName_;

private:
    static constexpr const char FormatName[] = "Yson";

    std::unique_ptr<IZeroCopyInput> InputStreamHolder_;
    std::unique_ptr<NYson::TYsonPullParser> Parser_;
    std::unique_ptr<NPython::TPullObjectBuilder> ObjectBuilder_;
};

constexpr const char TYsonIterator::FormatName[];

TString TYsonIterator::Name_ = TString();
TString TYsonIterator::Doc_ = TString();
TString TYsonIterator::TypeName_ = TString();

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
        Parser_ = TListFragmentParser(inputStream, YsonParserNestingLevelLimit);
    }

    Py::Object iter() override
    {
        return self();
    }

    PyObject* iternext() override
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
        behaviors().name("yt_yson_bindings.yson_lib.RawYsonIterator");
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
        IZeroCopyInput* inputStream,
        std::unique_ptr<IZeroCopyInput> inputStreamHolder,
        const std::optional<TString>& encoding,
        bool alwaysCreateAttributes)
    {
        YT_VERIFY(!inputStreamHolder || inputStreamHolder.get() == inputStream);


        InputStreamHolder_ = std::move(inputStreamHolder);
        Parser_.reset(new TYsonPullParser(inputStream, EYsonType::ListFragment, YsonParserNestingLevelLimit));
        ObjectBuilder_.reset(new TPullObjectBuilder(Parser_.get(), alwaysCreateAttributes, encoding));
    }

    Py::Object iter() override
    {
        return self();
    }

    PyObject* iternext() override
    {
        try {
            auto result = ObjectBuilder_->ParseObjectLazy().release();
            if (!result) {
                PyErr_SetNone(PyExc_StopIteration);
                return nullptr;
            }
            return result;
        } CATCH_AND_CREATE_YSON_ERROR("Yson load failed");

    }

    virtual ~TLazyYsonIterator()
    { }

    static void InitType()
    {
        behaviors().name("yt_yson_bindings.yson_lib.LazyYsonIterator");
        behaviors().doc("Iterates over stream with YSON rows");
        behaviors().supportGetattro();
        behaviors().supportSetattro();
        behaviors().supportIter();

        behaviors().readyType();
    }

private:
    std::unique_ptr<IZeroCopyInput> InputStreamHolder_;
    std::unique_ptr<NYson::TYsonPullParser> Parser_;
    std::unique_ptr<NPython::TPullObjectBuilder> ObjectBuilder_;
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
#if PY_VERSION_HEX < 0x03090000
        PyEval_InitThreads();
#endif

        TSignalRegistry::Get()->SetOverrideNonDefaultSignalHandlers(false);
        TSignalRegistry::Get()->PushCallback(SIGSEGV, CrashSignalHandler);
        TSignalRegistry::Get()->PushDefaultSignalHandler(SIGSEGV);

        InitTLazyYsonMapType();

        TYsonIterator::InitType();
        TRawYsonIterator::InitType();
        TLazyYsonIterator::InitType();
        TSkiffRecordPython::InitType();
        TSkiffRecordItemsIterator::InitType();
        TSkiffSchemaPython::InitType();
        TSkiffTableSwitchPython::InitType();
        TSkiffIterator::InitType();
        TSkiffRawIterator::InitType();
        TSkiffStructuredIterator::InitType();
        TSkiffOtherColumns::InitType();

        InitArrowIteratorType();

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

        add_keyword_method("dump_parquet", &TYsonModule::DumpParquet, "Dumps Parquet to file from Arrow stream");
        add_keyword_method("upload_parquet", &TYsonModule::UploadParquet, "Uploads Parquet from file as Arrow to stream");

        add_keyword_method("load_skiff_structured", &TYsonModule::LoadSkiffStructured, "Loads Skiff rows from stream in structured form");
        add_keyword_method("dump_skiff_structured", &TYsonModule::DumpSkiffStructured, "Dumps Skiff rows to stream in structured form");

        initialize("Python bindings for YSON and Skiff");

        Py::Dict moduleDict(moduleDictionary());
        Py::Object skiffRecordClass(TSkiffRecordPython::type());
        Py::Object skiffSchemaClass(TSkiffSchemaPython::type());
        Py::Object skiffTableSwitchClass(TSkiffTableSwitchPython::type());
        Py::Object skiffOtherColumns(TSkiffOtherColumns::type());
        moduleDict.setItem("SkiffRecord", skiffRecordClass);
        moduleDict.setItem("SkiffSchema", skiffSchemaClass);
        moduleDict.setItem("SkiffTableSwitch", skiffTableSwitchClass);
        moduleDict.setItem("SkiffOtherColumns", skiffOtherColumns);

        RegisterShutdown();
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

        std::optional<bool> skipUnknownFields;
        if (HasArgument(args, kwargs, "skip_unknown_fields")) {
            auto arg = ExtractArgument(args, kwargs, "skip_unknown_fields");
            skipUnknownFields = Py::Boolean(arg);
        }

        auto ysonFormat = NYson::EYsonFormat::Binary;
        if (HasArgument(args, kwargs, "yson_format")) {
            auto arg = ExtractArgument(args, kwargs, "yson_format");
            ysonFormat = ParseEnum<NYson::EYsonFormat>(ConvertStringObjectToString(arg));
        }

        std::optional<i64> outputLimit;
        if (HasArgument(args, kwargs, "output_limit")) {
            auto arg = Py::Int(ExtractArgument(args, kwargs, "output_limit"));
            outputLimit = static_cast<i64>(Py::Long(arg).as_long());
        }

        ValidateArgumentsEmpty(args, kwargs);

        try {
            return DumpsProtoImpl(protoObject, skipUnknownFields, ysonFormat, outputLimit);
        } CATCH_AND_CREATE_YSON_ERROR("Yson dumps_proto failed");
    }

    Py::Object LoadsProto(const Py::Tuple& args_, const Py::Dict& kwargs_)
    {
        auto args = args_;
        auto kwargs = kwargs_;

        auto stringObject = Py::Bytes(ExtractArgument(args, kwargs, "string"));
        Py::Callable protoClassObject;
        bool hasProtoClass = HasArgument(args, kwargs, "proto_class");
        if (hasProtoClass) {
            protoClassObject = Py::Callable(ExtractArgument(args, kwargs, "proto_class"));
        }

        std::optional<bool> skipUnknownFields;
        if (HasArgument(args, kwargs, "skip_unknown_fields")) {
            auto arg = ExtractArgument(args, kwargs, "skip_unknown_fields");
            skipUnknownFields = Py::Boolean(arg);
        }
        Py::Object protoObject;
        bool hasProtoObject = HasArgument(args, kwargs, "proto_object");
        if (hasProtoObject && !hasProtoClass) {
            protoObject = Py::Object(ExtractArgument(args, kwargs, "proto_object"));
        } else if (!hasProtoObject && hasProtoClass) {
            protoObject = protoClassObject.apply(Py::Tuple());
        } else {
            throw Py::RuntimeError("Exactly one argument: 'proto_class' or 'proto_object' must be given");
        }

        ValidateArgumentsEmpty(args, kwargs);

        try {
            return LoadsProtoImpl(stringObject, protoObject, skipUnknownFields);
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

    Py::Object DumpParquet(const Py::Tuple& args_, const Py::Dict& kwargs_)
    {
        auto args = args_;
        auto kwargs = kwargs_;
        return NPython::DumpParquet(args, kwargs);
    }

    Py::Object UploadParquet(const Py::Tuple& args_, const Py::Dict& kwargs_)
    {
        auto args = args_;
        auto kwargs = kwargs_;
        return NPython::UploadParquet(args, kwargs);
    }

    Py::Object LoadSkiffStructured(const Py::Tuple& args_, const Py::Dict& kwargs_)
    {
        auto args = args_;
        auto kwargs = kwargs_;
        return NPython::LoadSkiffStructured(args, kwargs);
    }

    Py::Object DumpSkiffStructured(const Py::Tuple& args_, const Py::Dict& kwargs_)
    {
        auto args = args_;
        auto kwargs = kwargs_;
        return NPython::DumpSkiffStructured(args, kwargs);
    }

    virtual ~TYsonModule()
    { }

private:
    Py::Object LoadImpl(
        Py::Tuple& args,
        Py::Dict& kwargs,
        std::unique_ptr<IZeroCopyInput> inputStreamHolder)
    {
        IZeroCopyInput* inputStream = inputStreamHolder.get();

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
                encodingParam = Py::String(*encoding);
            } else {
                encodingParam = Py::None();
            }
            if (ysonType == NYson::EYsonType::ListFragment) {
                Py::Callable classType(TLazyYsonIterator::type());
                Py::PythonClassObject<TLazyYsonIterator> pythonIter(classType.apply(Py::Tuple(), Py::Dict()));

                auto* iter = pythonIter.getCxxObject();
                iter->Init(inputStream, std::move(inputStreamHolder), encoding, alwaysCreateAttributes);
                return pythonIter;
            } else {
                TYsonPullParser parser(inputStreamHolder.get(), ysonType, YsonParserNestingLevelLimit);
                TPullObjectBuilder builder(&parser, alwaysCreateAttributes, encoding);
                if (ysonType == NYson::EYsonType::MapFragment) {
                    return Py::Object(builder.ParseMapLazy(NYson::EYsonItemType::EndOfStream).release(), /* owned */ true);
                } else {
                    return Py::Object(builder.ParseObjectLazy().release(), /* owned */ true);
                }
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

            TYsonPullParser parser(inputStreamHolder.get(), ysonType, YsonParserNestingLevelLimit);
            TPullObjectBuilder builder(&parser, alwaysCreateAttributes, encoding);
            if (ysonType == NYson::EYsonType::MapFragment) {
                return Py::Object(builder.ParseMap(NYson::EYsonItemType::EndOfStream, alwaysCreateAttributes).release(), /* owned */ true);
            } else {
                return Py::Object(builder.ParseObject(alwaysCreateAttributes).release(), /* owned */ true);
            }
        }
    }

    void DumpImpl(Py::Tuple& args, Py::Dict& kwargs, IZeroCopyOutput* outputStream)
    {
        auto obj = ExtractArgument(args, kwargs, "object");

        std::unique_ptr<IZeroCopyOutput> outputStreamHolder;
        if (!outputStream) {
            auto streamArg = ExtractArgument(args, kwargs, "stream");
            outputStreamHolder = CreateZeroCopyOutputStreamWrapper(streamArg);
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
            auto arg = Py::Long(ExtractArgument(args, kwargs, "indent"));
            auto longIndent = arg.as_long();
            if (longIndent > maxIndentValue) {
                throw CreateYsonError(Format("Indent value exceeds indentation limit: %v > %v",
                    longIndent,
                    maxIndentValue));
            }
            indent = static_cast<int>(longIndent);
        }

        bool ignoreInnerAttributes = false;
        if (HasArgument(args, kwargs, "ignore_inner_attributes")) {
            auto arg = ExtractArgument(args, kwargs, "ignore_inner_attributes");
            ignoreInnerAttributes = Py::Boolean(arg);
        }

        std::optional<TString> encoding("utf-8");
        if (HasArgument(args, kwargs, "encoding")) {
            auto arg = ExtractArgument(args, kwargs, "encoding");
            if (arg.isNone()) {
                encoding = std::nullopt;
            } else {
                encoding = ConvertStringObjectToString(arg);
            }
        }

        bool sortKeys = false;
        if (HasArgument(args, kwargs, "sort_keys")) {
            auto arg = ExtractArgument(args, kwargs, "sort_keys");
            sortKeys = Py::Boolean(arg);
        }

        ValidateArgumentsEmpty(args, kwargs);

        auto writer = NYson::CreateYsonWriter(
            outputStream,
            ysonFormat,
            ysonType,
            /* enableRaw */ false,
            indent);

        switch (ysonType) {
            case NYson::EYsonType::Node:
            case NYson::EYsonType::MapFragment:
                Serialize(obj, writer.get(), encoding, ignoreInnerAttributes, ysonType, sortKeys);
                break;

            case NYson::EYsonType::ListFragment: {
                auto iterator = CreateIterator(obj);
                size_t rowIndex = 0;
                TContext context;
                while (auto* item = PyIter_Next(*iterator)) {
                    context.RowIndex = rowIndex;
                    Serialize(Py::Object(item, true), writer.get(), encoding, ignoreInnerAttributes, NYson::EYsonType::Node, sortKeys, 0, &context);
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
        Y_PROTOBUF_SUPPRESS_NODISCARD fileDescriptorProto.ParseFromArray(serializedFileDescriptor.begin(), serializedFileDescriptor.size());

        auto result = GetDescriptorPool()->BuildFile(fileDescriptorProto);
        YT_VERIFY(result);
    }

    Py::Object DumpsProtoImpl(Py::Object protoObject, std::optional<bool> skipUnknownFields, NYson::EYsonFormat ysonFormat, std::optional<i64> outputLimit)
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

        TProtobufParserOptions options;
        if (skipUnknownFields) {
            options.SkipUnknownFields = *skipUnknownFields;
        } else {
            options.SkipUnknownFields = true;
        }

        if (outputLimit) {
            TLimitedYsonWriter writer(*outputLimit, ysonFormat);
            ParseProtobuf(&writer, &inputStream, messageType, options);
            return Py::ConvertToPythonString(writer.GetResult());
        } else {
            TString result;
            TStringOutput outputStream(result);
            TYsonWriter writer(&outputStream, ysonFormat);

            ParseProtobuf(&writer, &inputStream, messageType, options);
            return Py::ConvertToPythonString(result);
        }
    }

    Py::Object LoadsProtoImpl(Py::Object stringObject, Py::Object protoObject, std::optional<bool> skipUnknownFields)
    {
        auto descriptorObject = GetAttr(protoObject, "DESCRIPTOR");
        RegisterFileDescriptor(GetAttr(descriptorObject, "file"));

        auto fullName = ConvertStringObjectToString(GetAttr(descriptorObject, "full_name"));
        auto* descriptor = GetDescriptorPool()->FindMessageTypeByName(fullName);
        auto* messageType = ReflectProtobufMessageType(descriptor);

        TString result;
        ::google::protobuf::io::StringOutputStream outputStream(&result);

        TProtobufWriterOptions options;
        options.UnknownYsonFieldModeResolver = skipUnknownFields.value_or(false)
            ? TProtobufWriterOptions::CreateConstantUnknownYsonFieldModeResolver(EUnknownYsonFieldsMode::Skip)
            : TProtobufWriterOptions::CreateConstantUnknownYsonFieldModeResolver(EUnknownYsonFieldsMode::Fail);
        auto writer = CreateProtobufWriter(&outputStream, messageType, options);

        ParseYsonStringBuffer(ConvertToStringBuf(stringObject), EYsonType::Node, writer.get());

        Py::Callable(GetAttr(protoObject, "ParseFromString")).apply(Py::TupleN(Py::ConvertToPythonString(result)));
        return protoObject;
    }

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NPython

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
