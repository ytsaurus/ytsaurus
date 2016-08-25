#include "public.h"
#include "helpers.h"
#include "serialize.h"
#include "shutdown.h"
#include "stream.h"

#include <yt/ytlib/ypath/rich.h>

#include <yt/core/ytree/convert.h>

#include <contrib/libs/pycxx/Extensions.hxx>
#include <contrib/libs/pycxx/Objects.hxx>

namespace NYT {
namespace NPython {

using namespace NYTree;
using namespace NYPath;

///////////////////////////////////////////////////////////////////////////////

Py::Exception CreateYsonError(const NYT::TError& error)
{
    auto ysonErrorClass = Py::Callable(
        PyObject_GetAttr(
            PyImport_ImportModule("yt.yson.common"),
            PyString_FromString("YsonError")));

    Py::Dict options;
    options.setItem("message", ConvertTo<Py::Object>(error.GetMessage()));
    options.setItem("code", ConvertTo<Py::Object>(error.GetCode()));
    options.setItem("inner_errors", ConvertTo<Py::Object>(error.InnerErrors()));
    auto ysonError = ysonErrorClass.apply(options);
    return Py::Exception(*ysonError, ysonError);
}

Py::Exception CreateYsonError(const std::string& message)
{
    auto ysonErrorClass = Py::Object(
        PyObject_GetAttr(
            PyImport_ImportModule("yt.yson.common"),
            PyString_FromString("YsonError")));
    return Py::Exception(*ysonErrorClass, message);
}

#define CATCH \
    catch (const NYT::TErrorException& error) { \
        throw CreateYsonError(error.Error()); \
    } catch (const std::exception& ex) { \
        if (PyErr_ExceptionMatches(PyExc_BaseException)) { \
            throw; \
        } else { \
            throw CreateYsonError(ex.what()); \
        } \
    }

///////////////////////////////////////////////////////////////////////////////

class TYsonIterator
    : public Py::PythonClass<TYsonIterator>
{
public:
    TYsonIterator(Py::PythonClassInstance *self, Py::Tuple& args, Py::Dict& kwargs)
        : Py::PythonClass<TYsonIterator>::PythonClass(self, args, kwargs)
    { }

    void Init(TInputStream* inputStream, std::unique_ptr<TInputStream> inputStreamOwner, bool alwaysCreateAttributes)
    {
        YCHECK(!inputStreamOwner || inputStreamOwner.get() == inputStream);
        InputStream_ = inputStream;
        InputStreamOwner_ = std::move(inputStreamOwner);
        Consumer_.reset(new NYTree::TPythonObjectBuilder(alwaysCreateAttributes));
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
                int length = InputStream_->Read(Buffer_, BufferSize_);
                if (length != 0) {
                    Parser_->Read(TStringBuf(Buffer_, length));
                }
                if (BufferSize_ != length) {
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
        } CATCH;
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
    TInputStream* InputStream_;
    std::unique_ptr<TInputStream> InputStreamOwner_;

    bool IsStreamRead_;

    std::unique_ptr<NYTree::TPythonObjectBuilder> Consumer_;
    std::unique_ptr<NYson::TYsonParser> Parser_;

    static const int BufferSize_ = 1024 * 1024;
    char Buffer_[BufferSize_];
};

///////////////////////////////////////////////////////////////////////////////

class TRawYsonIterator
    : public Py::PythonClass<TRawYsonIterator>
{
public:
    TRawYsonIterator(Py::PythonClassInstance *self, Py::Tuple& args, Py::Dict& kwargs)
        : Py::PythonClass<TRawYsonIterator>::PythonClass(self, args, kwargs)
    { }

    void Init(TInputStream* inputStream, std::unique_ptr<TInputStream> inputStreamOwner)
    {
        InputStreamOwner_ = std::move(inputStreamOwner);
        Lexer_ = TListFragmentLexer(inputStream);
    }

    Py::Object iter()
    {
        return self();
    }

    PyObject* iternext()
    {
        try {
            auto item = Lexer_.NextItem();
            if (!item) {
                PyErr_SetNone(PyExc_StopIteration);
                return 0;
            }
            auto result = Py::String(item.Begin(), item.Size());
            result.increment_reference_count();
            return result.ptr();
        } CATCH;
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
    std::unique_ptr<TInputStream> InputStreamOwner_;
    TListFragmentLexer Lexer_;
};

///////////////////////////////////////////////////////////////////////////////

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

        TYsonIterator::InitType();
        TRawYsonIterator::InitType();

        add_keyword_method("load", &TYsonModule::Load, "Loads YSON from stream");
        add_keyword_method("loads", &TYsonModule::Loads, "Loads YSON from string");

        add_keyword_method("dump", &TYsonModule::Dump, "Dumps YSON to stream");
        add_keyword_method("dumps", &TYsonModule::Dumps, "Dumps YSON to string");

        add_keyword_method("parse_ypath", &TYsonModule::ParseYPath, "Parse YPath");

        initialize("Python bindings for YSON");
    }

    Py::Object Load(const Py::Tuple& args_, const Py::Dict& kwargs_)
    {
        auto args = args_;
        auto kwargs = kwargs_;

        try {
            return LoadImpl(args, kwargs, nullptr);
        } CATCH;
    }

    Py::Object Loads(const Py::Tuple& args_, const Py::Dict& kwargs_)
    {
        auto args = args_;
        auto kwargs = kwargs_;

        auto pythonString = ConvertToString(ExtractArgument(args, kwargs, "string"));
        auto string = Stroka(PyString_AsString(*pythonString), pythonString.size());

        std::unique_ptr<TInputStream> stringStream(new TOwningStringInput(string));

        try {
            return LoadImpl(args, kwargs, std::move(stringStream));
        } CATCH;
    }

    Py::Object Dump(const Py::Tuple& args_, const Py::Dict& kwargs_)
    {
        auto args = args_;
        auto kwargs = kwargs_;

        try {
            DumpImpl(args, kwargs, nullptr);
        } CATCH;

        return Py::None();
    }

    Py::Object Dumps(const Py::Tuple& args_, const Py::Dict& kwargs_)
    {
        auto args = args_;
        auto kwargs = kwargs_;

        Stroka result;
        TStringOutput stringOutput(result);

        try {
            DumpImpl(args, kwargs, &stringOutput);
        } CATCH;

        return Py::String(~result, result.Size());
    }

    Py::Object ParseYPath(const Py::Tuple& args_, const Py::Dict& kwargs_)
    {
        auto args = args_;
        auto kwargs = kwargs_;

        auto path = ConvertToStroka(ConvertToString(ExtractArgument(args, kwargs, "path")));
        ValidateArgumentsEmpty(args, kwargs);

        auto richPath = TRichYPath::Parse(path);
        return CreateYsonObject("YsonString", Py::String(richPath.GetPath()), ConvertTo<Py::Object>(richPath.Attributes().ToMap()));
    }

    virtual ~TYsonModule()
    { }

private:
    Py::Object LoadImpl(
        Py::Tuple& args,
        Py::Dict& kwargs,
        std::unique_ptr<TInputStream> inputStream)
    {
        // Holds inputStreamWrap if passed non-trivial stream argument
        TInputStream* inputStreamPtr;
        if (!inputStream) {
            auto streamArg = ExtractArgument(args, kwargs, "stream");

            if (PyFile_Check(streamArg.ptr())) {
                FILE* file = PyFile_AsFile(streamArg.ptr());
                inputStream.reset(new TFileInput(Duplicate(file)));
            } else {
                inputStream.reset(new TInputStreamWrap(streamArg));
            }
        }
        inputStreamPtr = inputStream.get();

        auto ysonType = NYson::EYsonType::Node;
        if (HasArgument(args, kwargs, "yson_type")) {
            auto arg = ExtractArgument(args, kwargs, "yson_type");
                ysonType = ParseEnum<NYson::EYsonType>(ConvertToStroka(ConvertToString(arg)));
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

        ValidateArgumentsEmpty(args, kwargs);

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
                iter->Init(inputStreamPtr, std::move(inputStream), alwaysCreateAttributes);
                return pythonIter;
            }
        } else {
            if (raw) {
                throw CreateYsonError("Raw mode is only supported for list fragments");
            }
            NYTree::TPythonObjectBuilder consumer(alwaysCreateAttributes);
            NYson::TYsonParser parser(&consumer, ysonType);

            const int BufferSize = 1024 * 1024;
            char buffer[BufferSize];

            if (ysonType == NYson::EYsonType::MapFragment) {
                consumer.OnBeginMap();
            }
            while (int length = inputStreamPtr->Read(buffer, BufferSize))
            {
                parser.Read(TStringBuf(buffer, length));
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

    void DumpImpl(Py::Tuple& args, Py::Dict& kwargs, TOutputStream* outputStream)
    {
        auto obj = ExtractArgument(args, kwargs, "object");

        // Holds outputStreamWrap if passed non-trivial stream argument
        std::unique_ptr<TOutputStreamWrap> outputStreamWrap;
        std::unique_ptr<TFileOutput> fileOutput;
        std::unique_ptr<TBufferedOutput> bufferedOutputStream;

        if (!outputStream) {
            auto streamArg = ExtractArgument(args, kwargs, "stream");

            if (PyFile_Check(streamArg.ptr())) {
                FILE* file = PyFile_AsFile(streamArg.ptr());
                fileOutput.reset(new TFileOutput(Duplicate(file)));
                outputStream = fileOutput.get();
            } else {
                outputStreamWrap.reset(new TOutputStreamWrap(streamArg));
                outputStream = outputStreamWrap.get();
            }
            bufferedOutputStream.reset(new TBufferedOutput(outputStream, 1024 * 1024));
            outputStream = bufferedOutputStream.get();
        }

        auto ysonFormat = NYson::EYsonFormat::Text;
        if (HasArgument(args, kwargs, "yson_format")) {
            auto arg = ExtractArgument(args, kwargs, "yson_format");
            ysonFormat = ParseEnum<NYson::EYsonFormat>(ConvertToStroka(ConvertToString(arg)));
        }

        NYson::EYsonType ysonType = NYson::EYsonType::Node;
        if (HasArgument(args, kwargs, "yson_type")) {
            auto arg = ExtractArgument(args, kwargs, "yson_type");
            ysonType = ParseEnum<NYson::EYsonType>(ConvertToStroka(ConvertToString(arg)));
        }

        int indent = NYson::TYsonWriter::DefaultIndent;
        if (HasArgument(args, kwargs, "indent")) {
            auto arg = ExtractArgument(args, kwargs, "indent");
            indent = static_cast<int>(Py::Int(arg).asLongLong());
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
                Serialize(obj, writer.get(), ignoreInnerAttributes, ysonType);
                break;

            case NYson::EYsonType::ListFragment: {
                auto iterator = Py::Object(PyObject_GetIter(obj.ptr()), true);
                while (auto* item = PyIter_Next(*iterator)) {
                    Serialize(Py::Object(item, true), writer.get(), ignoreInnerAttributes);
                }
                if (PyErr_Occurred()) {
                    throw Py::Exception();
                }
                break;
            }

            default:
                throw CreateYsonError(ToString(ysonType) + " is not supported");
        }

        writer->Flush();

        if (bufferedOutputStream) {
            bufferedOutputStream->Flush();
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NPython
} // namespace NYT

///////////////////////////////////////////////////////////////////////////////

#if defined( _WIN32 )
#define EXPORT_SYMBOL __declspec( dllexport )
#else
#define EXPORT_SYMBOL
#endif

extern "C" EXPORT_SYMBOL void inityson_lib()
{
    static NYT::NPython::TYsonModule* yson = new NYT::NPython::TYsonModule;
    Y_UNUSED(yson);
}

extern "C" EXPORT_SYMBOL void inityson_lib_d()
{
    inityson_lib();
}

