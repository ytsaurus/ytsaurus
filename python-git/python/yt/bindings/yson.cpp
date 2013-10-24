#include "public.h"
#include "common.h"
#include "stream.h"
#include "serialize.h"
#include "shutdown.h"

#include <core/ytree/convert.h>

#include <contrib/libs/pycxx/Objects.hxx>
#include <contrib/libs/pycxx/Extensions.hxx>

#include <iostream>

namespace NYT {
namespace NPython {

using namespace NYTree;

///////////////////////////////////////////////////////////////////////////////

class TYsonIter
    : public Py::PythonClass<TYsonIter>
{
public:
    TYsonIter(Py::PythonClassInstance *self, Py::Tuple& args, Py::Dict& kwargs)
        : Py::PythonClass<TYsonIter>::PythonClass(self, args, kwargs)
    { }

    void Init(NYson::EYsonType ysonType, TInputStream* inputStream, std::unique_ptr<TInputStream> inputStreamOwner)
    {
        YCHECK(!inputStreamOwner ||  inputStreamOwner.get() == inputStream);
        InputStream_ = inputStream;
        InputStreamOwner_ = std::move(inputStreamOwner);
        Parser_.reset(new NYson::TYsonParser(&Consumer_, ysonType));
        IsStreamRead_ = false;
    }

    Py::Object iter()
    {
        return self();
    }

    PyObject* iternext()
    {
        // Read unless we have whole row
        while (!Consumer_.HasObject() && !IsStreamRead_) {
            int length = InputStream_->Read(Buffer_, BufferSize_);
            Parser_->Read(TStringBuf(Buffer_, length));
            if (BufferSize_ != length) {
                IsStreamRead_ = true;
                Parser_->Finish();
            }
        }

        // Stop iteration if we done
        if (!Consumer_.HasObject()) {
            PyErr_SetNone(PyExc_StopIteration);
            return 0;
        }

        auto result = Consumer_.ExtractObject();
        // We should return pointer to alive object
        result.increment_reference_count();
        return result.ptr();
    }

    virtual ~TYsonIter()
    { }

    static void InitType()
    {
        behaviors().name("Yson iterator");
        behaviors().doc("Iterates over stream with yson records");
        behaviors().supportGetattro();
        behaviors().supportSetattro();
        behaviors().supportIter();

        behaviors().readyType();
    }

private:
    TInputStream* InputStream_;
    std::unique_ptr<TInputStream> InputStreamOwner_;

    bool IsStreamRead_;

    NYTree::TPythonObjectConsumer Consumer_;
    std::unique_ptr<NYson::TYsonParser> Parser_;

    static const int BufferSize_ = 1024 * 1024;
    char Buffer_[BufferSize_];
};

///////////////////////////////////////////////////////////////////////////////

class yson_module
    : public Py::ExtensionModule<yson_module>
{
public:
    yson_module()
        : Py::ExtensionModule<yson_module>("yson")
    {
        RegisterShutdown();

        TYsonIter::InitType();

        add_keyword_method("load", &yson_module::Load, "load yson from stream");
        add_keyword_method("loads", &yson_module::Loads, "load yson from string");

        add_keyword_method("dump", &yson_module::Dump, "dump yson to stream");
        add_keyword_method("dumps", &yson_module::Dumps, "dump yson to string");

        initialize("Yson python bindings");
    }

    Py::Object Load(const Py::Tuple& args_, const Py::Dict& kwargs_)
    {
        return LoadImpl(args_, kwargs_, 0);
    }

    Py::Object Loads(const Py::Tuple& args_, const Py::Dict& kwargs_)
    {
        auto args = args_;
        auto kwargs = kwargs_;

        auto string = ConvertToString(ExtractArgument(args, kwargs, "string"));
        
        char* rawStr = PyString_AsString(*string);
        int len = string.size();

        return LoadImpl(args, kwargs, std::unique_ptr<TInputStream>(new TMemoryInput(static_cast<void*>(rawStr), len)));
    }

    Py::Object Dump(const Py::Tuple& args_, const Py::Dict& kwargs_)
    {
        DumpImpl(args_, kwargs_, nullptr);

        return Py::None();
    }

    Py::Object Dumps(const Py::Tuple& args_, const Py::Dict& kwargs_)
    {
        Stroka result;
        TStringOutput stringOutput(result);

        DumpImpl(args_, kwargs_, &stringOutput);
        return Py::String(~result);
    }

    virtual ~yson_module()
    { }

private:
    Py::Object LoadImpl(const Py::Tuple& args_, const Py::Dict& kwargs_, std::unique_ptr<TInputStream> inputStream)
    {
        auto args = args_;
        auto kwargs = kwargs_;

        // Holds inputStreamWrap if passed non-trivial stream argument
        TInputStream* inputStreamPtr;
        if (!inputStream) {
            auto streamArg = ExtractArgument(args, kwargs, "stream");

            // In case of sys.stdin we write directly to stream
            // without any wrappers by optimization reasons.
            Py::Object pyStdin(PySys_GetObject(const_cast<char*>("__stdin__")));
            if (*pyStdin == *streamArg) {
                inputStreamPtr = &Cin;
            } else {
                inputStream.reset(new TInputStreamWrap(streamArg));
                inputStreamPtr = inputStream.get();
            }
        } else {
            inputStreamPtr = inputStream.get();
        }

        auto ysonType = NYson::EYsonType::Node;
        if (HasArgument(args, kwargs, "yson_type")) {
            auto arg = ExtractArgument(args, kwargs, "yson_type");
            ysonType = ParseEnum<NYson::EYsonType>(ConvertToStroka(ConvertToString(arg)));
        }

        if (args.length() > 0 || kwargs.length() > 0) {
            throw Py::RuntimeError("Incorrect arguments");
        }

        if (ysonType == NYson::EYsonType::MapFragment) {
            throw Py::RuntimeError("Map fragment is not supported");
        }


        if (ysonType == NYson::EYsonType::ListFragment) {
            Py::Callable class_type(TYsonIter::type());
            Py::PythonClassObject<TYsonIter> pythonIter(class_type.apply(Py::Tuple(), Py::Dict()));

            auto* iter = pythonIter.getCxxObject();
            iter->Init(ysonType, inputStreamPtr, std::move(inputStream));
            return pythonIter;
        } else {
            NYTree::TPythonObjectConsumer consumer;
            NYson::TYsonParser parser(&consumer, ysonType);

            const int BufferSize = 1024 * 1024;
            char buffer[BufferSize];
            while (int length = inputStream->Read(buffer, BufferSize))
            {
                parser.Read(TStringBuf(buffer, length));
                if (BufferSize != length) {
                    break;
                }
            }
            parser.Finish();

            return consumer.ExtractObject();
        }
    }

    void DumpImpl(const Py::Tuple& args_, const Py::Dict& kwargs_, TOutputStream* outputStream)
    {
        auto args = args_;
        auto kwargs = kwargs_;

        auto obj = ExtractArgument(args, kwargs, "object");

        // Holds outputStreamWrap if passed non-trivial stream argument
        std::unique_ptr<TOutputStreamWrap> outputStreamWrap;

        if (!outputStream) {
            auto streamArg = ExtractArgument(args, kwargs, "stream");

            // In case of sys.stdout and sys.stderr we write directly to stream
            // without any wrappers by optimization reasons.
            Py::Object pyStdout(PySys_GetObject(const_cast<char*>("__stdout__")));
            Py::Object pyStderr(PySys_GetObject(const_cast<char*>("__stderr__")));
            if (*pyStdout == *streamArg) {
                outputStream = &Cout;
            } else if (*pyStderr == *streamArg) {
                outputStream = &Cerr;
            } else {
                outputStreamWrap.reset(new TOutputStreamWrap(streamArg));
                outputStream = outputStreamWrap.get();
            }
        }

        NYson::EYsonFormat ysonFormat = NYson::EYsonFormat::Text;
        if (HasArgument(args, kwargs, "yson_format")) {
            auto arg = ExtractArgument(args, kwargs, "yson_format");
            ysonFormat = ParseEnum<NYson::EYsonFormat>(ConvertToStroka(ConvertToString(arg)));
        }

        NYson::EYsonType ysonType = NYson::EYsonType::Node;
        if (HasArgument(args, kwargs, "yson_type")) {
            auto arg = ExtractArgument(args, kwargs, "yson_type");
            ysonType = ParseEnum<NYson::EYsonType>(ConvertToStroka(ConvertToString(arg)));
        }

        int indent = 4;
        if (HasArgument(args, kwargs, "indent")) {
            auto arg = ExtractArgument(args, kwargs, "indent");
            indent = Py::Int(arg).asLongLong();
        }

        if (args.length() > 0 || kwargs.length() > 0) {
            throw Py::RuntimeError("Incorrect arguments");
        }

        NYson::TYsonWriter writer(outputStream, ysonFormat, ysonType, false, indent);
        if (ysonType == NYson::EYsonType::Node) {
            NYTree::Consume(obj, &writer);
        } else if (ysonType == NYson::EYsonType::ListFragment) {
            auto iterator = Py::Object(PyObject_GetIter(obj.ptr()));

            PyObject *item;
            while (item = PyIter_Next(*iterator)) {
                NYTree::Consume(Py::Object(item, true), &writer);
            }
        } else {
            throw Py::RuntimeError(ysonType.ToString() + " is not supported");
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
    static NYT::NPython::yson_module* yson = new NYT::NPython::yson_module;
    UNUSED(yson);
}

extern "C" EXPORT_SYMBOL void inityson_lib_d()
{
    inityson_lib();
}

