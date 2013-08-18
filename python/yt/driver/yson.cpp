#include "public.h"
#include "common.h"
#include "stream.h"
#include "serialize.h"

#include <ytlib/ytree/convert.h>

// For at_exit
#include <ytlib/logging/log_manager.h>
#include <ytlib/profiling/profiling_manager.h>
#include <ytlib/rpc/dispatcher.h>
#include <ytlib/bus/tcp_dispatcher.h>
#include <ytlib/chunk_client/dispatcher.h>

#include <contrib/libs/pycxx/Objects.hxx>
#include <contrib/libs/pycxx/Extensions.hxx>

#include <iostream>


namespace NYT {

using NYTree::ConvertTo;
using NYTree::ConvertToNode;
using NYTree::ConvertToYsonString;

namespace NPython {

class yson_python_module
    : public Py::ExtensionModule<yson_python_module>
{
public:
    yson_python_module()
        : Py::ExtensionModule<yson_python_module>("yson_python")
    {
        Py_AtExit(yson_python_module::at_exit);

        add_keyword_method("load", &yson_python_module::Load, "load yson from stream");
        add_keyword_method("loads", &yson_python_module::Loads, "load yson from string");
        
        add_keyword_method("dump", &yson_python_module::Dump, "dump yson to stream");
        add_keyword_method("dumps", &yson_python_module::Dumps, "dump yson to string");
        
        initialize("Yson python bindings");
    }

    Py::Object Load(const Py::Tuple& args_, const Py::Dict& kwargs_)
    {
        auto args = args_;
        auto kwargs = kwargs_;
        
        auto inputStream = TPythonInputStream(ExtractArgument(args, kwargs, "stream"));
        
        auto ysonType = NYson::EYsonType::Node;
        if (args.length() > 0 || kwargs.length() > 0) {
            auto arg = ExtractArgument(args, kwargs, "yson_type");
            ysonType = NYson::EYsonType::FromString(ConvertToStroka(ConvertToString(arg)));
        }
        
        if (args.length() > 0 || kwargs.length() > 0) {
            throw Py::RuntimeError("Incorrect arguments");
        }

        return ConvertTo<Py::Object>(NYTree::TYsonInput(&inputStream, ysonType));
    }
    
    Py::Object Loads(const Py::Tuple& args_, const Py::Dict& kwargs_)
    {
        auto args = args_;
        auto kwargs = kwargs_;
        
        auto string = ConvertToStroka(ConvertToString(ExtractArgument(args, kwargs, "string")));
        
        auto ysonType = NYson::EYsonType::Node;
        if (args.length() > 0 || kwargs.length() > 0) {
            auto arg = ExtractArgument(args, kwargs, "yson_type");
            ysonType = NYson::EYsonType::FromString(ConvertToStroka(ConvertToString(arg)));
        }
        
        if (args.length() > 0 || kwargs.length() > 0) {
            throw Py::RuntimeError("Incorrect arguments");
        }

        return ConvertTo<Py::Object>(NYTree::TYsonString(string, ysonType));
    }

    Py::Object Dump(const Py::Tuple& args_, const Py::Dict& kwargs_)
    {
        auto args = args_;
        auto kwargs = kwargs_;
        
        auto obj = ExtractArgument(args, kwargs, "object");
        auto outputStream = TPythonOutputStream(ExtractArgument(args, kwargs, "stream"));

        auto ysonFormat = NYson::EYsonFormat::Text;
        if (args.length() > 0 || kwargs.length() > 0) {
            auto arg = ExtractArgument(args, kwargs, "yson_format");
            ysonFormat = NYson::EYsonFormat::FromString(ConvertToStroka(ConvertToString(arg)));
        }
        
        if (args.length() > 0 || kwargs.length() > 0) {
            throw Py::RuntimeError("Incorrect arguments");
        }

        WriteYson(NYTree::TYsonOutput(&outputStream), obj, ysonFormat);

        return Py::None();
    }
    
    Py::Object Dumps(const Py::Tuple& args_, const Py::Dict& kwargs_)
    {
        auto args = args_;
        auto kwargs = kwargs_;
        
        auto obj = ExtractArgument(args, kwargs, "object");

        auto ysonFormat = NYson::EYsonFormat::Text;
        if (args.length() > 0 || kwargs.length() > 0) {
            auto arg = ExtractArgument(args, kwargs, "yson_format");
            ysonFormat = NYson::EYsonFormat::FromString(ConvertToStroka(ConvertToString(arg)));
        }
        
        if (args.length() > 0 || kwargs.length() > 0) {
            throw Py::RuntimeError("Incorrect arguments");
        }

        auto ysonString = ConvertToYsonString(obj, ysonFormat);
        return Py::String(~ysonString.Data());
    }

    static void at_exit()
    {
        // TODO: refactor system shutdown
        // XXX(sandello): Keep in sync with server/main.cpp, driver/main.cpp and utmain.cpp, python_bindings/driver.cpp
        NLog::TLogManager::Get()->Shutdown();
        NBus::TTcpDispatcher::Get()->Shutdown();
        NRpc::TDispatcher::Get()->Shutdown();
        NChunkClient::TDispatcher::Get()->Shutdown();
        NProfiling::TProfilingManager::Get()->Shutdown();
        TDelayedInvoker::Shutdown();
    }

    virtual ~yson_python_module()
    { }
};

} // namespace NPython

} // namespace NYT


#if defined( _WIN32 )
#define EXPORT_SYMBOL __declspec( dllexport )
#else
#define EXPORT_SYMBOL
#endif

extern "C" EXPORT_SYMBOL void inityson_python()
{
    static NYT::NPython::yson_python_module* yson_python = new NYT::NPython::yson_python_module;
    UNUSED(yson_python);
}

// symbol required for the debug version
extern "C" EXPORT_SYMBOL void inityson_python_d()
{ inityson_python(); }


