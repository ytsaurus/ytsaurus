#include "common.h"
#include "stream.h"
#include "serialize.h"

#include <ytlib/misc/intrusive_ptr.h>
#include <ytlib/formats/format.h>
#include <ytlib/driver/config.h>
#include <ytlib/driver/driver.h>
#include <ytlib/logging/log_manager.h>
#include <ytlib/ytree/convert.h>

// For at_exit
#include <ytlib/profiling/profiling_manager.h>
#include <ytlib/rpc/dispatcher.h>
#include <ytlib/bus/tcp_dispatcher.h>
#include <ytlib/chunk_client/dispatcher.h>

#include <contrib/libs/pycxx/Objects.hxx>
#include <contrib/libs/pycxx/Extensions.hxx>

#include <iostream>

namespace NYT {

using NFormats::TFormat;
using NFormats::EFormatType;
using NDriver::TDriverRequest;
using NDriver::TDriverConfig;
using NDriver::IDriverPtr;
using NDriver::CreateDriver;
using NYTree::IYsonConsumer;
using NYTree::ConvertToNode;
using NYTree::ConvertToYsonString;
using NYTree::EYsonFormat;

namespace NPython {

Py::Object ExtractArgument(Py::Tuple& args, Py::Dict& kwds, const std::string& name) {
    Py::Object result;
    if (kwds.hasKey(name)) {
        result = kwds[name];
        kwds.delItem(name);
    }
    else {
        if (args.length() == 0) {
            throw Py::RuntimeError("There is no argument " + name);
        }
        result = args.front();
        args = args.getSlice(1, args.length());
    }
    return result;
}

void ExtractFormat(const Py::Object& obj, TFormat& format)
{
    if (obj.isNone()) {
        format = TFormat(EFormatType::Null);
    }
    else {
        Deserialize(format, ConvertToNode(obj));
    }
}

class Driver
    : public Py::PythonClass<Driver>
{
public:
    Driver(Py::PythonClassInstance *self, Py::Tuple &args, Py::Dict &kwds)
        : Py::PythonClass<Driver>::PythonClass(self, args, kwds)
    {
        Py::Object configDict = ExtractArgument(args, kwds, "config");
        if (args.length() > 0 || kwds.length() > 0) {
            throw Py::RuntimeError("Incorrect arguments for initialization of Driver");
        }
        auto config = New<TDriverConfig>();
        auto configNode = ConvertToNode(configDict);
        try {
            config->Load(configNode);
        } catch(const TErrorException& error) {
            throw Py::RuntimeError("Fail while loading config: " + error.Error().GetMessage());
        }
        NLog::TLogManager::Get()->Configure(configNode->AsMap()->FindChild("logging"));
        DriverInstance_ = CreateDriver(config);
    }
    
    virtual ~Driver()
    { }

    static void InitType() {
        behaviors().name("Driver");
        behaviors().doc("Some documentation");
        behaviors().supportGetattro();
        behaviors().supportSetattro();
        
        PYCXX_ADD_KEYWORDS_METHOD(execute, Execute, "TODO(ignat): make documentation");
        
        behaviors().readyType();
    }

    Py::Object Execute(Py::Tuple& args, Py::Dict &kwds) {
        auto pyRequest = ExtractArgument(args, kwds, "request");
        if (args.length() > 0 || kwds.length() > 0) {
            throw Py::RuntimeError("Incorrect arguments for execute command");
        }
        
        TDriverRequest request;
        request.CommandName = ConvertToStroka(Py::String(GetAttr(pyRequest, "command_name")));
        request.Arguments = ConvertToNode(GetAttr(pyRequest, "arguments"))->AsMap();
        ExtractFormat(GetAttr(pyRequest, "input_format"), request.InputFormat);
        ExtractFormat(GetAttr(pyRequest, "output_format"), request.OutputFormat);

        TAutoPtr<TPythonInputStream> inputStream(
            new TPythonInputStream(GetAttr(pyRequest, "input_stream")));
        TAutoPtr<TPythonOutputStream> outputStream(
            new TPythonOutputStream(GetAttr(pyRequest, "output_stream")));
        request.InputStream = inputStream.Get();
        request.OutputStream = outputStream.Get();

        auto response = DriverInstance_->Execute(request);
        return ConvertToPythonString(ToString(response.Error));
    }
    PYCXX_KEYWORDS_METHOD_DECL(Driver, Execute)

private:
    IDriverPtr DriverInstance_;
};

class ytlib_python_module 
    : public Py::ExtensionModule<ytlib_python_module>
{
public:
    ytlib_python_module()
        : Py::ExtensionModule<ytlib_python_module>("ytlib_python")
    {
        Py_AtExit(ytlib_python_module::at_exit);

        Driver::InitType();
        
        initialize("Ytlib python bindings");
        
        Py::Dict moduleDict(moduleDictionary());
        moduleDict["Driver"] = Driver::type();
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

    virtual ~ytlib_python_module()
    { }
};

} // namespace NPython

} // namespace NYT


#if defined( _WIN32 )
#define EXPORT_SYMBOL __declspec( dllexport )
#else
#define EXPORT_SYMBOL
#endif

extern "C" EXPORT_SYMBOL void initytlib_python()
{
    static NYT::NPython::ytlib_python_module* ytlib_python = new NYT::NPython::ytlib_python_module;
}

// symbol required for the debug version
extern "C" EXPORT_SYMBOL void initytlib_python_d()
{ initytlib_python(); }

