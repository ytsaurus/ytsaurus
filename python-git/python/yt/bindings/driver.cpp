#include "public.h"
#include "common.h"
#include "stream.h"
#include "serialize.h"
#include "response.h"
#include "buffered_stream.h"
#include "descriptor.h"

#include <core/misc/at_exit_manager.h>
#include <core/misc/intrusive_ptr.h>

#include <core/concurrency/async_stream.h>

#include <core/logging/log_manager.h>

#include <core/tracing/trace_manager.h>

#include <core/ytree/convert.h>

#include <ytlib/formats/format.h>

#include <ytlib/api/connection.h>

#include <ytlib/driver/config.h>
#include <ytlib/driver/driver.h>
#include <ytlib/driver/dispatcher.h>

#include <ytlib/object_client/object_service_proxy.h>

#include <ytlib/hydra/hydra_service_proxy.h>

#include <contrib/libs/pycxx/Objects.hxx>
#include <contrib/libs/pycxx/Extensions.hxx>

#include <iostream>

namespace NYT {
namespace NPython {

///////////////////////////////////////////////////////////////////////////////

using namespace NFormats;
using namespace NDriver;
using namespace NYson;
using namespace NYTree;
using namespace NConcurrency;

///////////////////////////////////////////////////////////////////////////////

Py::Exception CreateYtError(const std::string& message)
{
    static PyObject* ytErrorClass = nullptr;
    if (!ytErrorClass) {
        ytErrorClass = PyObject_GetAttr(
            PyImport_ImportModule("yt.common"),
            PyString_FromString("YtError"));
    }
    return Py::Exception(ytErrorClass, message);
}

///////////////////////////////////////////////////////////////////////////////

class TDriver
    : public Py::PythonClass<TDriver>
{
public:
    TDriver(Py::PythonClassInstance *self, Py::Tuple& args, Py::Dict& kwargs)
        : Py::PythonClass<TDriver>::PythonClass(self, args, kwargs)
    {
        Py::Object configDict = ExtractArgument(args, kwargs, "config");
        if (args.length() > 0 || kwargs.length() > 0) {
            throw Py::RuntimeError("Incorrect arguments");
        }
        auto config = New<TDriverConfig>();
        auto configNode = ConvertToNode(configDict);
        try {
            config->Load(configNode);
        } catch(const std::exception& ex) {
            throw Py::RuntimeError(Stroka("Error loading driver configuration\n") + ex.what());
        }
        DriverInstance_ = CreateDriver(config);
    }

    virtual ~TDriver()
    { }

    static void InitType()
    {
        behaviors().name("Driver");
        behaviors().doc("Represents YT driver");
        behaviors().supportGetattro();
        behaviors().supportSetattro();

        PYCXX_ADD_KEYWORDS_METHOD(execute, Execute, "Executes the request");
        PYCXX_ADD_KEYWORDS_METHOD(get_command_descriptor, GetCommandDescriptor, "Describes the command");
        PYCXX_ADD_KEYWORDS_METHOD(get_command_descriptors, GetCommandDescriptors, "Describes all commands");
        PYCXX_ADD_KEYWORDS_METHOD(build_snapshot, BuildSnapshot, "Force master to build a snapshot");
        PYCXX_ADD_KEYWORDS_METHOD(gc_collect, GcCollect, "Run garbage collection");
        PYCXX_ADD_KEYWORDS_METHOD(clear_metadata_caches, ClearMetadataCaches, "Clear metadata caches");

        behaviors().readyType();
    }

    Py::Object Execute(Py::Tuple& args, Py::Dict& kwargs)
    {
        auto pyRequest = ExtractArgument(args, kwargs, "request");
        if (args.length() > 0 || kwargs.length() > 0) {
            throw Py::RuntimeError("Incorrect arguments");
        }

        Py::Callable class_type(TDriverResponse::type());
        Py::PythonClassObject<TDriverResponse> pythonResponse(class_type.apply(Py::Tuple(), Py::Dict()));
        auto* response = pythonResponse.getCxxObject();

        TDriverRequest request;
        request.CommandName = ConvertToStroka(Py::String(GetAttr(pyRequest, "command_name")));
        request.Parameters = ConvertToNode(GetAttr(pyRequest, "parameters"))->AsMap();

        auto user = GetAttr(pyRequest, "user");
        if (!user.isNone()) {
            request.AuthenticatedUser = ConvertToStroka(Py::String(user));
        }

        auto inputStreamObj = GetAttr(pyRequest, "input_stream");
        if (!inputStreamObj.isNone()) {
            std::unique_ptr<TInputStreamWrap> inputStream(new TInputStreamWrap(inputStreamObj));
            request.InputStream = CreateAsyncInputStream(inputStream.get());
            response->OwnInputStream(inputStream);
        }

        auto outputStreamObj = GetAttr(pyRequest, "output_stream");
        if (!outputStreamObj.isNone()) {
            bool isBufferedStream = PyObject_IsInstance(outputStreamObj.ptr(), TBufferedStreamWrap::type().ptr());
            if (isBufferedStream) {
                auto* pythonStream = dynamic_cast<TBufferedStreamWrap*>(Py::getPythonExtensionBase(outputStreamObj.ptr()));
                request.OutputStream = pythonStream->GetStream();
            } else {
                std::unique_ptr<TOutputStreamWrap> outputStream(new TOutputStreamWrap(outputStreamObj));
                request.OutputStream = CreateAsyncOutputStream(outputStream.get());
                response->OwnOutputStream(outputStream);
            }
        }

        try {
            response->SetResponse(DriverInstance_->Execute(request));
        } catch (const std::exception& error) {
            throw CreateYtError(error.what());
        }

        return pythonResponse;
    }
    PYCXX_KEYWORDS_METHOD_DECL(TDriver, Execute)

    Py::Object GetCommandDescriptor(Py::Tuple& args, Py::Dict& kwargs)
    {
        auto commandName = ConvertToStroka(ConvertToString(ExtractArgument(args, kwargs, "command_name")));
        if (args.length() > 0 || kwargs.length() > 0) {
            throw Py::RuntimeError("Incorrect arguments");
        }

        Py::Callable class_type(TCommandDescriptor::type());
        Py::PythonClassObject<TCommandDescriptor> descriptor(class_type.apply(Py::Tuple(), Py::Dict()));
        try {
            descriptor.getCxxObject()->SetDescriptor(DriverInstance_->GetCommandDescriptor(commandName));
        } catch (const std::exception& error) {
            throw CreateYtError(error.what());
        }

        return descriptor;
    }
    PYCXX_KEYWORDS_METHOD_DECL(TDriver, GetCommandDescriptor)

    Py::Object GetCommandDescriptors(Py::Tuple& args, Py::Dict& kwargs)
    {
        if (args.length() > 0 || kwargs.length() > 0) {
            throw Py::RuntimeError("Incorrect arguments");
        }

        try {
            auto descriptors = Py::List();
            for (const auto& nativeDescriptor : DriverInstance_->GetCommandDescriptors()) {
                Py::Callable class_type(TCommandDescriptor::type());
                Py::PythonClassObject<TCommandDescriptor> descriptor(class_type.apply(Py::Tuple(), Py::Dict()));
                descriptor.getCxxObject()->SetDescriptor(nativeDescriptor);
                descriptors.append(descriptor);
            }
            return descriptors;
        } catch (const std::exception& error) {
            throw CreateYtError(error.what());
        }
    }
    PYCXX_KEYWORDS_METHOD_DECL(TDriver, GetCommandDescriptors)

    Py::Object ConfigureDispatcher(Py::Tuple& args, Py::Dict& kwargs)
    {
        auto heavyPoolSize = Py::Int(ExtractArgument(args, kwargs, "command_name")).asLongLong();
        if (args.length() > 0 || kwargs.length() > 0) {
            throw Py::RuntimeError("Incorrect arguments");
        }

        NDriver::TDispatcher::Get()->Configure(heavyPoolSize);

        return Py::None();
    }
    PYCXX_KEYWORDS_METHOD_DECL(TDriver, ConfigureDispatcher)

    Py::Object GcCollect(Py::Tuple& args, Py::Dict& kwargs)
    {
        try {
            NObjectClient::TObjectServiceProxy proxy(DriverInstance_->GetConnection()->GetMasterChannel());
            proxy.SetDefaultTimeout(Null); // infinity
            auto req = proxy.GCCollect();
            auto rsp = req->Invoke().Get();
            if (!rsp->IsOK()) {
                return ConvertTo<Py::Object>(TError(*rsp));
            }
        } catch (const std::exception& error) {
            throw CreateYtError(error.what());
        }
        return Py::None();
    }
    PYCXX_KEYWORDS_METHOD_DECL(TDriver, GcCollect)

    Py::Object BuildSnapshot(Py::Tuple& args, Py::Dict& kwargs)
    {
        bool setReadOnly = false;
        if (args.length() > 0 || kwargs.length() > 0) {
            setReadOnly = static_cast<bool>(Py::Boolean(ExtractArgument(args, kwargs, "set_read_only")));
        }
        if (args.length() > 0 || kwargs.length() > 0) {
            throw Py::RuntimeError("Incorrect arguments");
        }

        try {
            NObjectClient::TObjectServiceProxy proxy(DriverInstance_->GetConnection()->GetMasterChannel());
            proxy.SetDefaultTimeout(Null); // infinity
            auto req = proxy.BuildSnapshot();
            req->set_set_read_only(setReadOnly);

            auto rsp = req->Invoke().Get();
            if (!rsp->IsOK()) {
                return ConvertTo<Py::Object>(TError(*rsp));
            }

            int snapshotId = rsp->snapshot_id();
            printf("Snapshot %d is built\n", snapshotId);
        } catch (const std::exception& error) {
            throw CreateYtError(error.what());
        }

        return Py::None();
    }
    PYCXX_KEYWORDS_METHOD_DECL(TDriver, BuildSnapshot)

    Py::Object ClearMetadataCaches(Py::Tuple& args, Py::Dict& kwargs)
    {
        if (args.length() > 0 || kwargs.length() > 0) {
            throw Py::RuntimeError("Incorrect arguments");
        }

        try {
            DriverInstance_->GetConnection()->ClearMetadataCaches();
        } catch (const std::exception& error) {
            throw CreateYtError(error.what());
        }

        return Py::None();
    }
    PYCXX_KEYWORDS_METHOD_DECL(TDriver, ClearMetadataCaches)

private:
    IDriverPtr DriverInstance_;
};

///////////////////////////////////////////////////////////////////////////////

class driver_module
    : public Py::ExtensionModule<driver_module>
{
public:
    driver_module()
        // It should be the same as .so file name
        : Py::ExtensionModule<driver_module>("driver_lib")
    {
        PyEval_InitThreads();

        YCHECK(!AtExitManager_);
        AtExitManager_ = new TAtExitManager();
        Py_AtExit([] () { delete AtExitManager_; });

        TDriver::InitType();
        TBufferedStreamWrap::InitType();
        TDriverResponse::InitType();
        TCommandDescriptor::InitType();

        add_keyword_method("configure_logging", &driver_module::ConfigureLogging, "configure logging of driver instances");
        add_keyword_method("configure_tracing", &driver_module::ConfigureTracing, "configure tracing");

        initialize("Python bindings for driver");

        Py::Dict moduleDict(moduleDictionary());
        moduleDict["Driver"] = TDriver::type();
        moduleDict["BufferedStream"] = TBufferedStreamWrap::type();
    }

    virtual ~driver_module()
    { }

    Py::Object ConfigureLogging(const Py::Tuple& args_, const Py::Dict& kwargs_)
    {
        auto args = args_;
        auto kwargs = kwargs_;

        auto config = ConvertToNode(ExtractArgument(args, kwargs, "config"));

        if (args.length() > 0 || kwargs.length() > 0) {
            throw CreateYtError("Incorrect arguments");
        }

        NLog::TLogManager::Get()->Configure(config->AsMap());

        return Py::None();
    }

    Py::Object ConfigureTracing(const Py::Tuple& args_, const Py::Dict& kwargs_)
    {
        auto args = args_;
        auto kwargs = kwargs_;

        auto config = ConvertToNode(ExtractArgument(args, kwargs, "config"));

        if (args.length() > 0 || kwargs.length() > 0) {
            throw CreateYtError("Incorrect arguments");
        }

        NTracing::TTraceManager::Get()->Configure(config->AsMap());

        return Py::None();
    }

private:
    static TAtExitManager* AtExitManager_;
};

TAtExitManager* driver_module::AtExitManager_ = nullptr;

///////////////////////////////////////////////////////////////////////////////

} // namespace NPython
} // namespace NYT

///////////////////////////////////////////////////////////////////////////////

#if defined( _WIN32 )
#define EXPORT_SYMBOL __declspec( dllexport )
#else
#define EXPORT_SYMBOL
#endif

extern "C" EXPORT_SYMBOL void initdriver_lib()
{
    static NYT::NPython::driver_module* driver = new NYT::NPython::driver_module;
    UNUSED(driver);
}

// symbol required for the debug version
extern "C" EXPORT_SYMBOL void initdriver_lib_d()
{
    initdriver_lib();
}
