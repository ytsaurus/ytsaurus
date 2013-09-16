#include "public.h"
#include "common.h"
#include "stream.h"
#include "serialize.h"
#include "response.h"
#include "buffered_stream.h"
#include "descriptor.h"
#include "shutdown.h"

#include <core/misc/intrusive_ptr.h>

#include <core/concurrency/async_stream.h>

#include <core/formats/format.h>

#include <core/logging/log_manager.h>
#include <core/ytree/convert.h>

#include <ytlib/driver/config.h>
#include <ytlib/driver/driver.h>
#include <ytlib/driver/dispatcher.h>

#include <ytlib/object_client/object_service_proxy.h>
#include <ytlib/meta_state/meta_state_manager_proxy.h>

// For at_exit
#include <core/profiling/profiling_manager.h>

#include <core/rpc/dispatcher.h>

#include <core/bus/tcp_dispatcher.h>

#include <ytlib/chunk_client/dispatcher.h>

#include <contrib/libs/pycxx/Objects.hxx>
#include <contrib/libs/pycxx/Extensions.hxx>

#include <iostream>

///////////////////////////////////////////////////////////////////////////////

namespace NYT {

namespace NPython {

///////////////////////////////////////////////////////////////////////////////

using namespace NFormats;
using namespace NDriver;
using namespace NYson;
using namespace NYTree;
using namespace NConcurrency;

///////////////////////////////////////////////////////////////////////////////

class TDriver
    : public Py::PythonClass<TDriver>
{
public:
    TDriver(Py::PythonClassInstance *self, Py::Tuple &args, Py::Dict &kwds)
        : Py::PythonClass<TDriver>::PythonClass(self, args, kwds)
    {
        Py::Object configDict = ExtractArgument(args, kwds, "config");
        if (args.length() > 0 || kwds.length() > 0) {
            throw Py::RuntimeError("Incorrect arguments");
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

    virtual ~TDriver()
    { }

    static void InitType() {
        behaviors().name("Driver");
        behaviors().doc("Represents YT driver");
        behaviors().supportGetattro();
        behaviors().supportSetattro();

        PYCXX_ADD_KEYWORDS_METHOD(execute, Execute, "Executes the request");
        PYCXX_ADD_KEYWORDS_METHOD(get_command_descriptor, GetCommandDescriptor, "Describes the command");
        PYCXX_ADD_KEYWORDS_METHOD(get_command_descriptors, GetCommandDescriptors, "Describes all commands");
        PYCXX_ADD_KEYWORDS_METHOD(build_snapshot, BuildSnapshot, "Force metastate to build snapshot");
        PYCXX_ADD_KEYWORDS_METHOD(gc_collect, GcCollect, "Run garbage collection");

        behaviors().readyType();
    }

    Py::Object Execute(Py::Tuple& args, Py::Dict &kwds)
    {
        auto pyRequest = ExtractArgument(args, kwds, "request");
        if (args.length() > 0 || kwds.length() > 0) {
            throw Py::RuntimeError("Incorrect arguments");
        }

        Py::Callable class_type(TDriverResponse::type());
        Py::PythonClassObject<TDriverResponse> pythonResponse(class_type.apply(Py::Tuple(), Py::Dict()));
        auto* response = pythonResponse.getCxxObject();

        TDriverRequest request;
        request.CommandName = ConvertToStroka(Py::String(GetAttr(pyRequest, "command_name")));
        request.Arguments = ConvertToNode(GetAttr(pyRequest, "arguments"))->AsMap();
        
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

        response->SetResponse(DriverInstance_->Execute(request));
        return pythonResponse;
    }
    PYCXX_KEYWORDS_METHOD_DECL(TDriver, Execute)

    Py::Object GetCommandDescriptor(Py::Tuple& args, Py::Dict &kwds)
    {
        auto commandName = ConvertToStroka(ConvertToString(ExtractArgument(args, kwds, "command_name")));
        if (args.length() > 0 || kwds.length() > 0) {
            throw Py::RuntimeError("Incorrect arguments");
        }
        
        Py::Callable class_type(TCommandDescriptor::type());
        Py::PythonClassObject<TCommandDescriptor> descriptor(class_type.apply(Py::Tuple(), Py::Dict()));
        descriptor.getCxxObject()->SetDescriptor(DriverInstance_->GetCommandDescriptor(commandName));
        return descriptor;
    }
    PYCXX_KEYWORDS_METHOD_DECL(TDriver, GetCommandDescriptor)
    
    Py::Object GetCommandDescriptors(Py::Tuple& args, Py::Dict &kwds)
    {
        if (args.length() > 0 || kwds.length() > 0) {
            throw Py::RuntimeError("Incorrect arguments");
        }

        auto descriptors = Py::List();
        FOREACH (const auto& nativeDescriptor, DriverInstance_->GetCommandDescriptors()) {
            Py::Callable class_type(TCommandDescriptor::type());
            Py::PythonClassObject<TCommandDescriptor> descriptor(class_type.apply(Py::Tuple(), Py::Dict()));
            descriptor.getCxxObject()->SetDescriptor(nativeDescriptor);
            descriptors.append(descriptor);
        }
        return descriptors;
    }
    PYCXX_KEYWORDS_METHOD_DECL(TDriver, GetCommandDescriptors)
    
    Py::Object ConfigureDispatcher(Py::Tuple& args, Py::Dict &kwds)
    {
        auto heavyPoolSize = Py::Int(ExtractArgument(args, kwds, "command_name")).asLongLong();
        if (args.length() > 0 || kwds.length() > 0) {
            throw Py::RuntimeError("Incorrect arguments");
        }

        NDriver::TDispatcher::Get()->Configure(heavyPoolSize);

        return Py::None();
    }
    PYCXX_KEYWORDS_METHOD_DECL(TDriver, ConfigureDispatcher)

    Py::Object GcCollect(Py::Tuple& args, Py::Dict &kwds)
    {
        NObjectClient::TObjectServiceProxy proxy(DriverInstance_->GetMasterChannel());
        proxy.SetDefaultTimeout(Null); // infinity
        auto req = proxy.GCCollect();
        auto rsp = req->Invoke().Get();
        if (!rsp->IsOK()) {
            return ConvertTo<Py::Object>(TError(*rsp));
        }
        return Py::None();
    }
    PYCXX_KEYWORDS_METHOD_DECL(TDriver, GcCollect)

    Py::Object BuildSnapshot(Py::Tuple& args, Py::Dict &kwds)
    {
        bool setReadOnly = false;
        if (args.length() > 0 || kwds.length() > 0) {
            setReadOnly = static_cast<bool>(Py::Boolean(ExtractArgument(args, kwds, "set_read_only")));
        }
        if (args.length() > 0 || kwds.length() > 0) {
            throw Py::RuntimeError("Incorrect arguments");
        }

        NMetaState::TMetaStateManagerProxy proxy(DriverInstance_->GetMasterChannel());
        proxy.SetDefaultTimeout(Null); // infinity
        auto req = proxy.BuildSnapshot();
        req->set_set_read_only(setReadOnly);

        auto rsp = req->Invoke().Get();
        if (!rsp->IsOK()) {
            return ConvertTo<Py::Object>(TError(*rsp));
        }

        int snapshotId = rsp->snapshot_id();
        printf("Snapshot %d is built\n", snapshotId);

        return Py::None();
    }
    PYCXX_KEYWORDS_METHOD_DECL(TDriver, BuildSnapshot)

private:
    IDriverPtr DriverInstance_;
};

///////////////////////////////////////////////////////////////////////////////

class driver_module
    : public Py::ExtensionModule<driver_module>
{
public:
    driver_module()
        : Py::ExtensionModule<driver_module>("driver")
    {
        RegisterShutdown();

        TDriver::InitType();
        TBufferedStreamWrap::InitType();
        TDriverResponse::InitType();
        TCommandDescriptor::InitType();

        initialize("Python bindings for driver");

        Py::Dict moduleDict(moduleDictionary());
        moduleDict["Driver"] = TDriver::type();
        moduleDict["BufferedStream"] = TBufferedStreamWrap::type();
    }

    virtual ~driver_module()
    { }
};

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
{ initdriver_lib(); }

