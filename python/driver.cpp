#include "public.h"
#include "buffered_stream.h"
#include "descriptor.h"
#include "helpers.h"
#include "response.h"
#include "serialize.h"
#include "shutdown.h"
#include "stream.h"

#include <yt/ytlib/api/admin.h>
#include <yt/ytlib/api/connection.h>

#include <yt/ytlib/driver/config.h>
#include <yt/ytlib/driver/driver.h>

#include <yt/ytlib/formats/format.h>

#include <yt/ytlib/hydra/hydra_service_proxy.h>

#include <yt/ytlib/object_client/object_service_proxy.h>

#include <yt/ytlib/tablet_client/public.h>

#include <yt/core/concurrency/async_stream.h>

#include <yt/core/logging/log_manager.h>

#include <yt/core/misc/intrusive_ptr.h>

#include <yt/core/tracing/trace_manager.h>

#include <yt/core/ytree/convert.h>

#include <contrib/libs/pycxx/Extensions.hxx>
#include <contrib/libs/pycxx/Objects.hxx>

#include <iostream>

namespace NYT {
namespace NPython {

///////////////////////////////////////////////////////////////////////////////

using namespace NFormats;
using namespace NDriver;
using namespace NYson;
using namespace NYTree;
using namespace NConcurrency;
using namespace NTabletClient;

///////////////////////////////////////////////////////////////////////////////

Py::Exception CreateYtError(const NYT::TError& error)
{
    auto ytErrorClass = Py::Callable(
        PyObject_GetAttr(
            PyImport_ImportModule("yt.common"),
            PyString_FromString("YtError")));

    Py::Dict options;
    options.setItem("message", ConvertTo<Py::Object>(error.GetMessage()));
    options.setItem("code", ConvertTo<Py::Object>(error.GetCode()));
    options.setItem("inner_errors", ConvertTo<Py::Object>(error.InnerErrors()));
    auto ytError = ytErrorClass.apply(Py::Tuple(), options);
    return Py::Exception(*ytErrorClass, ytError);
}

Py::Exception CreateYtError(const std::string& message)
{
    auto ytErrorClass = Py::Object(
        PyObject_GetAttr(
            PyImport_ImportModule("yt.common"),
            PyString_FromString("YtError")));
    return Py::Exception(*ytErrorClass, message);
}

#define CATCH \
    catch (const NYT::TErrorException& error) { \
        throw CreateYtError(error.Error()); \
    } catch (const std::exception& ex) { \
        if (PyErr_ExceptionMatches(PyExc_BaseException)) { \
            throw; \
        } else { \
            throw CreateYtError(ex.what()); \
        } \
    }

///////////////////////////////////////////////////////////////////////////////

class TDriver
    : public Py::PythonClass<TDriver>
{
public:
    TDriver(Py::PythonClassInstance *self, Py::Tuple& args, Py::Dict& kwargs)
        : Py::PythonClass<TDriver>::PythonClass(self, args, kwargs)
    {
        auto configDict = ExtractArgument(args, kwargs, "config");
        ValidateArgumentsEmpty(args, kwargs);

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
        behaviors().doc("Represents a YT driver");
        behaviors().supportGetattro();
        behaviors().supportSetattro();

        PYCXX_ADD_KEYWORDS_METHOD(execute, Execute, "Executes the request");
        PYCXX_ADD_KEYWORDS_METHOD(get_command_descriptor, GetCommandDescriptor, "Describes the command");
        PYCXX_ADD_KEYWORDS_METHOD(get_command_descriptors, GetCommandDescriptors, "Describes all commands");
        PYCXX_ADD_KEYWORDS_METHOD(build_snapshot, BuildSnapshot, "Force to build a snapshot");
        PYCXX_ADD_KEYWORDS_METHOD(gc_collect, GCCollect, "Run garbage collection");
        PYCXX_ADD_KEYWORDS_METHOD(clear_metadata_caches, ClearMetadataCaches, "Clear metadata caches");

        behaviors().readyType();
    }

    Py::Object Execute(Py::Tuple& args, Py::Dict& kwargs)
    {
        auto pyRequest = ExtractArgument(args, kwargs, "request");
        ValidateArgumentsEmpty(args, kwargs);

        Py::Callable classType(TDriverResponse::type());
        Py::PythonClassObject<TDriverResponse> pythonResponse(classType.apply(Py::Tuple(), Py::Dict()));
        auto* response = pythonResponse.getCxxObject();

        TDriverRequest request;
        request.CommandName = ConvertToStroka(Py::String(GetAttr(pyRequest, "command_name")));
        request.Parameters = ConvertToNode(GetAttr(pyRequest, "parameters"))->AsMap();
        request.ResponseParametersConsumer = response->GetResponseParametersConsumer();

        auto user = GetAttr(pyRequest, "user");
        if (!user.isNone()) {
            request.AuthenticatedUser = ConvertToStroka(Py::String(user));
        }

        auto inputStreamObj = GetAttr(pyRequest, "input_stream");
        if (!inputStreamObj.isNone()) {
            std::unique_ptr<TInputStreamWrap> inputStream(new TInputStreamWrap(inputStreamObj));
            request.InputStream = CreateAsyncAdapter(inputStream.get());
            response->OwnInputStream(inputStream);
        }

        auto outputStreamObj = GetAttr(pyRequest, "output_stream");
        TBufferedStreamWrap* bufferedOutputStream = nullptr;
        if (!outputStreamObj.isNone()) {
            bool isBufferedStream = PyObject_IsInstance(outputStreamObj.ptr(), TBufferedStreamWrap::type().ptr());
            if (isBufferedStream) {
                bufferedOutputStream = dynamic_cast<TBufferedStreamWrap*>(Py::getPythonExtensionBase(outputStreamObj.ptr()));
                request.OutputStream = bufferedOutputStream->GetStream();
            } else {
                std::unique_ptr<TOutputStreamWrap> outputStream(new TOutputStreamWrap(outputStreamObj));
                request.OutputStream = CreateAsyncAdapter(outputStream.get());
                response->OwnOutputStream(outputStream);
            }
        }

        try {
            auto driverResponse = DriverInstance_->Execute(request);
            response->SetResponse(driverResponse);
            if (bufferedOutputStream) {
                auto outputStream = bufferedOutputStream->GetStream();
                driverResponse.Subscribe(BIND([=] (TError error) {
                    outputStream->Finish();
                }));
            }
        } CATCH;

        return pythonResponse;
    }
    PYCXX_KEYWORDS_METHOD_DECL(TDriver, Execute)

    Py::Object GetCommandDescriptor(Py::Tuple& args, Py::Dict& kwargs)
    {
        auto commandName = ConvertToStroka(ConvertToString(ExtractArgument(args, kwargs, "command_name")));
        ValidateArgumentsEmpty(args, kwargs);

        Py::Callable class_type(TCommandDescriptor::type());
        Py::PythonClassObject<TCommandDescriptor> descriptor(class_type.apply(Py::Tuple(), Py::Dict()));
        try {
            descriptor.getCxxObject()->SetDescriptor(DriverInstance_->GetCommandDescriptor(commandName));
        } CATCH;

        return descriptor;
    }
    PYCXX_KEYWORDS_METHOD_DECL(TDriver, GetCommandDescriptor)

    Py::Object GetCommandDescriptors(Py::Tuple& args, Py::Dict& kwargs)
    {
        ValidateArgumentsEmpty(args, kwargs);

        try {
            auto descriptors = Py::Dict();
            for (const auto& nativeDescriptor : DriverInstance_->GetCommandDescriptors()) {
                Py::Callable class_type(TCommandDescriptor::type());
                Py::PythonClassObject<TCommandDescriptor> descriptor(class_type.apply(Py::Tuple(), Py::Dict()));
                descriptor.getCxxObject()->SetDescriptor(nativeDescriptor);
                descriptors.setItem(~nativeDescriptor.CommandName, descriptor);
            }
            return descriptors;
        } CATCH;
    }
    PYCXX_KEYWORDS_METHOD_DECL(TDriver, GetCommandDescriptors)

    Py::Object GCCollect(Py::Tuple& args, Py::Dict& kwargs)
    {
        try {
            auto admin = DriverInstance_->GetConnection()->CreateAdmin();
            WaitFor(admin->GCCollect())
                .ThrowOnError();
            return Py::None();
        } CATCH;
    }
    PYCXX_KEYWORDS_METHOD_DECL(TDriver, GCCollect)

    Py::Object BuildSnapshot(Py::Tuple& args, Py::Dict& kwargs)
    {
        auto options = NApi::TBuildSnapshotOptions();

        if (HasArgument(args, kwargs, "set_read_only")) {
            options.SetReadOnly = static_cast<bool>(Py::Boolean(ExtractArgument(args, kwargs, "set_read_only")));
        }

        if (HasArgument(args, kwargs, "cell_id")) {
            auto cellIdStr = ConvertToStroka(ConvertToString(ExtractArgument(args, kwargs, "cell_id")));
            options.CellId = TTabletCellId::FromString(cellIdStr);
        }

        ValidateArgumentsEmpty(args, kwargs);

        try {
            auto admin = DriverInstance_->GetConnection()->CreateAdmin();
            int snapshotId = WaitFor(admin->BuildSnapshot(options))
                .ValueOrThrow();
            return Py::Long(snapshotId);
        } CATCH;
    }
    PYCXX_KEYWORDS_METHOD_DECL(TDriver, BuildSnapshot)

    Py::Object ClearMetadataCaches(Py::Tuple& args, Py::Dict& kwargs)
    {
        ValidateArgumentsEmpty(args, kwargs);

        try {
            DriverInstance_->GetConnection()->ClearMetadataCaches();
            return Py::None();
        } CATCH;
    }
    PYCXX_KEYWORDS_METHOD_DECL(TDriver, ClearMetadataCaches)

private:
    IDriverPtr DriverInstance_;
};

///////////////////////////////////////////////////////////////////////////////

class TDriverModule
    : public Py::ExtensionModule<TDriverModule>
{
public:
    TDriverModule()
        // This should be the same as .so file name.
        : Py::ExtensionModule<TDriverModule>("driver_lib")
    {
        PyEval_InitThreads();

        RegisterShutdown();

        TDriver::InitType();
        TBufferedStreamWrap::InitType();
        TDriverResponse::InitType();
        TCommandDescriptor::InitType();

        add_keyword_method("configure_logging", &TDriverModule::ConfigureLogging, "Configures YT driver logging");
        add_keyword_method("configure_tracing", &TDriverModule::ConfigureTracing, "Configures YT driver tracing");

        initialize("Python bindings for YT driver");

        Py::Dict moduleDict(moduleDictionary());
        moduleDict["Driver"] = TDriver::type();
        moduleDict["BufferedStream"] = TBufferedStreamWrap::type();
        moduleDict["Response"] = TDriverResponse::type();
    }

    Py::Object ConfigureLogging(const Py::Tuple& args_, const Py::Dict& kwargs_)
    {
        auto args = args_;
        auto kwargs = kwargs_;

        auto config = ConvertToNode(ExtractArgument(args, kwargs, "config"));
        ValidateArgumentsEmpty(args, kwargs);

        NLogging::TLogManager::Get()->Configure(config->AsMap());

        return Py::None();
    }

    Py::Object ConfigureTracing(const Py::Tuple& args_, const Py::Dict& kwargs_)
    {
        auto args = args_;
        auto kwargs = kwargs_;

        auto config = ConvertToNode(ExtractArgument(args, kwargs, "config"));
        ValidateArgumentsEmpty(args, kwargs);

        NTracing::TTraceManager::Get()->Configure(config->AsMap());

        return Py::None();
    }

    virtual ~TDriverModule()
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
    static const auto* driver = new NYT::NPython::TDriverModule;
    Y_UNUSED(driver);
}

// This symbol is required for debug version.
extern "C" EXPORT_SYMBOL void initdriver_lib_d()
{
    initdriver_lib();
}
