#include <yt/python/driver/lib/descriptor.h>
#include <yt/python/driver/lib/response.h>
#include <yt/python/driver/lib/error.h>
#include <yt/python/driver/lib/driver.h>

#include <yt/python/common/helpers.h>

#include <yt/client/api/rpc_proxy/connection.h>
#include <yt/client/api/rpc_proxy/config.h>

#include <yt/client/api/admin.h>
#include <yt/client/api/transaction.h>

#include <yt/client/driver/config.h>

#include <yt/core/logging/log_manager.h>
#include <yt/core/logging/config.h>

#include <yt/core/misc/shutdown.h>

#include <yt/core/tracing/trace_manager.h>
#include <yt/core/tracing/config.h>

namespace NYT::NPython {

////////////////////////////////////////////////////////////////////////////////

using namespace NFormats;
using namespace NDriver;
using namespace NYson;
using namespace NYTree;
using namespace NConcurrency;
using namespace NApi;

////////////////////////////////////////////////////////////////////////////////

class TDriverRpc
    : public Py::PythonClass<TDriverRpc>
    , public TDriverBase
{
public:
    TDriverRpc(Py::PythonClassInstance *self, Py::Tuple& args, Py::Dict& kwargs)
        : Py::PythonClass<TDriverRpc>::PythonClass(self, args, kwargs)
    {
        auto configDict = ExtractArgument(args, kwargs, "config");
        ValidateArgumentsEmpty(args, kwargs);

        INodePtr configNode;
        IDriverPtr driver;

        try {
            configNode = ConvertToNode(configDict);

            auto connectionConfig = ConvertTo<NRpcProxy::TConnectionConfigPtr>(configNode);
            auto connection = NRpcProxy::CreateConnection(connectionConfig);

            auto driverConfig = ConvertTo<TDriverConfigPtr>(configNode);
            driver = CreateDriver(connection, driverConfig);
        } catch(const std::exception& ex) {
            throw Py::RuntimeError(TString("Error creating driver\n") + ex.what());
        }

        TDriverBase::Initialize(driver, configNode);
    }

    static void InitType()
    {
        behaviors().name("driver_rpc_lib.Driver");
        behaviors().doc("Represents a YT RPC driver");
        behaviors().supportGetattro();
        behaviors().supportSetattro();

        PYCXX_ADD_DRIVER_METHODS

        behaviors().readyType();
    }

    PYCXX_DECLARE_DRIVER_METHODS(TDriverRpc)

    virtual Py::Type GetDriverType() const override
    {
        return TDriverRpc::type();
    }
};

////////////////////////////////////////////////////////////////////////////////

class TDriverRpcModule
    : public Py::ExtensionModule<TDriverRpcModule>
    , public TDriverModuleBase
{
public:
    TDriverRpcModule()
        // This should be the same as .so file name.
        : Py::ExtensionModule<TDriverRpcModule>("driver_rpc_lib")
    {
        TDriverModuleBase::Initialize(
            "driver_rpc_lib",
            [](){TDriverRpc::InitType();},
            [&](){initialize("Python RPC bindings for YT driver");},
            std::bind(&TDriverRpcModule::moduleDictionary, this),
            &TDriverRpcModule::add_keyword_method);

        moduleDictionary().setItem("Driver", TDriverRpc::type());
    }

    virtual ~TDriverRpcModule() = default;
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
    static auto* driverRpcModule = new NYT::NPython::TDriverRpcModule;
    return driverRpcModule->module().ptr();
}

#if PY_MAJOR_VERSION < 3
extern "C" EXPORT_SYMBOL void initdriver_rpc_lib() { Y_UNUSED(init_module()); }
extern "C" EXPORT_SYMBOL void initdriver_rpc_lib_d() { initdriver_rpc_lib(); }
#else
extern "C" EXPORT_SYMBOL PyObject* PyInit_driver_rpc_lib() { return init_module(); }
extern "C" EXPORT_SYMBOL PyObject* PyInit_driver_rpc_lib_d() { return PyInit_driver_rpc_lib(); }
#endif
