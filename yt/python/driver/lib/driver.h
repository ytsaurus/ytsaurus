#pragma once

#include <yt/python/common/helpers.h>

#include <yt/client/api/config.h>

#include <yt/client/driver/driver.h>

namespace NYT::NPython {

////////////////////////////////////////////////////////////////////////////////

#define PYCXX_DECLARE_DRIVER_METHODS(className) \
    PYCXX_KEYWORDS_METHOD_DECL(className, Execute) \
    PYCXX_KEYWORDS_METHOD_DECL(className, GetCommandDescriptor) \
    PYCXX_KEYWORDS_METHOD_DECL(className, GetCommandDescriptors) \
    PYCXX_KEYWORDS_METHOD_DECL(className, Terminate) \
    PYCXX_KEYWORDS_METHOD_DECL(className, GetConfig) \
    PYCXX_VARARGS_METHOD_DECL(className, DeepCopy)

#define PYCXX_ADD_DRIVER_METHODS \
    PYCXX_ADD_KEYWORDS_METHOD(execute, Execute, "Executes the request"); \
    PYCXX_ADD_KEYWORDS_METHOD(get_command_descriptor, GetCommandDescriptor, "Describes the command"); \
    PYCXX_ADD_KEYWORDS_METHOD(get_command_descriptors, GetCommandDescriptors, "Describes all commands"); \
    PYCXX_ADD_KEYWORDS_METHOD(terminate, Terminate, "Terminate driver"); \
    PYCXX_ADD_KEYWORDS_METHOD(get_config, GetConfig, "Get config"); \
    PYCXX_ADD_VARARGS_METHOD(__deepcopy__, DeepCopy, "Deep copy Driver object");


////////////////////////////////////////////////////////////////////////////////

class TDriverBase
{
public:
    TDriverBase();
    ~TDriverBase();

    void Initialize(
        const NDriver::IDriverPtr& driver,
        const NYTree::INodePtr& configNode);

    Py::Object Execute(Py::Tuple& args, Py::Dict& kwargs);
    Py::Object GetCommandDescriptor(Py::Tuple& args, Py::Dict& kwargs);
    Py::Object GetCommandDescriptors(Py::Tuple& args, Py::Dict& kwargs);
    Py::Object Terminate(const Py::Tuple& args, const Py::Dict& kwargs);
    Py::Object GetConfig(const Py::Tuple& args, const Py::Dict& kwargs);
    Py::Object DeepCopy(const Py::Tuple& args);

private:
    const TGuid Id_;
    bool Terminated_ = false;
    bool Initialized_ = false;

    void DoTerminate();

protected:
    NDriver::IDriverPtr UnderlyingDriver_;
    NYTree::INodePtr ConfigNode_;

    const NLogging::TLogger Logger;

    virtual Py::Type GetDriverType() const = 0;
};

////////////////////////////////////////////////////////////////////////////////

void InitializeDriverModule();

////////////////////////////////////////////////////////////////////////////////

class TDriverModuleBase
{
public:
    typedef Py::Object (TDriverModuleBase::*PycxxMethod)(const Py::Tuple &args, const Py::Dict& kwargs);

    void Initialize(
        const TString& moduleName,
        std::function<void()> initTypeFunction,
        std::function<void()> initModule,
        std::function<Py::Dict()> getModuleDictionary,
        std::function<void(const char*, PycxxMethod, const char*)> addPycxxMethod);

    Py::Object ConfigureLogging(const Py::Tuple& args_, const Py::Dict& kwargs_);
    Py::Object ConfigureAddressResolver(const Py::Tuple& args_, const Py::Dict& kwargs_);
    Py::Object ConfigureTracing(const Py::Tuple& args_, const Py::Dict& kwargs_);
    Py::Object ReopenLogs(const Py::Tuple& args_, const Py::Dict& kwargs_);
    Py::Object Shutdown(const Py::Tuple& args_, const Py::Dict& kwargs_);
    Py::Object InternalShutdown(const Py::Tuple& args_, const Py::Dict& kwargs_);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NPython
