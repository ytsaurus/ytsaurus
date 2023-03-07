#include "shutdown.h"

#include <yt/core/misc/shutdown.h>

#include <Objects.hxx> // pycxx
#include <Extensions.hxx> // pycxx

#include <array>

namespace NYT::NPython {

////////////////////////////////////////////////////////////////////////////////

static constexpr int MaxAdditionalShutdownCallbackCount = 10;
static std::array<TCallback<void()>, MaxAdditionalShutdownCallbackCount> BeforeFinalizeShutdownCallbacks;
static std::array<TCallback<void()>, MaxAdditionalShutdownCallbackCount> AfterFinalizeShutdownCallbacks;
static std::array<TCallback<void()>, MaxAdditionalShutdownCallbackCount> AfterShutdownCallbacks;

////////////////////////////////////////////////////////////////////////////////

void Shutdown();

////////////////////////////////////////////////////////////////////////////////

class TShutdownModule
    : public Py::ExtensionModule<TShutdownModule>
{
public:
    TShutdownModule()
        : Py::ExtensionModule<TShutdownModule>("yt_shutdown_lib")
    {
        PyEval_InitThreads();

        add_keyword_method("shutdown", &TShutdownModule::Shutdown, "Performs python-side shutdown for yt bindings");

        initialize("Python bindings for shutdown");

        Py::Dict moduleDict(moduleDictionary());

        auto atexitModule = Py::Module(PyImport_ImportModule("atexit"), /* owned */ true);
        auto registerFunc = Py::Callable(PyObject_GetAttrString(atexitModule.ptr(), "register"), /* owned */ true);
        registerFunc.apply(Py::TupleN(moduleDict.getItem("shutdown")), Py::Dict());

        Py_AtExit(NPython::Shutdown);
    }

    virtual ~TShutdownModule()
    { }

    Py::Object Shutdown(const Py::Tuple& /*args_*/, const Py::Dict& /*kwargs_*/)
    {
        for (int index = 0; index < MaxAdditionalShutdownCallbackCount; ++index) {
            const auto& callback = BeforeFinalizeShutdownCallbacks[index];
            if (callback) {
                callback.Run();
            }
        }
        return Py::None();
    }
};

////////////////////////////////////////////////////////////////////////////////

void Shutdown()
{
    for (int index = 0; index < MaxAdditionalShutdownCallbackCount; ++index) {
        const auto& callback = AfterFinalizeShutdownCallbacks[index];
        if (callback) {
            callback.Run();
        }
    }

    NYT::Shutdown();

    for (int index = 0; index < MaxAdditionalShutdownCallbackCount; ++index) {
        const auto& callback = AfterShutdownCallbacks[index];
        if (callback) {
            callback.Run();
        }
    }
}

void RegisterBeforeFinalizeShutdownCallback(TCallback<void()> callback, int index)
{
    YT_VERIFY(0 <= index && index < MaxAdditionalShutdownCallbackCount);
    BeforeFinalizeShutdownCallbacks[index] = callback;
}

void RegisterAfterFinalizeShutdownCallback(TCallback<void()> callback, int index)
{
    YT_VERIFY(0 <= index && index < MaxAdditionalShutdownCallbackCount);
    AfterFinalizeShutdownCallbacks[index] = callback;
}

void RegisterAfterShutdownCallback(TCallback<void()> callback, int index)
{
    YT_VERIFY(0 <= index && index < MaxAdditionalShutdownCallbackCount);
    AfterShutdownCallbacks[index] = callback;
}

void RegisterShutdown()
{
    static TShutdownModule* shutdown = new NYT::NPython::TShutdownModule;
    Y_UNUSED(shutdown);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NPython
