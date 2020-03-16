#include "shutdown.h"

#include <yt/core/misc/shutdown.h>

#include <Objects.hxx> // pycxx

#include <array>

namespace NYT::NPython {

////////////////////////////////////////////////////////////////////////////////

static constexpr int MaxAdditionalShutdownCallbackCount = 10;
static std::array<TCallback<void()>, MaxAdditionalShutdownCallbackCount> AdditionalShutdownCallbacks;

void Shutdown()
{
    for (int index = 0; index < MaxAdditionalShutdownCallbackCount; ++index) {
        auto& callback = AdditionalShutdownCallbacks[index];
        if (callback) {
            callback.Run();
            callback.Reset();
        }
    }
    NYT::Shutdown();
}

void RegisterShutdownCallback(TCallback<void()> additionalCallback, int index)
{
    YT_VERIFY(0 <= index && index < MaxAdditionalShutdownCallbackCount);
    AdditionalShutdownCallbacks[index] = additionalCallback;
}

void RegisterShutdown(const Py::Object& beforePythonFinalizeCallback)
{
    static bool registered = false;

    if (!registered) {
        registered = true;

        if (!beforePythonFinalizeCallback.isNone()) {
            auto atexitModule = Py::Module(PyImport_ImportModule("atexit"), /* owned */ true);
            auto registerFunc = Py::Callable(PyObject_GetAttrString(atexitModule.ptr(), "register"), /* owned */ true);
            registerFunc.apply(Py::TupleN(beforePythonFinalizeCallback), Py::Dict());
        }

        Py_AtExit(NPython::Shutdown);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NPython

