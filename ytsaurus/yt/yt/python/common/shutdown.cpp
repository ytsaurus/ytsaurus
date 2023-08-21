#include "shutdown.h"
#include "helpers.h"

#include <yt/yt/core/misc/shutdown.h>

#include <yt/yt/core/actions/future.h>

#include <CXX/Objects.hxx> // pycxx
#include <CXX/Extensions.hxx> // pycxx

#include <array>

namespace NYT::NPython {

////////////////////////////////////////////////////////////////////////////////

static constexpr int MaxAdditionalShutdownCallbackCount = 10;
static std::array<TCallback<void()>, MaxAdditionalShutdownCallbackCount> BeforeFinalizeShutdownCallbacks;
static std::array<TCallback<void()>, MaxAdditionalShutdownCallbackCount> AfterFinalizeShutdownCallbacks;
static std::array<TCallback<void()>, MaxAdditionalShutdownCallbackCount> AfterShutdownCallbacks;

////////////////////////////////////////////////////////////////////////////////

static std::atomic<i64> ReleaseAcquiredCounter = 0;

static YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, FutureSpinLock);
static i64 FutureCookieCounter = 0;
static bool FutureFinalizationStarted = false;
static THashMap<TFutureCookie, TFuture<void>> RegisteredFutures;

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
#if PY_VERSION_HEX < 0x03090000
        PyEval_InitThreads();
#endif

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
            if (const auto& callback = BeforeFinalizeShutdownCallbacks[index]) {
                callback();
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

void EnterReleaseAcquireGuard()
{
    ++ReleaseAcquiredCounter;
}

void LeaveReleaseAcquireGuard()
{
    --ReleaseAcquiredCounter;
}

TFutureCookie RegisterFuture(TFuture<void> future)
{
    auto guard = Guard(FutureSpinLock);

    if (FutureFinalizationStarted) {
        return InvalidFutureCookie;
    }

    ++FutureCookieCounter;
    YT_VERIFY(RegisteredFutures.emplace(FutureCookieCounter, std::move(future)).second);

    return FutureCookieCounter;
}

void UnregisterFuture(TFutureCookie cookie)
{
    auto guard = Guard(FutureSpinLock);

    YT_VERIFY(RegisteredFutures.erase(cookie) == 1);
}

void FinalizeFutures()
{
    bool hasUnsetFuture = false;
    {
        auto guard = Guard(FutureSpinLock);

        FutureFinalizationStarted = true;

        for (const auto& [_, future] : RegisteredFutures) {
            if (!future.IsSet()) {
                hasUnsetFuture = true;
                future.Cancel(TError(NYT::EErrorCode::Canceled, "Python finalization started"));
            }
        }
    }

    if (hasUnsetFuture) {
        TReleaseAcquireGilGuard guard;
        while (ReleaseAcquiredCounter.load() > 1) {
            Sleep(TDuration::MilliSeconds(100));
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NPython
