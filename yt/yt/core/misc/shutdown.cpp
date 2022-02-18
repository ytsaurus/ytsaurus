#include "shutdown.h"

#include <yt/yt/core/misc/collection_helpers.h>
#include <yt/yt/core/misc/proc.h>
#include <yt/yt/core/misc/singleton.h>

#include <library/cpp/yt/threading/fork_aware_spin_lock.h>
#include <library/cpp/yt/threading/event_count.h>

#include <util/generic/algorithm.h>

#include <util/system/env.h>
#include <util/system/thread.h>

#include <thread>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

class TShutdownManager
{
public:
    static TShutdownManager* Get()
    {
        return LeakySingleton<TShutdownManager>();
    }

    TShutdownCookie RegisterShutdownCallback(
        TString name,
        TClosure callback,
        int priority)
    {
        auto guard = Guard(Lock_);

        if (ShutdownStarted_.load()) {
            if (auto* logFile = GetShutdownLogFile()) {
                ::fprintf(logFile, "*** Attempt to register shutdown callback when shutdown is already in progress (Name: %s)\n",
                    name.c_str());
            }
            return nullptr;
        }

        auto registeredCallback = New<TRefCountedRegisteredCallback>();
        registeredCallback->Name = std::move(name);
        registeredCallback->Callback = std::move(callback);
        registeredCallback->Priority = priority;
        InsertOrCrash(RegisteredCallbacks_, registeredCallback.Get());

        if (auto* logFile = GetShutdownLogFile()) {
            ::fprintf(logFile, "*** Shutdown callback registered (Name: %s, Priority: %d)\n",
                registeredCallback->Name.c_str(),
                registeredCallback->Priority);
        }

        return registeredCallback;
    }

    void Shutdown(const TShutdownOptions& options)
    {
        std::vector<TRegisteredCallback> registeredCallbacks;

        {
            auto guard = Guard(Lock_);

            if (ShutdownStarted_.load()) {
                return;
            }

            ShutdownStarted_.store(true);
            ShutdownThreadId_.store(GetCurrentThreadId());

            if (auto* logFile = GetShutdownLogFile()) {
                ::fprintf(logFile, "*** Shutdown started (ThreadId: %" PRISZT ")\n",
                    GetCurrentThreadId());
            }

            for (auto* registeredCallback : RegisteredCallbacks_) {
                registeredCallbacks.push_back(*registeredCallback);
            }
        }

        SortBy(registeredCallbacks, [] (const auto& registeredCallback) {
            return registeredCallback.Priority;
        });

        NThreading::TEvent shutdownCompleteEvent;
        std::thread watchdogThread([&] {
            ::TThread::SetCurrentThreadName("ShutdownWD");
            if (!shutdownCompleteEvent.Wait(options.GraceTimeout)) {
                if (options.AbortOnHang) {
                    ::fprintf(stderr, "*** Shutdown hung, aborting\n");
                    YT_ABORT();
                } else {
                    ::fprintf(stderr, "*** Shutdown hung, exiting\n");
                    ::_exit(options.HungExitCode);
                }
            }
        });

        for (auto it = registeredCallbacks.rbegin(); it != registeredCallbacks.rend(); it++) {
            const auto& registeredCallback = *it;
            if (auto* logFile = GetShutdownLogFile()) {
                ::fprintf(logFile, "*** Running callback (Name: %s, Priority: %d)\n",
                    registeredCallback.Name.c_str(),
                    registeredCallback.Priority);
            }
            registeredCallback.Callback();
        }

        shutdownCompleteEvent.NotifyOne();
        watchdogThread.join();

        if (auto* logFile = GetShutdownLogFile()) {
            ::fprintf(logFile, "*** Shutdown completed\n");
        }
    }

    bool IsShutdownStarted()
    {
        return ShutdownStarted_.load();
    }

    void EnableShutdownLoggingToStderr()
    {
        ShutdownLogFile_.store(stderr);
    }

    void EnableShutdownLoggingToFile(const TString& fileName)
    {
        auto* file = fopen(fileName.c_str(), "w");
        if (!file) {
            ::fprintf(stderr, "*** Could not open the shutdown logging file\n");
            return;
        }
        // Although POSIX guarantees fprintf always to be thread-safe (see fprintf(2)),
        // it seems to be a good idea to disable buffering for the log file.
        ::setvbuf(file, nullptr, _IONBF, 0);
        ShutdownLogFile_.store(file);
    }

    FILE* GetShutdownLogFile()
    {
        return ShutdownLogFile_.load();
    }

    size_t GetShutdownThreadId()
    {
        return ShutdownThreadId_.load();
    }

private:
    std::atomic<FILE*> ShutdownLogFile_ = IsShutdownLoggingEnabledImpl() ? stderr : nullptr;

    NThreading::TForkAwareSpinLock Lock_;

    struct TRegisteredCallback
    {
        TString Name;
        TClosure Callback;
        int Priority;
    };

    struct TRefCountedRegisteredCallback
        : public TRegisteredCallback
        , public TRefCounted
    {
        ~TRefCountedRegisteredCallback()
        {
            TShutdownManager::Get()->UnregisterShutdownCallback(this);
        }
    };

    std::unordered_set<TRefCountedRegisteredCallback*> RegisteredCallbacks_;
    std::atomic<bool> ShutdownStarted_ = false;
    std::atomic<size_t> ShutdownThreadId_ = 0;


    static bool IsShutdownLoggingEnabledImpl()
    {
        auto value = GetEnv("YT_ENABLE_SHUTDOWN_LOGGING");
        value.to_lower();
        return value == "1" || value == "true";
    }

    void UnregisterShutdownCallback(TRefCountedRegisteredCallback* registeredCallback)
    {
        auto guard = Guard(Lock_);
        if (auto* logFile = GetShutdownLogFile()) {
            ::fprintf(logFile, "*** Shutdown callback unregistered (Name: %s, Priority: %d)\n",
                registeredCallback->Name.c_str(),
                registeredCallback->Priority);
        }
        EraseOrCrash(RegisteredCallbacks_, registeredCallback);
    }

    DECLARE_LEAKY_SINGLETON_FRIEND()
};

////////////////////////////////////////////////////////////////////////////////

TShutdownCookie RegisterShutdownCallback(
    TString name,
    TClosure callback,
    int priority)
{
    return TShutdownManager::Get()->RegisterShutdownCallback(
        std::move(name),
        std::move(callback),
        priority);
}

void Shutdown(const TShutdownOptions& options)
{
    TShutdownManager::Get()->Shutdown(options);
}

bool IsShutdownStarted()
{
    return TShutdownManager::Get()->IsShutdownStarted();
}

void EnableShutdownLoggingToStderr()
{
    TShutdownManager::Get()->EnableShutdownLoggingToStderr();
}

void EnableShutdownLoggingToFile(const TString& fileName)
{
    TShutdownManager::Get()->EnableShutdownLoggingToFile(fileName);
}

FILE* GetShutdownLogFile()
{
    return TShutdownManager::Get()->GetShutdownLogFile();
}

size_t GetShutdownThreadId()
{
    return TShutdownManager::Get()->GetShutdownThreadId();
}

////////////////////////////////////////////////////////////////////////////////

static const void* ShutdownGuardInitializer = [] {
    class TShutdownGuard
    {
    public:
        ~TShutdownGuard()
        {
            if (auto* logFile = TShutdownManager::Get()->GetShutdownLogFile()) {
                fprintf(logFile, "*** Shutdown guard destructed\n");
            }
            Shutdown();
        }
    };

    static thread_local TShutdownGuard Guard;
    return nullptr;
}();

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
