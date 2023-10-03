#pragma once

#include <library/cpp/logger/log.h>

#include <util/draft/ip.h>
#include <util/generic/fwd.h>
#include <util/generic/hash.h>
#include <util/generic/variant.h>
#include <util/network/sock.h>
#include <util/system/file.h>
#include <util/system/getpid.h>
#include <util/system/rwlock.h>

class TProcessUID {
    template <typename T>
    friend TString ToString(const T& t);

    template <typename T, typename TChar>
    friend bool TryFromStringImpl(const TChar* data, size_t len, T& result);

    template <typename T>
    friend struct THash;

public:
    TProcessUID()
        : TProcessUID(0, 0)
    {
    }

    /// For testing.
    TProcessUID(TProcessId pid, time_t ctime);

    /// Basic check that process is alive.
    bool CheckProcess(TLog* log = nullptr) const noexcept;

    bool operator==(const TProcessUID& other) const;
    bool operator!=(const TProcessUID& other) const {
        return !(operator==(other));
    }

    TProcessId GetPid() const {
        return Pid;
    }

    time_t GetStartTime() const {
        return StartTime;
    }

    static TProcessUID GetMyName();
    static TString GetMyUniqueSuffix();

    TString ToPlainString() const;

protected:
    /// Process start time to avoid collisions for Pid (in milliseconds).
    time_t StartTime;
    /// Process id.
    TProcessId Pid;
};

class TSystemWideName: public TProcessUID {
    template <typename T>
    friend TString ToString(const T& t);

    template <typename T, typename TChar>
    friend bool TryFromStringImpl(const TChar* data, size_t len, T& result);

    /// Network or local socket info.
    using TAddress = std::variant<std::pair<TIp4Or6, TIpPort>, TString>;

public:
    TSystemWideName()
        : TSystemWideName(TSystemWideName::TAddress(TString("")))
    {
    }

    /// Basic check that process is alive.
    bool CheckProcess(TLog* log = nullptr) const noexcept;

    bool operator==(const TSystemWideName& other) const;
    bool operator!=(const TSystemWideName& other) const {
        return !(operator==(other));
    }

    static TSystemWideName GetMyName(const TSockAddrInet6&);
    static TSystemWideName GetMyName(const TSockAddrInet&);
    static TSystemWideName GetMyName(const TSockAddrLocal&);

    TString ToGrpcAddress() const;

    TString ToPlainString() const;

    TString GetLocalSocket() const noexcept;

    void SetPort(TIpPort);

protected:
    TSystemWideName(const TAddress& address, TProcessId pid = 0, time_t ctime = 0);
    TSystemWideName(const TAddress& address, const TProcessUID& base);

public:
    /// ToString from each instance is smaller than this value.
    constexpr static int MAX_SIZE = 512;
    /// Socket timeout to check if server is alive.
    static int MSEC_SOCKET_TIMEOUT; // = 10;

protected:
    /// Network or local socket info.
    TAddress Address;
};

// Mode to capture lock.
enum ELockWait {
    Blocking,
    NonBlocking
};

// Check liveness of process after write to lock.
enum ECheckLiveness {
    CheckAlive,
    NoCheckAlive
};

template <typename Value>
class TSystemGuard : TNonCopyable {
public:
    using TValue = Value;
    /// Name of persistent resource where info about singleton process is
    /// stored.
    using TNamePtr = TStringBuf;
    /// Install action takes
    /// 1. stale information and
    /// 2. write-action and abort on failure.
    /// Error through exception.
    using TInstallOwner =
        std::function<TValue(TValue, std::function<void(TValue)>)>;

public:
    TSystemGuard(TNamePtr name, bool ro, TLog& log)
        : Name(name)
        , ReadOnly(ro)
        , Own(false)
        , Log(log)
    {
    }

    virtual ~TSystemGuard() {
    }

    /// Read and try to capture the lock Name.
    ///
    /// Checks if resource in Name is valid.
    /// If valid returns TValue stored in location pointed to by Name
    /// else try to capture resource and write info using \arg capture.
    ///
    /// May throw TLockFailed
    virtual std::pair<TValue, bool> TryToReplace(TInstallOwner capture, ELockWait blocking, ECheckLiveness check) = 0;

    /// Read info from Name.
    virtual TMaybe<TValue> Read(ELockWait blocking) noexcept = 0;

    /// Force write to Name.
    /// Suitable for maintenance mode.
    ///
    /// May throw TLockFailed
    virtual void Write(TInstallOwner capture, ELockWait blocking, ECheckLiveness check) = 0;

    bool IsCaptured() const {
        return Own;
    }

protected:
    void SetCaptured(bool own) {
        Own = own;
    }

protected:
    /// "Pointer" value. Managed TNamePtr.
    TString Name;
    /// Should not try to capture write lock if ReadOnly.
    bool ReadOnly;
    /// Whether `this` is owning instance.
    bool Own;
    /// Logger, externally managed
    TLog& Log;
};

template <typename Value>
class TFileReadWriteLock final: public TSystemGuard<Value> {
    using _Base = TSystemGuard<Value>;

public:
    using TValue = typename _Base::TValue;
    using TNamePtr = typename _Base::TNamePtr;
    using TInstallOwner = typename _Base::TInstallOwner;

    /// Type to throw if locking failed.
    struct TLockFailed: public virtual TSystemError {};

public:
    TFileReadWriteLock(TNamePtr fname, bool ro, TLog& log)
        : _Base(fname, ro, log)
    {
    }

    /// May throw TLockFailed and TFileError.
    std::pair<TValue, bool> TryToReplace(TInstallOwner capture, ELockWait blocking, ECheckLiveness check) override;
    TMaybe<TValue> Read(ELockWait blocking) noexcept override;
    /// May throw TLockFailed and TFileError.
    void Write(TInstallOwner capture, ELockWait blocking, ECheckLiveness check) override;

private:
    TValue CachedValue_;
    /// Synchronize within process: any thread can release file lock if closes
    /// __some__ file descriptor for the lock file.
    TRWMutex ThreadingLock_;

    /// Retry parameter. Retries can be performed externally as well.
    static i64 ATTEMPTS; // = 5;
    /// 3 millisecond pause between attempts to capture the lock.
    static i64 TIMEOUT; // = 3000;
};

template <typename SystemLocking, typename Value>
class TPSingleton final : TNonCopyable {
    static_assert(std::is_base_of<TSystemGuard<Value>, SystemLocking>::value,
                  "SystemLocking should be descendant of TSystemGuard");
    static_assert(std::is_same<typename TSystemGuard<Value>::TValue, Value>::value);

public:
    /// Server-side callback to install this process as owner.
    /// In maintenance mode should gracefully shutdown previous owner.
    using TServerStart = typename SystemLocking::TInstallOwner;
    using TServerStop = TServerStart;
    /// Persistent 'name' of resource holding name of singleton process.
    using TPersistentName = typename SystemLocking::TNamePtr;
    /// Name of the current process.
    using TValue = Value;

private:
    using TStart = TMaybe<TServerStart>;
    using TStop = TStart;

public:
    TPSingleton(TPersistentName name, TLog& log)
        : SystemLock_(new SystemLocking(name, true /*Read-only*/, log))
    {
    }

    TPSingleton(TPersistentName name, TServerStart serverStart, TServerStop serverStop, TLog& log)
        : SystemLock_(new SystemLocking(name, false /*Read-write*/, log))
        , StartProcedure_(serverStart)
        , StopProcedure_(serverStop)
    {
    }

    TMaybe<TValue> GetInstanceName(ELockWait blocking) noexcept {
        if (auto name = SystemLock_->Read(blocking)) {
            return name.GetRef();
        }
        if (!StartProcedure_.Empty()) {
            try {
                return MakeMaybe(GetInstanceNameForOwnership(blocking));
            } catch (const typename SystemLocking::TLockFailed&) {
                return TMaybe<TValue>();
            }
        }
        return SystemLock_->Read(blocking);
    }

    TValue GetInstanceNameForOwnership(ELockWait blocking, ECheckLiveness check = CheckAlive) {
        Y_VERIFY(!StartProcedure_.Empty());
        return SystemLock_->TryToReplace(StartProcedure_.GetRef(), blocking, check)
            .first;
    }

    void StartMaintenance(ELockWait blocking, ECheckLiveness check = CheckAlive) {
        Y_VERIFY(!StartProcedure_.Empty());
        SystemLock_->Write(StartProcedure_.GetRef(), blocking, check);
    }

    /// Stop shutdown myself holding lock.
    /// Reduces the number of retries to start new server.
    /// Do not try too hard. Shutdown from maintenance may hold the lock.
    void StopLocked(ELockWait blocking) {
        Y_VERIFY(!StopProcedure_.Empty() && IsCaptured());
        try {
            SystemLock_->Write(StopProcedure_.GetRef(), blocking, NoCheckAlive);
        } catch (const typename SystemLocking::TLockFailed&) {
            StopProcedure_.GetRef()(TValue(), [](TValue) -> void {});
        }
    }

    bool IsCaptured() const {
        return SystemLock_->IsCaptured();
    }

private:
    THolder<SystemLocking> SystemLock_;
    TStart StartProcedure_;
    TStop StopProcedure_;
};

/// Type being actually used.
using TPFileSingleton = TPSingleton<TFileReadWriteLock<TSystemWideName>, TSystemWideName>;

/// Singleton for Python.
/// fresh parameter is for tests to switch between lockFiles
TPFileSingleton* GetClientPSingleton(TStringBuf lockFile, TStringBuf logName, bool fresh = false);

#include "systemptr-impl.h"
