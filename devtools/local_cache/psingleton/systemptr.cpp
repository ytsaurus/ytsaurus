#include "systemptr.h"

#include <devtools/local_cache/common/logger-utils/fallback_logger.h>

#include <library/cpp/logger/null.h>
#include <library/cpp/logger/rotating_file.h>

#include <library/cpp/logger/global/common.h>
#include <library/cpp/logger/global/rty_formater.h>

#include <util/datetime/systime.h>
#include <util/folder/path.h>
#include <util/generic/bt_exception.h>
#include <util/generic/scope.h>
#include <util/generic/singleton.h>
#include <util/generic/strbuf.h>
#include <util/generic/utility.h>
#include <util/network/sock.h>
#include <util/stream/file.h>
#include <util/string/cast.h>

#include <contrib/libs/grpc/src/proto/grpc/health/v1/health.grpc.pb.h>
#include <contrib/libs/grpc/include/grpcpp/create_channel.h>
#include <contrib/libs/grpc/include/grpcpp/resource_quota.h>
#include <contrib/libs/grpc/include/grpcpp/security/credentials.h>

#include <contrib/libs/grpc/include/grpc/grpc.h>

#include <chrono>

TProcessUID::TProcessUID(TProcessId pid, time_t ctime)
    : StartTime(ctime)
    , Pid(pid)
{
}

bool TProcessUID::CheckProcess(TLog* log) const noexcept {
    if (Pid == 0) {
        return false;
    }

    try {
        auto pidTime = NSystemWideName::GetProcessCtime(Pid);
        if (pidTime != StartTime) {
            if (log) {
                LOGGER_CHECKED_GENERIC_LOG(*log, TRTYLogPreprocessor, TLOG_RESOURCES, "RESOURCE[CTIME]")
                    << "Bad time: " << pidTime << " " << StartTime << " " << LastSystemError() << Endl;
            }
            return false;
        }
    } catch (const TSystemError& e) {
        if (log) {
            LOGGER_CHECKED_GENERIC_LOG(*log, TRTYLogPreprocessor, TLOG_RESOURCES, "RESOURCE[CTIME]")
                << "Exception caught: "
                << " " << StartTime << " " << e.Status() << Endl;
        }
        return false;
    }

    return true;
}

bool TProcessUID::operator==(const TProcessUID& other) const {
    return Pid == other.Pid && StartTime == other.StartTime;
}

TProcessUID TProcessUID::GetMyName() {
    TProcessId pid = GetPID();
    return TProcessUID(pid, NSystemWideName::GetProcessCtime(pid));
}

TString TProcessUID::GetMyUniqueSuffix() {
    TProcessId pid = GetPID();
    return ToString(NSystemWideName::GetProcessCtime(pid)) + "." + ToString(pid);
}

TSystemWideName::TSystemWideName(const TAddress& address, TProcessId pid, time_t ctime)
    : TProcessUID(pid, ctime)
    , Address(address)
{
}

TSystemWideName::TSystemWideName(const TAddress& address, const TProcessUID& base)
    : TProcessUID(base)
    , Address(address)
{
}

bool TSystemWideName::CheckProcess(TLog* log) const noexcept {
    if (!TProcessUID::CheckProcess(log)) {
        return false;
    }

    if (auto* local = std::get_if<1>(&Address)) {
        if (!TFsPath(*local).Exists()) {
            if (log) {
                LOGGER_CHECKED_GENERIC_LOG(*log, TRTYLogPreprocessor, TLOG_RESOURCES, "RESOURCE[GRPC]")
                    << "Local socket file removed: " << *local << Endl;
            }
            return false;
        }
    }

    using namespace grpc;

    health::v1::HealthCheckResponse response;
    health::v1::HealthCheckRequest request;
    request.set_service("");
    auto channel = CreateChannel(ToGrpcAddress(), InsecureChannelCredentials());
    auto stub = health::v1::Health::NewStub(channel);

    int attempt = 0;
    auto timeout = MSEC_SOCKET_TIMEOUT;
    do {
        ClientContext ctx;
        ctx.set_deadline(std::chrono::system_clock::now() + std::chrono::milliseconds(timeout));
        ctx.set_wait_for_ready(true);
        auto status = stub->Check(&ctx, request, &response);
        if (status.ok()) {
            return response.Getstatus() == grpc::health::v1::HealthCheckResponse::SERVING;
        }
        if (status.error_code() == grpc::StatusCode::CANCELLED) {
            timeout = 2 * timeout;
        } else if ((status.error_code() == grpc::StatusCode::UNAVAILABLE ||
                    status.error_code() == grpc::StatusCode::UNKNOWN || // grpc#15623
                    status.error_code() == grpc::StatusCode::DEADLINE_EXCEEDED) &&
                   attempt < 3) {
            // Latency sensitive, relying on process check to reduce wait time here.
            if (!TProcessUID::CheckProcess(log)) {
                return false;
            }
        } else {
            if (log) {
                LOGGER_CHECKED_GENERIC_LOG(*log, TRTYLogPreprocessor, TLOG_RESOURCES, "RESOURCE[GRPC]")
                    << "GRPC error: " << (int)status.error_code() << ", message: " << status.error_message() << Endl;
            }
            break;
        }
        ++attempt;
    } while (attempt < 6); // up to 32x increase.

    if (log) {
        LOGGER_CHECKED_GENERIC_LOG(*log, TRTYLogPreprocessor, TLOG_RESOURCES, "RESOURCE[GRPC]")
            << "GRPC attempts exhausted " << attempt << Endl;
    }

    return false;
}

bool TSystemWideName::operator==(const TSystemWideName& other) const {
    return TProcessUID::operator==(other) && Address == other.Address;
}

TSystemWideName TSystemWideName::GetMyName(const TSockAddrInet6& iaddr) {
    TAddress addr(std::make_pair(TIp4Or6(Ip6FromString(iaddr.GetIp().c_str())), iaddr.GetPort()));
    return TSystemWideName(addr, TProcessUID::GetMyName());
}

TSystemWideName TSystemWideName::GetMyName(const TSockAddrInet& iaddr) {
    TAddress addr(std::make_pair(TIp4Or6(iaddr.GetIp()), iaddr.GetPort()));
    return TSystemWideName(addr, TProcessUID::GetMyName());
}

TSystemWideName TSystemWideName::GetMyName(const TSockAddrLocal& laddr) {
    TSystemWideName::TAddress addr(laddr.ToString());
    return TSystemWideName(addr, TProcessUID::GetMyName());
}

TString TSystemWideName::ToGrpcAddress() const {
    TString addr;
    if (auto* neta = std::get_if<0>(&Address)) {
        if (auto* ip4 = std::get_if<TIp4>(&neta->first)) {
            addr = IpToString(*ip4) + ":";
        } else if (auto* ip6 = std::get_if<TIp6>(&neta->first)) {
            addr = "[" + Ip6ToString(*ip6) + "]:";
        } else {
            Y_UNREACHABLE();
        }
        addr += ToString(neta->second);
    } else if (auto* local = std::get_if<1>(&Address)) {
#if defined(_win_) || defined(_cygwin_)
        // Windows code mimics unix:// socket using localhost AF_INET.
        Y_ENSURE_EX(false, TWithBackTrace<yexception>());
#endif
        addr = ToString("unix:") + *local;
    } else {
        Y_UNREACHABLE();
    }
    return addr;
}

TString TSystemWideName::GetLocalSocket() const noexcept {
    if (auto* local = std::get_if<1>(&Address)) {
        return *local;
    }
    return "";
}

void TSystemWideName::SetPort(TIpPort port) {
    if (auto* neta = std::get_if<0>(&Address)) {
        neta->second = port;
    }
}

int TSystemWideName::MSEC_SOCKET_TIMEOUT = 500;

// TFileReadWriteLock<Value> helpers
namespace {
    class TUnlock {
    public:
        template <typename T>
        static inline void Destroy(T* file) noexcept {
            file->Flock(LOCK_UN);
        }
    };

    class TRewind {
    public:
        template <typename T>
        static inline void Destroy(T* file) noexcept {
            file->Seek(0, sSet);
        }
    };

    template <typename Value>
    using TFunc = std::function<Value(Value, std::function<void(Value)>)>;

    /// Basic IO
    /// @{
    template <typename Value>
    TMaybe<Value> ReadOther(TFile* file, ECheckLiveness check, TLog* log) {
        THolder<TFile, TRewind> resetter(file);
        auto value = FromString<Value>(TUnbufferedFileInput(*file).ReadAll());
        if (check == NoCheckAlive || value.CheckProcess(log)) {
            return MakeMaybe(value);
        }
        return TMaybe<Value>();
    }

    template <typename Value>
    void WriteMyself(TFile* file, Value value, ECheckLiveness check, TLog* log) {
        file->Resize(0);
        THolder<TFile, TRewind> resetter(file);

        Y_ENSURE_EX(check == NoCheckAlive || value.CheckProcess(log), TWithBackTrace<yexception>());

        TUnbufferedFileOutput(*file).Write(ToString(value));
    }

    template <typename Value>
    std::pair<Value, bool> ReadOtherOrWriteMyself(TFile* file, TFunc<Value> capture, ECheckLiveness check, TLog* log) {
        auto readVal = ReadOther<Value>(file, NoCheckAlive, log);
        Y_ENSURE_EX(!readVal.Empty(), TWithBackTrace<yexception>());

        if (readVal.GetRef().CheckProcess(log)) {
            return std::make_pair(readVal.GetRef(), false);
        }
        auto myIdentity = capture(readVal.GetRef(),
                                  [file, check, log](Value v) { WriteMyself(file, v, check, log); });
        return std::make_pair(std::move(myIdentity), true);
    };

    template <typename Value>
    Value ReadOtherAndWriteMyself(TFile* file, TFunc<Value> capture, ECheckLiveness check, TLog* log) {
        auto readVal = ReadOther<Value>(file, NoCheckAlive, log);
        Y_ENSURE_EX(!readVal.Empty(), TWithBackTrace<yexception>());
        auto myIdentity = capture(readVal.GetRef(),
                                  [file, check, log](Value v) { WriteMyself(file, v, check, log); });
        return myIdentity;
    };
    /// @}

    template <typename Value>
    TMaybe<Value> LockedRead(TFile* file, ELockWait blocking, TLog& log, const TString& fname, bool& outLockObtained) {
        using ReturnT = TMaybe<Value>;

        outLockObtained = false;

        // EINTR loop
        for (int attempt = 0; true; ++attempt) {
            if (attempt == 0 && blocking == Blocking) {
                LOGGER_CHECKED_GENERIC_LOG(log, TRTYLogPreprocessor, TLOG_RESOURCES, "RESOURCE[FLOCK]") << "File lock in blocking mode: " << fname << Endl;
            }
            try {
                auto mode = LOCK_SH | (blocking == NonBlocking ? LOCK_NB : 0);
                file->Flock(mode);
                THolder<TFile, TUnlock> unlocker(file);

                outLockObtained = true;

                if (file->GetLength() > Value::MAX_SIZE) {
                    LOGGER_CHECKED_GENERIC_LOG(log, TRTYLogPreprocessor, TLOG_ERR, "ERR[FLOCK]") << "Corrupted file while reading: " << fname << Endl;
                    return ReturnT();
                }

                auto res = ReadOther<Value>(file, CheckAlive, &log);
                LOGGER_CHECKED_GENERIC_LOG(log, TRTYLogPreprocessor, TLOG_RESOURCES, "RESOURCE[FLOCK]") << "Done reading file atomically: " << fname << Endl;
                return res;
            } catch (TFileError& f) {
                int lastErr = f.Status();
                if (lastErr == EINTR) {
                    continue;
                }
                // Did not see specific errno set for non-blocking read (__win__).
                if (lastErr != EWOULDBLOCK && lastErr != EAGAIN && lastErr != EACCES) {
                    throw;
                }
            }
            break;
        }
        return ReturnT();
    }

    // If lock successful then non-Empty result.
    template <typename Value>
    TMaybe<std::pair<Value, bool>> LockedReadAndWrite(TFile* file, TFunc<Value> capture, ELockWait blocking, TLog& log, const TString& fname, ECheckLiveness check) {
        // EINTR loop
        for (int attempt = 0; true; ++attempt) {
            if (attempt == 0 && blocking == Blocking) {
                LOGGER_CHECKED_GENERIC_LOG(log, TRTYLogPreprocessor, TLOG_RESOURCES, "RESOURCE[FLOCK]") << "File lock in blocking mode: " << fname << Endl;
            }
            try {
                auto mode = LOCK_EX | (blocking == NonBlocking ? LOCK_NB : 0);
                file->Flock(mode);
                THolder<TFile, TUnlock> unlocker(file);

                if (file->GetLength() > Value::MAX_SIZE) {
                    // Hard error. Replace with myself.
                    auto* plog = &log;
                    auto myIdentity = capture(Value(), [file, check, plog](Value v) { WriteMyself(file, v, check, plog); });
                    auto res = MakeMaybe(std::make_pair(std::move(myIdentity), true));
                    LOGGER_CHECKED_GENERIC_LOG(log, TRTYLogPreprocessor, TLOG_ERR, "ERR[FLOCK]") << "Overwritten corrupted file atomically: " << fname << Endl;
                    return res;
                }

                auto res = MakeMaybe(ReadOtherOrWriteMyself<Value>(file, capture, check, &log));
                LOGGER_CHECKED_GENERIC_LOG(log, TRTYLogPreprocessor, TLOG_RESOURCES, "RESOURCE[FLOCK]") << "Done read-or-write file atomically: " << fname << Endl;
                return res;
            } catch (TFileError& f) {
                int lastErr = f.Status();
                // Retry
                if (lastErr == EINTR) {
                    continue;
                }
                if (lastErr != EWOULDBLOCK && lastErr != EAGAIN && lastErr != EACCES) {
                    throw;
                }
            }
            break;
        }
        return TMaybe<std::pair<Value, bool>>();
    }

    // If lock successful then non-Empty result.
    template <typename Value>
    TMaybe<Value> LockedWrite(TFile* file, TFunc<Value> capture, ELockWait blocking, TLog& log, const TString& fname, ECheckLiveness check) {
        // EINTR loop
        for (int attempt = 0; true; ++attempt) {
            if (attempt == 0 && blocking == Blocking) {
                LOGGER_CHECKED_GENERIC_LOG(log, TRTYLogPreprocessor, TLOG_RESOURCES, "RESOURCE[FLOCK]") << "File lock in blocking mode: " << fname << Endl;
            }

            try {
                auto mode = LOCK_EX | (blocking == NonBlocking ? LOCK_NB : 0);
                file->Flock(mode);
                THolder<TFile, TUnlock> unlocker(file);

                if (file->GetLength() > Value::MAX_SIZE) {
                    // Hard error. Replace with myself.
                    auto* plog = &log;
                    auto myIdentity = capture(Value(), [file, check, plog](Value v) { WriteMyself(file, v, check, plog); });
                    LOGGER_CHECKED_GENERIC_LOG(log, TRTYLogPreprocessor, TLOG_ERR, "ERR[FLOCK]") << "Overwritten corrupted file atomically: " << fname << Endl;
                    return myIdentity;
                }

                auto res = MakeMaybe(ReadOtherAndWriteMyself<Value>(file, capture, check, &log));
                LOGGER_CHECKED_GENERIC_LOG(log, TRTYLogPreprocessor, TLOG_RESOURCES, "RESOURCE[FLOCK]") << "Done writing file atomically: " << fname << Endl;
                return res;
            } catch (TFileError& f) {
                int lastErr = f.Status();
                // Retry
                if (lastErr == EINTR) {
                    continue;
                }
                if (lastErr != EWOULDBLOCK && lastErr != EAGAIN && lastErr != EACCES) {
                    throw;
                }
            }
            break;
        }
        return TMaybe<Value>();
    }
}

template <typename Value>
std::pair<typename TFileReadWriteLock<Value>::TValue, bool>
TFileReadWriteLock<Value>::TryToReplace(TInstallOwner capture, ELockWait blocking, ECheckLiveness check) {
    Y_ASSERT(!_Base::ReadOnly);
    if (this->IsCaptured() || CachedValue_.CheckProcess(&this->Log)) {
        return std::make_pair(CachedValue_, this->IsCaptured());
    }

    LOGGER_CHECKED_GENERIC_LOG(this->Log, TRTYLogPreprocessor, TLOG_RESOURCES, "RESOURCE[FLOCK]") << "Start check'n'write file atomically" << Endl;
    auto start = TInstant::Now();
    Y_DEFER {
        LOGGER_CHECKED_GENERIC_LOG(this->Log, TRTYLogPreprocessor, TLOG_RESOURCES, "RESOURCES[FLOCK]")
            << "Synchronization time: "
            << TInstant::Now() - start << Endl;
    };
    EOpenMode perm = AWUser | ARUser;
    EOpenMode mode = OpenAlways | RdWr;

    TFile file(_Base::Name, mode | perm);
    TWriteGuard threadlock(ThreadingLock_);

    for (int i = 0; i < ATTEMPTS; ++i) {
        {
            bool lockObtained = false;
            auto maybe = LockedRead<TValue>(&file, NonBlocking, _Base::Log, _Base::Name, lockObtained);
            if (!maybe.Empty()) {
                CachedValue_ = maybe.GetRef();
                this->SetCaptured(false);
                return std::make_pair(CachedValue_, this->IsCaptured());
            }
            if (lockObtained) {
                usleep(TIMEOUT);
            }
        }
        {
            auto nonBlockingAttempt = i + 1 < ATTEMPTS || blocking == NonBlocking ? NonBlocking : Blocking;
            auto maybe = LockedReadAndWrite<TValue>(&file, capture, nonBlockingAttempt, this->Log, _Base::Name, check);
            if (!maybe.Empty()) {
                CachedValue_ = maybe.GetRef().first;
                this->SetCaptured(maybe.GetRef().second);
                return std::make_pair(CachedValue_, this->IsCaptured());
            }
            usleep(TIMEOUT);
        }
    }
    ythrow TLockFailed();
}

template <typename Value>
TMaybe<typename TFileReadWriteLock<Value>::TValue>
TFileReadWriteLock<Value>::Read(ELockWait blocking) noexcept {
    if (this->IsCaptured() || CachedValue_.CheckProcess(&this->Log)) {
        return CachedValue_;
    }
    LOGGER_CHECKED_GENERIC_LOG(this->Log, TRTYLogPreprocessor, TLOG_RESOURCES, "RESOURCE[FLOCK]") << "Start reading file atomically" << Endl;
    auto start = TInstant::Now();
    Y_DEFER {
        LOGGER_CHECKED_GENERIC_LOG(this->Log, TRTYLogPreprocessor, TLOG_RESOURCES, "RESOURCES[FLOCK]")
            << "Synchronization time: "
            << TInstant::Now() - start << Endl;
    };

    EOpenMode perm = AWUser | ARUser;
    EOpenMode mode = OpenAlways | RdOnly;

    try {
        TFile file(_Base::Name, mode | perm);
        TReadGuard threadlock(ThreadingLock_);

        for (int i = 0; i < ATTEMPTS; ++i) {
            auto nonBlockingAttempt = i + 1 < ATTEMPTS || blocking == NonBlocking ? NonBlocking : Blocking;
            bool lockObtained = false;
            auto maybe = LockedRead<TValue>(&file, nonBlockingAttempt, this->Log, _Base::Name, lockObtained);
            if (!maybe.Empty()) {
                CachedValue_ = maybe.GetRef();
                return MakeMaybe(CachedValue_);
            }
            if (lockObtained) {
                return TMaybe<TValue>();
            }
            usleep(TIMEOUT);
        }
    } catch (const TSystemError& e) {
    }
    return TMaybe<TValue>();
}

template <typename Value>
void TFileReadWriteLock<Value>::Write(TInstallOwner capture, ELockWait blocking, ECheckLiveness check) {
    Y_ASSERT(!_Base::ReadOnly);
    LOGGER_CHECKED_GENERIC_LOG(this->Log, TRTYLogPreprocessor, TLOG_RESOURCES, "RESOURCE[FLOCK]") << "Start read'n'write file atomically" << Endl;
    auto start = TInstant::Now();
    Y_DEFER {
        LOGGER_CHECKED_GENERIC_LOG(this->Log, TRTYLogPreprocessor, TLOG_RESOURCES, "RESOURCES[FLOCK]")
            << "Synchronization time: "
            << TInstant::Now() - start << Endl;
    };

    EOpenMode perm = AWUser | ARUser;
    EOpenMode mode = OpenAlways | RdWr;

    TFile file(_Base::Name, mode | perm);
    TWriteGuard threadlock(ThreadingLock_);

    for (int i = 0; i < ATTEMPTS; ++i) {
        auto nonBlockingAttempt = i + 1 < ATTEMPTS || blocking == NonBlocking ? NonBlocking : Blocking;
        auto maybe = LockedWrite<TValue>(&file, capture, nonBlockingAttempt, this->Log, _Base::Name, check);
        if (!maybe.Empty()) {
            CachedValue_ = maybe.GetRef();
            this->SetCaptured(true);
            return;
        }
        usleep(TIMEOUT);
    }
    ythrow TLockFailed();
}

template <>
i64 TFileReadWriteLock<TSystemWideName>::ATTEMPTS = 5;

template <>
i64 TFileReadWriteLock<TSystemWideName>::TIMEOUT = 3000;

TPFileSingleton* GetClientPSingleton(TStringBuf lockFile, TStringBuf logName, bool fresh) {
    static TLog clientLog(MakeHolder<TNullLogBackend>());
    static TRWMutex logMutex;
    TWriteGuard logLock(logMutex);
    if (logName.Empty()) {
        clientLog.ResetBackend(MakeHolder<TNullLogBackend>());
    } else {
        clientLog.ResetBackend(MakeHolder<TLogBackendWithFallback>(
            new TRotatingFileLogBackend(ToString(logName), 4000, 1),
            new TNullLogBackend()));
    }
    return fresh ? new TPFileSingleton(lockFile, clientLog) : Singleton<TPFileSingleton>(lockFile, clientLog);
}
