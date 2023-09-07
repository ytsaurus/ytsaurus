#include "fallback_logger.h"
#include "library/cpp/logger/record.h"

#include <util/system/rwlock.h>
#include <library/cpp/deprecated/atomic/atomic.h>

#include <array>

class TLogBackendWithFallback::TImpl {
public:
    enum Elogger : int {
        MainLogger,
        FallbackLogger,
        NoLogger
    };

    inline TImpl()
        : LoggerIdx_(static_cast<TAtomic>(NoLogger))
    {
    }

    inline void WriteData(const TLogRecord& rec) {
        // Upper bound on number of iterations.
        for (size_t i = 0, e = Loggers_.size(); i != e; ++i) {
            auto kind = static_cast<size_t>(AtomicGet(LoggerIdx_));
            try {
                TReadGuard rguard(Lock_);
                kind = static_cast<size_t>(AtomicGet(LoggerIdx_));
                if (kind < Loggers_.size() && Loggers_[kind]) {
                    Loggers_[kind]->WriteData(rec);
                }
                return;
            } catch (...) {
                TWriteGuard wguard(Lock_);
                if (kind == static_cast<size_t>(AtomicGet(LoggerIdx_))) {
                    FallbackToNextLogger();
                }
                if (static_cast<size_t>(AtomicGet(LoggerIdx_)) == Loggers_.size()) {
                    throw;
                }
            }
        }
    }

    inline void ReopenLog() {
        TWriteGuard wguard(Lock_);

        for (size_t i = 0, e = Loggers_.size(); i != e; ++i) {
            try {
                auto kind = static_cast<size_t>(AtomicGet(LoggerIdx_));
                if (kind < Loggers_.size() && Loggers_[kind]) {
                    Loggers_[kind]->ReopenLog();
                }
                return;
            } catch (...) {
                FallbackToNextLogger();
                if (static_cast<size_t>(AtomicGet(LoggerIdx_)) == Loggers_.size()) {
                    throw;
                }
            }
        }
    }

    // Should be called from single-threaded environment.
    // Afterward LoggerIdx_ is non-decreasing.
    void SetLogger(TAtomicSharedPtr<TLogBackend> logger, size_t idx) noexcept {
        TWriteGuard guard(Lock_);
        Loggers_[idx] = logger;
        AtomicSet(LoggerIdx_,
                  static_cast<TAtomic>(Min(static_cast<size_t>(idx),
                                           static_cast<size_t>(AtomicGet(LoggerIdx_)))));
    }

private:
    /// Lock is externally held
    void FallbackToNextLogger() {
        auto kind = static_cast<size_t>(AtomicGet(LoggerIdx_));
        if (kind < Loggers_.size()) {
            Loggers_[kind] = nullptr;
            AtomicSet(LoggerIdx_, static_cast<TAtomic>(kind + 1));
        }
    }

    TRWMutex Lock_;
    std::array<TAtomicSharedPtr<TLogBackend>, NoLogger> Loggers_;
    TAtomic LoggerIdx_;
};

TLogBackendWithFallback::TLogBackendWithFallback(TAtomicSharedPtr<TLogBackend> mainLogger, TAtomicSharedPtr<TLogBackend> fallbackLogger)
    : Impl_(new TImpl())
{
    Impl_->SetLogger(mainLogger, static_cast<size_t>(TImpl::MainLogger));
    Impl_->SetLogger(fallbackLogger, static_cast<size_t>(TImpl::FallbackLogger));
}

TLogBackendWithFallback::~TLogBackendWithFallback() {
}

void TLogBackendWithFallback::WriteData(const TLogRecord& rec) {
    Impl_->WriteData(rec);
}

void TLogBackendWithFallback::ReopenLog() {
    TAtomicSharedPtr<TImpl> copy = Impl_;
    if (copy) {
        copy->ReopenLog();
    }
}
