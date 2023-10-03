#pragma once

#include "library/cpp/logger/backend.h"

#include <util/generic/fwd.h>
#include <util/generic/ptr.h>

/// Best effort in the case there is some exception (supports main and fallback logger).
/// No guarantees: if some logger is stale then no data is written.
/// Main/Fallback loggers should be set in single-threaded environment.
class TLogBackendWithFallback: public TLogBackend {
public:
    TLogBackendWithFallback(TAtomicSharedPtr<TLogBackend> mainLogger, TAtomicSharedPtr<TLogBackend> fallbackLogger);
    ~TLogBackendWithFallback() override;

    void WriteData(const TLogRecord& rec) override;
    void ReopenLog() override;

private:
    class TImpl;
    TAtomicSharedPtr<TImpl> Impl_;
};
