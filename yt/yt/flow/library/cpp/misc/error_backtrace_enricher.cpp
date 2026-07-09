#include "error_backtrace_enricher.h"

#include <yt/yt/core/misc/configurable_singleton_def.h>
#include <yt/yt/core/ytree/yson_struct.h>

#include <library/cpp/yt/backtrace/backtrace.h>

#include <util/system/backtrace.h>

namespace NYT::NFlow {

using namespace NConcurrency;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

bool CheckLevel(const TError& error, EBacktraceEnricherLevel level)
{
    if (level == EBacktraceEnricherLevel::Disabled) {
        return false;
    }
    if (level == EBacktraceEnricherLevel::EnabledForAll) {
        return true;
    }

    bool isGeneric = (error.GetCode() == NYT::EErrorCode::Generic);
    bool isTrivialCanceled = (error.GetCode() == NYT::EErrorCode::Canceled && error.GetMessage() == "Canceled");
    if (level == EBacktraceEnricherLevel::EnabledForTrivialErrors && !(isGeneric || isTrivialCanceled)) {
        return false;
    }

    if (level == EBacktraceEnricherLevel::EnabledForNotNativeErrors && !error.Attributes().Contains("exception_type")) {
        return false;
    }

    for (const auto& innerError : error.InnerErrors()) {
        if (!CheckLevel(innerError, level)) {
            return false;
        }
    }

    return true;
}

const TBacktraceEnricherStatePtr& TBacktraceEnricherState::Get()
{
    static TBacktraceEnricherStatePtr state = New<TBacktraceEnricherState>();
    return state;
}

void EnrichErrorWithBacktrace(TError* error, const std::exception&)
{
    static const std::string backtraceAttributeKey = "origin_backtrace";

    const auto& state = TBacktraceEnricherState::Get();
    if (!CheckLevel(*error, state->Level.load(std::memory_order::relaxed))) {
        return;
    }

    auto utilBacktrace = ::TBackTrace::FromCurrentException();
    auto backtraceBytes = as_bytes(::TBackTraceView(utilBacktrace));
    if (!backtraceBytes.empty()) {
        auto backtraceBytesString = std::string(backtraceBytes.data(), backtraceBytes.size());

        std::string symbolizedBacktrace;
        if (!state->CachedBacktraces.Get(backtraceBytesString, symbolizedBacktrace)) {
            symbolizedBacktrace = NBacktrace::SymbolizeBacktrace(NBacktrace::TBacktrace(utilBacktrace.data(), utilBacktrace.size()));
            state->CachedBacktraces.InsertIfAbsent(backtraceBytesString, symbolizedBacktrace);
        }

        if (auto oldSymbolizedBacktrace = error->Attributes().Find<std::string>(backtraceAttributeKey)) {
            symbolizedBacktrace = Format("%v\n"
                "-----------------\n"
                "(ThreadName: %v, FiberId: %llx, NewDatetime: %v)\n"
                "%v",
                *oldSymbolizedBacktrace,
                GetCurrentThreadName().ToStringBuf(),
                GetCurrentFiberId(),
                TInstant::Now(),
                symbolizedBacktrace);
        }

        *error <<= TErrorAttribute(backtraceAttributeKey, symbolizedBacktrace);
    }
}

void InitBacktraceEnricher()
{
    [[maybe_unused]] static bool initialized = std::invoke([] {
        TError::RegisterFromExceptionEnricher(&EnrichErrorWithBacktrace);
        return true;
    });
}

////////////////////////////////////////////////////////////////////////////////

void TBacktraceEnricherSpec::Register(TRegistrar registrar)
{
    registrar.Parameter("level", &TThis::Level)
        .Default(EBacktraceEnricherLevel::Disabled);
}

void TBacktraceEnricherDynamicSpec::Register(TRegistrar registrar)
{
    registrar.Parameter("level", &TThis::Level)
        .Default();
}

void ConfigureSingleton(const TBacktraceEnricherSpecPtr& spec)
{
    InitBacktraceEnricher();
    TBacktraceEnricherState::Get()->Level.store(spec->Level, std::memory_order::relaxed);
}

void ReconfigureSingleton(
    const TBacktraceEnricherSpecPtr& spec,
    const TBacktraceEnricherDynamicSpecPtr& dynamicSpec)
{
    InitBacktraceEnricher();
    TBacktraceEnricherState::Get()->Level.store(dynamicSpec->Level.value_or(spec->Level), std::memory_order::relaxed);
}

void SetupSingletonConfigParameter(TYsonStructParameter<TBacktraceEnricherSpecPtr>& parameter)
{
    parameter.DefaultNew();
}

void SetupSingletonConfigParameter(TYsonStructParameter<TBacktraceEnricherDynamicSpecPtr>& parameter)
{
    parameter.DefaultNew();
}

YT_DEFINE_RECONFIGURABLE_SINGLETON(
    "error_backtrace_enricher",
    TBacktraceEnricherSpec,
    TBacktraceEnricherDynamicSpec);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
