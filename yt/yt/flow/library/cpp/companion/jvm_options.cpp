#include "jvm_options.h"

#include <iterator>

#include <library/cpp/yt/string/format.h>

#include <yt/yt/core/logging/log.h>
#include <yt/yt/core/misc/fs.h>

#include <util/string/split.h>
#include <util/system/env.h>

namespace NYT::NFlow::NCompanion {

////////////////////////////////////////////////////////////////////////////////

YT_DEFINE_GLOBAL(const NLogging::TLogger, Logger, "JvmOptions");

namespace {

constexpr TStringBuf DefaultCompanionLogDir = "./logs";

std::vector<std::string> ParseJvmOptions(const std::string& optionsString)
{
    std::vector<std::string> result;
    StringSplitter(optionsString).Split(' ').SkipEmpty().Collect(&result);
    return result;
}

std::vector<std::string> GetDefaultJfrOptions(const std::string& logDir)
{
    return {
        "-XX:+UnlockDiagnosticVMOptions",
        "-XX:+DebugNonSafepoints",
        Format("-XX:StartFlightRecording=disk=true,settings=profile,maxage=24h,maxsize=1000m,dumponexit=true,filename=%v/dump.jfr", logDir),
        Format("-XX:FlightRecorderOptions=repository=%v/jfr,maxchunksize=30M", logDir),
    };
}

std::vector<std::string> GetDefaultGcLogOptions(const std::string& logDir)
{
    return {
        Format("-Xlog:gc:file=%v/gc.log:time,uptime:filecount=10,filesize=50m", logDir),
    };
}

std::vector<std::string> GetDefaultCrashAndOomOptions(const std::string& logDir)
{
    return {
        "-XX:+ExitOnOutOfMemoryError",
        // Coredump file follows host rules.
        "-XX:+CreateCoredumpOnCrash",
        // JVM diagnostic info dump with jvm coredump.
        Format("-XX:ErrorFile=%v/hs_err_%%p.log", logDir),

    };
}

void WarnIfHeapDumpOptionsNotSet(const std::vector<std::string>& jvmOptions)
{
    bool hasHeapDumpOnOom = false;
    bool hasHeapDumpPath = false;
    for (const auto& opt : jvmOptions) {
        if (opt.find("-XX:+HeapDumpOnOutOfMemoryError") != std::string::npos) {
            hasHeapDumpOnOom = true;
        }
        if (opt.find("-XX:HeapDumpPath") != std::string::npos) {
            hasHeapDumpPath = true;
        }
    }

    if (!hasHeapDumpOnOom || !hasHeapDumpPath) {
        YT_LOG_WARNING("Heap dump options are not configured. "
            "It is recommended to set -XX:+HeapDumpOnOutOfMemoryError and -XX:HeapDumpPath=<path> "
            "via YT_FLOW_COMPANION_JVM_EXTRA_OPTS environment variable "
            "to enable heap dump generation on OutOfMemoryError "
            "(HasHeapDumpOnOutOfMemoryError: %v, HasHeapDumpPath: %v)",
            hasHeapDumpOnOom,
            hasHeapDumpPath);
    }
}

std::vector<std::string> BuildJvmOptions()
{
    auto logDir = GetEnv("YT_FLOW_COMPANION_LOG_DIR");
    if (logDir.empty()) {
        logDir = DefaultCompanionLogDir;
        NFS::MakeDirRecursive(logDir);
    }
    logDir = NFS::GetRealPath(logDir);
    YT_LOG_INFO("Directory for JVM diagnostic info (LogDir: %v)", logDir);

    std::vector<std::string> result;

    if (GetEnv("YT_FLOW_COMPANION_JFR_DISABLED") != "1") {
        if (auto jfrOpts = GetEnv("YT_FLOW_COMPANION_JFR_OPTS"); !jfrOpts.empty()) {
            auto parsed = ParseJvmOptions(jfrOpts);
            result.insert(result.end(), std::make_move_iterator(parsed.begin()), std::make_move_iterator(parsed.end()));
        } else {
            auto defaults = GetDefaultJfrOptions(logDir);
            result.insert(result.end(), std::make_move_iterator(defaults.begin()), std::make_move_iterator(defaults.end()));
        }
    }

    if (GetEnv("YT_FLOW_COMPANION_GC_LOG_DISABLED") != "1") {
        if (auto gcOpts = GetEnv("YT_FLOW_COMPANION_GC_LOG_OPTS"); !gcOpts.empty()) {
            auto parsed = ParseJvmOptions(gcOpts);
            result.insert(result.end(), std::make_move_iterator(parsed.begin()), std::make_move_iterator(parsed.end()));
        } else {
            auto defaults = GetDefaultGcLogOptions(logDir);
            result.insert(result.end(), std::make_move_iterator(defaults.begin()), std::make_move_iterator(defaults.end()));
        }
    }

    auto crashOom = GetDefaultCrashAndOomOptions(logDir);
    result.insert(result.end(), std::make_move_iterator(crashOom.begin()), std::make_move_iterator(crashOom.end()));

    return result;
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

std::vector<std::string> ResolveJvmOptions()
{
    auto base = BuildJvmOptions();

    if (auto extraOpts = GetEnv("YT_FLOW_COMPANION_JVM_EXTRA_OPTS"); !extraOpts.empty()) {
        auto extra = ParseJvmOptions(extraOpts);
        base.insert(base.end(), std::make_move_iterator(extra.begin()), std::make_move_iterator(extra.end()));
    }

    WarnIfHeapDumpOptionsNotSet(base);

    return base;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NCompanion
