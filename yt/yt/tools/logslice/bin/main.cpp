#include <yt/yt/tools/logslice/log_slice_engine.h>
#include <yt/yt/tools/logslice/time_parser.h>

#include <library/cpp/getopt/last_getopt.h>

#include <util/stream/output.h>
#include <util/stream/str.h>
#include <util/system/file.h>

#include <stdexcept>

using namespace NYT;
using namespace NYT::NLogSlice;

int main(int argc, char** argv)
{
    try {
        TString startString;
        TString endString;
        TString codecString;
        TString grepLine;
        i64 extendSeconds = 0;
        bool infoMode = false;

        auto opts = NLastGetopt::TOpts::Default();
        opts.AddLongOption('t', "start", "start of the time window (default: beginning of file)")
            .RequiredArgument("TIME")
            .StoreResult(&startString);
        opts.AddLongOption('e', "end", "end of the time window (default: end of file)")
            .RequiredArgument("TIME")
            .StoreResult(&endString);
        opts.AddLongOption("info", "print first and last line timestamps; incompatible with --start/--end")
            .NoArgument()
            .StoreValue(&infoMode, true);
        opts.AddLongOption("extend", "widen the window by this many seconds on both sides")
            .RequiredArgument("SECONDS")
            .StoreResult(&extendSeconds)
            .DefaultValue(0);
        opts.AddLongOption("codec", "compression codec: auto, zstd, gzip or plain")
            .RequiredArgument("CODEC")
            .StoreResult(&codecString)
            .DefaultValue("auto");
        opts.AddLongOption('g', "grep", "grep arguments as a single string, split into tokens (quotes group multi-word patterns)")
            .RequiredArgument("ARGS")
            .StoreResult(&grepLine);
        // The first free argument is the log file; any further free arguments
        // (typically passed after "--") are forwarded to grep verbatim.
        opts.SetFreeArgsMin(1);
        opts.SetFreeArgTitle(0, "log_file", "log file (.zst, .gz or plain .log) [-- GREP_ARGS...]");
        opts.AddHelpOption();

        NLastGetopt::TOptsParseResult parsed(&opts, argc, argv);
        const auto& freeArgs = parsed.GetFreeArgs();
        TString logFileName = TString(freeArgs[0]);
        std::vector<TString> grepArgs(freeArgs.begin() + 1, freeArgs.end());
        if (parsed.Has('g')) {
            auto extra = SplitCommandLine(grepLine);
            grepArgs.insert(grepArgs.end(), extra.begin(), extra.end());
        }

        bool hasStart = parsed.Has('t');
        bool hasEnd = parsed.Has('e');
        if (infoMode && (hasStart || hasEnd)) {
            throw std::runtime_error("--info cannot be combined with --start or --end");
        }

        auto codec = ParseCompressionCodec(codecString);
        auto engine = CreateLogSliceEngine(codec, logFileName);
        TFile file(logFileName, OpenExisting | RdOnly);

        if (infoMode) {
            auto range = engine->GetTimeRange(file);
            if (range) {
                Cout << "first: " << FormatLogTime(range->first) << "\n";
                Cout << "last:  " << FormatLogTime(range->second) << "\n";
            } else {
                Cout << "no timestamped lines\n";
            }
            Cout.Flush();
            return 0;
        }

        auto extend = TDuration::Seconds(extendSeconds);

        auto from = TInstant::Zero();
        if (hasStart) {
            auto start = ParseQueryTime(startString);
            from = start.GetValue() > extend.GetValue() ? start - extend : TInstant::Zero();
        }

        auto to = TInstant::Max();
        if (hasEnd) {
            to = ParseQueryTime(endString) + extend;
        }

        if (grepArgs.empty()) {
            engine->Slice(file, from, to, Cout);
        } else {
            // Stream the slice straight through grep instead of buffering it: a
            // multi-gigabyte log would otherwise exhaust memory and be killed
            // with SIGKILL (exit code 137).
            FilterWithGrep(
                grepArgs,
                [&] (IOutputStream& sink) {
                    engine->Slice(file, from, to, sink);
                },
                Cout);
        }
        Cout.Flush();
    } catch (const std::exception& ex) {
        Cerr << "ERROR: " << ex.what() << Endl;
        return 1;
    }

    return 0;
}
