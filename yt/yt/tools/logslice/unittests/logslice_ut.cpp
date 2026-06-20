#include <yt/yt/tools/logslice/log_slice_engine.h>
#include <yt/yt/tools/logslice/time_parser.h>

#include <yt/yt/core/logging/config.h>
#include <yt/yt/core/logging/file_log_writer.h>
#include <yt/yt/core/logging/formatter.h>
#include <yt/yt/core/logging/log_writer.h>
#include <yt/yt/core/logging/log_writer_factory.h>
#include <yt/yt/core/logging/system_log_event_provider.h>

#include <yt/yt/core/concurrency/scheduler_api.h>

#include <yt/yt/core/test_framework/framework.h>

#include <library/cpp/yt/cpu_clock/clock.h>
#include <library/cpp/yt/logging/logger.h>

#include <util/stream/str.h>
#include <util/string/split.h>
#include <util/system/file.h>
#include <util/system/tempfile.h>

namespace NYT::NLogSlice {
namespace {

using namespace NYT::NLogging;
using namespace NYT::NConcurrency;

////////////////////////////////////////////////////////////////////////////////

const TLoggingCategory* GetTestCategory()
{
    static NLogging::TLogger Logger("Test");
    return Logger.GetCategory();
}

class TLogSliceTest
    : public ::testing::Test
    , public ILogWriterHost
{
protected:
    // ILogWriterHost.
    IInvokerPtr GetCompressionInvoker() override
    {
        return GetCurrentInvoker();
    }

    TLogEvent MakeEvent(TInstant instant, TStringBuf message)
    {
        TLogEvent event;
        event.Family = ELogFamily::PlainText;
        event.Category = GetTestCategory();
        event.Level = ELogLevel::Info;
        event.MessageRef = TSharedRef::FromString(TString(message));
        event.MessageKind = ELogMessageKind::Unstructured;
        event.Instant = InstantToCpuInstant(instant);
        return event;
    }

    //! Writes the given (instant, message) events to a log file using the standard
    //! YT file log writer with the given compression method (nullopt = plain text).
    //! Each event is flushed separately so that, for the compressed formats, it
    //! lands in its own block -- exercising the block-level binary search.
    void WriteLog(
        const TString& fileName,
        std::optional<ECompressionMethod> compressionMethod,
        const std::vector<std::pair<TInstant, TString>>& events)
    {
        auto config = New<TFileLogWriterConfig>();
        config->FileName = fileName;
        config->EnableCompression = compressionMethod.has_value();
        if (compressionMethod) {
            config->CompressionMethod = *compressionMethod;
        }
        // Keep the file deterministic: no "Logging started" banner.
        config->EnableSystemMessages = false;

        auto writer = CreateFileLogWriter(
            std::make_unique<TPlainTextLogFormatter>(),
            CreateDefaultSystemLogEventProvider(config),
            "test_writer",
            config,
            this);

        for (const auto& [instant, message] : events) {
            auto event = MakeEvent(instant, message);
            writer->Write(event);
            writer->Flush();
        }
        writer->Flush();
    }

    //! Slices [fileName] using an auto-detected engine (so the file's extension
    //! also drives which engine is selected).
    std::vector<TString> Slice(const TString& fileName, TInstant from, TInstant to)
    {
        auto engine = CreateLogSliceEngine(ECompressionCodec::Auto, fileName);

        TFile file(fileName, OpenExisting | RdOnly);
        TStringStream output;
        engine->Slice(file, from, to, output);

        std::vector<TString> lines;
        for (TStringBuf line : StringSplitter(output.Str()).Split('\n').SkipEmpty()) {
            lines.emplace_back(line);
        }
        return lines;
    }

    //! Extracts the message field (4th tab-separated column) of a formatted log line.
    static TString MessageOf(TStringBuf line)
    {
        TStringBuf rest = line;
        rest.NextTok('\t'); // timestamp
        rest.NextTok('\t'); // level
        rest.NextTok('\t'); // category
        return TString(rest.NextTok('\t'));
    }

    std::vector<TString> MessagesOf(const std::vector<TString>& lines)
    {
        std::vector<TString> messages;
        for (const auto& line : lines) {
            messages.push_back(MessageOf(line));
        }
        return messages;
    }

    //! Runs the same set of range queries against a log file holding events
    //! msg-0..msg-9 spaced one second apart starting at [base]. Boundaries are
    //! placed half a second off the event instants so the test is insensitive to
    //! the sub-microsecond rounding of the forged cpu-instant round-trip.
    void CheckSliceRanges(const TString& fileName, TInstant base)
    {
        auto half = TDuration::MilliSeconds(500);

        // A middle sub-range.
        {
            auto messages = MessagesOf(Slice(fileName, base + TDuration::Seconds(2) - half, base + TDuration::Seconds(5) + half));
            EXPECT_EQ((std::vector<TString>{"msg-2", "msg-3", "msg-4", "msg-5"}), messages);
        }

        // The full range returns everything in order.
        {
            auto messages = MessagesOf(Slice(fileName, base - half, base + TDuration::Seconds(9) + half));
            EXPECT_EQ(10u, messages.size());
            EXPECT_EQ("msg-0", messages.front());
            EXPECT_EQ("msg-9", messages.back());
        }

        // A narrow window around a single event selects exactly that line.
        {
            auto messages = MessagesOf(Slice(fileName, base + TDuration::Seconds(7) - half, base + TDuration::Seconds(7) + half));
            EXPECT_EQ((std::vector<TString>{"msg-7"}), messages);
        }

        // A range entirely before the log is empty.
        {
            auto messages = MessagesOf(Slice(fileName, base - TDuration::Seconds(100), base - TDuration::Seconds(1)));
            EXPECT_TRUE(messages.empty());
        }

        // A range entirely after the log is empty.
        {
            auto messages = MessagesOf(Slice(fileName, base + TDuration::Seconds(100), base + TDuration::Seconds(200)));
            EXPECT_TRUE(messages.empty());
        }
    }

    //! Reports the (first, last) timestamps of a log file via an auto-detected engine.
    std::optional<std::pair<TInstant, TInstant>> TimeRange(const TString& fileName)
    {
        auto engine = CreateLogSliceEngine(ECompressionCodec::Auto, fileName);
        TFile file(fileName, OpenExisting | RdOnly);
        return engine->GetTimeRange(file);
    }

    std::vector<std::pair<TInstant, TString>> MakeEvents(TInstant base)
    {
        std::vector<std::pair<TInstant, TString>> events;
        for (int i = 0; i < 10; ++i) {
            events.emplace_back(base + TDuration::Seconds(i), Format("msg-%v", i));
        }
        return events;
    }
};

////////////////////////////////////////////////////////////////////////////////

TEST_F(TLogSliceTest, ParseLogLineTime)
{
    auto local = ParseLogLineTime("2020-01-02 03:04:05,123456\tI\tTest\tmessage");
    ASSERT_TRUE(local.has_value());

    // Round-trips through the local-time formatter the writer uses.
    auto query = ParseQueryTime("2020-01-02 03:04:05");
    EXPECT_EQ(query + TDuration::MicroSeconds(123456), *local);

    // Lines without a leading timestamp are rejected.
    EXPECT_FALSE(ParseLogLineTime("not a log line").has_value());
    EXPECT_FALSE(ParseLogLineTime("").has_value());

    // FormatLogTime is the inverse of ParseLogLineTime: it reproduces the exact
    // 26-char log timestamp, which parses back to the same instant.
    EXPECT_EQ("2020-01-02 03:04:05,123456", FormatLogTime(*local));
    EXPECT_EQ(local, ParseLogLineTime(FormatLogTime(*local)));
}

TEST_F(TLogSliceTest, ParseQueryTimeFormats)
{
    // Full local timestamp and web-interface format denote the same instant.
    EXPECT_EQ(
        ParseQueryTime("2018-11-16 13:56:14"),
        ParseQueryTime("16 Nov 2018 13:56:14"));

    // The ISO UTC format is interpreted in UTC.
    auto iso = ParseQueryTime("2019-09-19T11:46:04.848360Z");
    EXPECT_EQ(TInstant::Seconds(1568893564) + TDuration::MicroSeconds(848360), iso);

    // "now" is close to the current instant.
    auto now = ParseQueryTime("now");
    EXPECT_LT(TInstant::Now() - now, TDuration::Seconds(5));

    // Unparseable input throws.
    EXPECT_THROW(ParseQueryTime("garbage"), std::exception);
}

TEST_F(TLogSliceTest, ParseQueryTimeSubsecond)
{
    auto whole = ParseQueryTime("2026-06-18 06:00:10");

    // Full 6-digit microsecond selector.
    EXPECT_EQ(whole + TDuration::MicroSeconds(246995), ParseQueryTime("2026-06-18 06:00:10,246995"));

    // 1 to 6 digits are right-padded to microseconds.
    EXPECT_EQ(whole + TDuration::MicroSeconds(500000), ParseQueryTime("2026-06-18 06:00:10,5"));
    EXPECT_EQ(whole + TDuration::MicroSeconds(246000), ParseQueryTime("2026-06-18 06:00:10,246"));
    EXPECT_EQ(whole + TDuration::MicroSeconds(7), ParseQueryTime("2026-06-18 06:00:10,000007"));

    // A dot delimits the subsecond part just like a comma.
    EXPECT_EQ(whole + TDuration::MicroSeconds(246995), ParseQueryTime("2026-06-18 06:00:10.246995"));
    EXPECT_EQ(whole + TDuration::MicroSeconds(7), ParseQueryTime("2026-06-18 06:00:10.000007"));
    EXPECT_EQ(ParseQueryTime("2026-06-19 09:55:55") + TDuration::MicroSeconds(10000),
        ParseQueryTime("2026-06-19 09:55:55.01"));

    // Dotted fractions follow the same validation as comma ones.
    EXPECT_THROW(ParseQueryTime("2026-06-18 06:00:10."), std::exception);
    EXPECT_THROW(ParseQueryTime("2026-06-18 06:00:10.1234567"), std::exception);

    // Empty, too long, or non-digit fractions are rejected.
    EXPECT_THROW(ParseQueryTime("2026-06-18 06:00:10,"), std::exception);
    EXPECT_THROW(ParseQueryTime("2026-06-18 06:00:10,1234567"), std::exception);
    EXPECT_THROW(ParseQueryTime("2026-06-18 06:00:10,12x"), std::exception);
}

TEST_F(TLogSliceTest, CodecDetectionAndParsing)
{
    EXPECT_EQ(ECompressionCodec::Zstd, DetectCompressionCodec("node.debug.log.zst"));
    EXPECT_EQ(ECompressionCodec::Gzip, DetectCompressionCodec("node.debug.log.gz"));
    EXPECT_EQ(ECompressionCodec::PlainText, DetectCompressionCodec("node.debug.log"));
    EXPECT_EQ(ECompressionCodec::PlainText, DetectCompressionCodec("whatever"));

    EXPECT_EQ(ECompressionCodec::Auto, ParseCompressionCodec("auto"));
    EXPECT_EQ(ECompressionCodec::Zstd, ParseCompressionCodec("zstd"));
    EXPECT_EQ(ECompressionCodec::Gzip, ParseCompressionCodec("gzip"));
    EXPECT_EQ(ECompressionCodec::PlainText, ParseCompressionCodec("plain"));
    EXPECT_THROW(ParseCompressionCodec("lzma"), std::exception);
}

TEST_F(TLogSliceTest, SliceZstd)
{
    TTempFile logFile(GenerateRandomFileName("log") + ".zst");
    auto base = ParseQueryTime("2021-06-07T08:09:10.000000Z");
    WriteLog(logFile.Name(), ECompressionMethod::Zstd, MakeEvents(base));
    CheckSliceRanges(logFile.Name(), base);
}

TEST_F(TLogSliceTest, SliceGzip)
{
    TTempFile logFile(GenerateRandomFileName("log") + ".gz");
    auto base = ParseQueryTime("2021-06-07T08:09:10.000000Z");
    WriteLog(logFile.Name(), ECompressionMethod::Gzip, MakeEvents(base));
    CheckSliceRanges(logFile.Name(), base);
}

TEST_F(TLogSliceTest, SlicePlainText)
{
    TTempFile logFile(GenerateRandomFileName("log") + ".log");
    auto base = ParseQueryTime("2021-06-07T08:09:10.000000Z");
    WriteLog(logFile.Name(), /*compressionMethod*/ std::nullopt, MakeEvents(base));
    CheckSliceRanges(logFile.Name(), base);
}

TEST_F(TLogSliceTest, SliceOpenBounds)
{
    TTempFile logFile(GenerateRandomFileName("log") + ".log");
    auto base = ParseQueryTime("2021-06-07T08:09:10.000000Z");
    WriteLog(logFile.Name(), /*compressionMethod*/ std::nullopt, MakeEvents(base));

    auto half = TDuration::MilliSeconds(500);

    // Open start (read from the beginning of file): everything up to msg-3.
    {
        auto messages = MessagesOf(Slice(logFile.Name(), TInstant::Zero(), base + TDuration::Seconds(3) + half));
        EXPECT_EQ((std::vector<TString>{"msg-0", "msg-1", "msg-2", "msg-3"}), messages);
    }

    // Open end (read until the end of file): msg-6 onwards.
    {
        auto messages = MessagesOf(Slice(logFile.Name(), base + TDuration::Seconds(6) - half, TInstant::Max()));
        EXPECT_EQ((std::vector<TString>{"msg-6", "msg-7", "msg-8", "msg-9"}), messages);
    }

    // Both bounds open: the whole file.
    {
        auto messages = MessagesOf(Slice(logFile.Name(), TInstant::Zero(), TInstant::Max()));
        EXPECT_EQ(10u, messages.size());
        EXPECT_EQ("msg-0", messages.front());
        EXPECT_EQ("msg-9", messages.back());
    }
}

TEST_F(TLogSliceTest, GetTimeRange)
{
    auto base = ParseQueryTime("2021-06-07T08:09:10.000000Z");
    auto tolerance = TDuration::MilliSeconds(1);

    for (auto [suffix, method] : std::vector<std::pair<TString, std::optional<ECompressionMethod>>>{
        {".log", std::nullopt},
        {".zst", ECompressionMethod::Zstd},
        {".gz", ECompressionMethod::Gzip},
    }) {
        TTempFile logFile(GenerateRandomFileName("log") + suffix);
        WriteLog(logFile.Name(), method, MakeEvents(base));

        auto range = TimeRange(logFile.Name());
        ASSERT_TRUE(range.has_value());
        EXPECT_LT(range->first - base, tolerance);
        EXPECT_LT((base + TDuration::Seconds(9)) - range->second, tolerance);
    }

    // An empty file has no time range.
    {
        TTempFile logFile(GenerateRandomFileName("log") + ".log");
        TFile(logFile.Name(), CreateAlways | WrOnly).Close();
        EXPECT_FALSE(TimeRange(logFile.Name()).has_value());
    }
}

TEST_F(TLogSliceTest, GetTimeRangeBrokenLastLine)
{
    auto base = ParseQueryTime("2021-06-07T08:09:10.000000Z");
    auto tolerance = TDuration::MilliSeconds(1);

    // An actively written log is typically truncated mid-line: its physically last
    // block decodes to a final line whose leading timestamp is incomplete (no
    // microseconds). GetTimeRange must drop that broken line and report the
    // timestamp of the previous, complete line as "last".
    for (auto [suffix, method] : std::vector<std::pair<TString, std::optional<ECompressionMethod>>>{
        {".log", std::nullopt},
        {".zst", ECompressionMethod::Zstd},
        {".gz", ECompressionMethod::Gzip},
    }) {
        TTempFile logFile(GenerateRandomFileName("log") + suffix);
        WriteLog(logFile.Name(), method, MakeEvents(base));

        // Append a partially written line (an incomplete timestamp, no newline),
        // mimicking a log captured while the writer was mid-record.
        {
            TFile file(logFile.Name(), OpenExisting | WrOnly | ForAppend);
            TStringBuf broken = "2021-06-07 11:09:1";
            file.Write(broken.data(), broken.size());
        }

        auto range = TimeRange(logFile.Name());
        ASSERT_TRUE(range.has_value());
        EXPECT_LT(range->first - base, tolerance);
        // The last *complete* event is msg-9 at base + 9s; the broken trailing line
        // must be ignored rather than reported (or parsed as a bogus instant).
        EXPECT_LT((base + TDuration::Seconds(9)) - range->second, tolerance);
        EXPECT_LT(range->second - (base + TDuration::Seconds(9)), tolerance);

        // The read path must agree with --info: slicing to the end of the file
        // (open end) must surface the same last complete line, not stop a chunk
        // short of it.
        auto messages = MessagesOf(Slice(logFile.Name(), base - TDuration::MilliSeconds(500), TInstant::Max()));
        ASSERT_FALSE(messages.empty());
        EXPECT_EQ("msg-9", messages.back());
        EXPECT_EQ(10u, messages.size());
    }
}

// Test wrapper: runs the streaming FilterWithGrep over an in-memory string and
// collects grep's output back into a string.
static TString FilterWithGrep(const std::vector<TString>& grepArgs, const TString& input)
{
    TStringStream output;
    NYT::NLogSlice::FilterWithGrep(
        grepArgs,
        [&] (IOutputStream& sink) {
            sink.Write(input.data(), input.size());
        },
        output);
    return output.Str();
}

TEST_F(TLogSliceTest, FilterWithGrep)
{
    TString input = "alpha\nbeta\ngamma\n";

    // A plain pattern keeps only matching lines.
    EXPECT_EQ("beta\n", FilterWithGrep({"beta"}, input));

    // No match yields an empty result (grep exit code 1 is not an error).
    EXPECT_EQ("", FilterWithGrep({"zzz"}, input));

    // Options are forwarded verbatim: -v inverts, -i is case-insensitive.
    EXPECT_EQ("alpha\ngamma\n", FilterWithGrep({"-v", "beta"}, input));
    EXPECT_EQ("Alpha\n", FilterWithGrep({"-i", "alpha"}, "Alpha\nbeta\n"));
}

TEST_F(TLogSliceTest, SplitCommandLine)
{
    EXPECT_EQ((std::vector<TString>{"-o", "-E", "[0-9]+"}), SplitCommandLine("-o -E [0-9]+"));

    // A quoted multi-word pattern stays a single argument.
    EXPECT_EQ((std::vector<TString>{"-E", "foo bar"}), SplitCommandLine("-E 'foo bar'"));
    EXPECT_EQ((std::vector<TString>{"foo bar baz"}), SplitCommandLine("\"foo bar baz\""));

    // Surrounding whitespace and empty input.
    EXPECT_EQ((std::vector<TString>{"a", "b"}), SplitCommandLine("  a   b  "));
    EXPECT_TRUE(SplitCommandLine("   ").empty());

    // An unbalanced quote is rejected.
    EXPECT_THROW(SplitCommandLine("-E 'oops"), std::exception);
}

TEST_F(TLogSliceTest, GrepArgString)
{
    // -o with -E extracts only the matched substring.
    EXPECT_EQ("123\n", FilterWithGrep(SplitCommandLine("-o -E [0-9]+"), "abc123def\n"));

    // A multi-word pattern matches only the line containing it verbatim.
    EXPECT_EQ("foo bar baz\n", FilterWithGrep(SplitCommandLine("'foo bar'"), "foo bar baz\nfoobar\n"));

    // -E alternation selector works.
    EXPECT_EQ("alpha\ngamma\n", FilterWithGrep(SplitCommandLine("-E 'alpha|gamma'"), "alpha\nbeta\ngamma\n"));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NLogSlice
