#include <yt/yt/tools/logslice/lib/log_slice_engine.h>
#include <yt/yt/tools/logslice/lib/time_parser.h>

#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/logging/config.h>
#include <yt/yt/core/logging/file_log_writer.h>
#include <yt/yt/core/logging/formatter.h>
#include <yt/yt/core/logging/log_writer.h>
#include <yt/yt/core/logging/log_writer_factory.h>
#include <yt/yt/core/logging/system_log_event_provider.h>

#include <yt/yt/core/concurrency/scheduler_api.h>

#include <yt/yt/core/ytree/fluent.h>

#include <library/cpp/streams/zstd/zstd.h>

#include <library/cpp/yt/logging/logger.h>
#include <library/cpp/yt/logging/structured_payload.h>
#include <library/cpp/yt/logging/tagged_payload.h>

#include <library/cpp/yt/cpu_clock/clock.h>

#include <util/generic/size_literals.h>

#include <util/stream/file.h>
#include <util/stream/str.h>
#include <util/stream/zlib.h>

#include <util/string/split.h>

#include <util/system/file.h>
#include <util/system/tempfile.h>

namespace NYT::NLogSlice {
namespace {

using namespace NConcurrency;
using namespace NLogging;

////////////////////////////////////////////////////////////////////////////////

const TLoggingCategory* GetTestCategory()
{
    static NLogging::TLogger Logger("Test");
    return Logger.GetCategory();
}

class TOutputFailingOnce
    : public IOutputStream
{
private:
    bool Failed_ = false;

    void DoWrite(const void* /*buffer*/, size_t /*length*/) override
    {
        if (!Failed_) {
            Failed_ = true;
            throw std::runtime_error("Output failed");
        }
    }
};

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
        event.Payload = MakeTaggedPayloadFromMessage(message);
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
        const std::vector<std::pair<TInstant, TString>>& events,
        bool structured = false)
    {
        auto writer = CreateWriter(fileName, compressionMethod, structured ? ELogFormat::Yson : ELogFormat::PlainText);
        for (const auto& [instant, message] : events) {
            auto event = MakeEvent(instant, message);
            if (structured) {
                event.Family = ELogFamily::Structured;
                event.Payload = MakeStructuredPayloadFromYson(
                    NYTree::BuildYsonStringFluently<NYson::EYsonType::MapFragment>()
                        .Item("timestamp").Value(instant)
                        .Item("event_type").Value("fair_share_info")
                        .Item("snapshot").Value(message)
                        .Finish());
            }
            writer->Write(event);
            writer->Flush();
        }
        writer->Flush();
    }

    using TLogBlock = std::vector<std::pair<TInstant, TString>>;
    using TLogSession = std::vector<TLogBlock>;

    //! Writes [sessions] to [fileName], reopening the writer for each session and
    //! flushing after each block, so a compressed log gets one block per entry.
    //! Structured "Logging started" events carry no "timestamp", as the system
    //! event provider writes them.
    void WriteLogSessions(
        const TString& fileName,
        std::optional<ECompressionMethod> compressionMethod,
        const std::vector<TLogSession>& sessions,
        ELogFormat format = ELogFormat::PlainText)
    {
        for (const auto& session : sessions) {
            auto writer = CreateWriter(fileName, compressionMethod, format);
            for (const auto& block : session) {
                for (const auto& [instant, message] : block) {
                    auto event = MakeEvent(instant, message);
                    if (format != ELogFormat::PlainText) {
                        event.Family = ELogFamily::Structured;
                        event.Payload = MakeStructuredPayloadFromYson(
                            NYTree::BuildYsonStringFluently<NYson::EYsonType::MapFragment>()
                                .DoIf(!message.StartsWith("Logging started"), [&] (auto fluent) {
                                    fluent.Item("timestamp").Value(instant);
                                })
                                .Item("message").Value(message)
                                .Finish());
                    }
                    writer->Write(event);
                }
                writer->Flush();
            }
        }
    }

    IFileLogWriterPtr CreateWriter(
        const TString& fileName,
        std::optional<ECompressionMethod> compressionMethod,
        ELogFormat format)
    {
        auto config = New<TFileLogWriterConfig>();
        config->FileName = fileName;
        config->EnableCompression = compressionMethod.has_value();
        if (compressionMethod) {
            config->CompressionMethod = *compressionMethod;
        }
        // Keep the file deterministic: no "Logging started" banner.
        config->EnableSystemMessages = false;

        std::unique_ptr<ILogFormatter> formatter;
        if (format == ELogFormat::PlainText) {
            formatter = std::make_unique<TPlainTextLogFormatter>();
        } else {
            formatter = std::make_unique<TStructuredLogFormatter>(TStructuredLogFormatterOptions{
                .Format = format,
            });
        }
        return CreateFileLogWriter(
            std::move(formatter),
            CreateDefaultSystemLogEventProvider(config),
            "test_writer",
            config,
            this);
    }

    //! Slices [fileName] using an auto-detected engine (so the file's extension
    //! also drives which engine is selected).
    std::vector<std::string> Slice(
        const TString& fileName,
        TInstant from,
        TInstant to,
        TDuration seekMargin = TDuration::Zero())
    {
        auto engine = CreateLogSliceEngine(ECompressionCodec::Auto, fileName);

        TFile file(fileName, OpenExisting | RdOnly);
        TStringStream output;
        engine->Slice(file, from, to, seekMargin, output);

        std::vector<std::string> lines;
        for (TStringBuf line : StringSplitter(output.Str()).Split('\n').SkipEmpty()) {
            lines.emplace_back(line);
        }
        return lines;
    }

    //! Extracts the message field (4th tab-separated column) of a formatted log line.
    static std::string MessageOf(TStringBuf line)
    {
        TStringBuf rest = line;
        rest.NextTok('\t'); // timestamp
        rest.NextTok('\t'); // level
        rest.NextTok('\t'); // category
        return std::string(rest.NextTok('\t'));
    }

    std::vector<std::string> MessagesOf(const std::vector<std::string>& lines)
    {
        std::vector<std::string> messages;
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
            EXPECT_EQ((std::vector<std::string>{"msg-2", "msg-3", "msg-4", "msg-5"}), messages);
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
            EXPECT_EQ((std::vector<std::string>{"msg-7"}), messages);
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

    std::optional<TInstant> FindFirstTime(const TString& fileName)
    {
        auto engine = CreateLogSliceEngine(ECompressionCodec::Auto, fileName);
        TFile file(fileName, OpenExisting | RdOnly);
        return engine->FindFirstTime(file);
    }

    static void CompressAsOneFrame(const TString& sourceFileName, const TString& fileName, ECompressionMethod method)
    {
        auto text = TUnbufferedFileInput(sourceFileName).ReadAll();
        TUnbufferedFileOutput output(fileName);
        std::unique_ptr<IOutputStream> compressor;
        if (method == ECompressionMethod::Zstd) {
            compressor = std::make_unique<TZstdCompress>(&output);
        } else {
            compressor = std::make_unique<TZLibCompress>(&output, ZLib::GZip);
        }
        compressor->Write(text);
        compressor->Finish();
    }

    std::vector<std::pair<TInstant, TString>> MakeEvents(TInstant base)
    {
        std::vector<std::pair<TInstant, TString>> events;
        for (int i = 0; i < 10; ++i) {
            events.emplace_back(base + TDuration::Seconds(i), Format("msg-%v", i));
        }
        return events;
    }

    static void WriteOtherFormatLines(const TString& fileName, i64 size)
    {
        TUnbufferedFileOutput output(fileName);
        i64 written = 0;
        for (int index = 0; written < size; ++index) {
            auto line = Format("2021-06-07 08:00:00.000 INFO  worker(pid=1, tid=0x1) [default] main.cpp:49: line %v\n", index);
            output << line;
            written += std::ssize(line);
        }
    }

    static int WriteLinesWithRandomTails(const TString& fileName, i64 size, const std::function<TString(int)>& makeLineHead)
    {
        TUnbufferedFileOutput output(fileName);
        ui64 state = 1;
        i64 written = 0;
        int index = 0;
        for (; written < size; ++index) {
            auto line = makeLineHead(index);
            for (int position = 0; position < 400; ++position) {
                state = state * 6364136223846793005ULL + 1442695040888963407ULL;
                line += "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ+/"[state >> 58];
            }

            line += '\n';
            output << line;
            written += std::ssize(line);
        }

        return index;
    }

    static i64 GetThreadReadByteCount()
    {
        auto io = TUnbufferedFileInput("/proc/thread-self/io").ReadAll();
        for (auto line : StringSplitter(io).Split('\n')) {
            TStringBuf value;
            if (line.Token().AfterPrefix("rchar: ", value)) {
                return FromString<i64>(value);
            }
        }
        THROW_ERROR_EXCEPTION("No read byte count in /proc/thread-self/io");
    }

    static i64 CountReadBytes(const std::function<void()>& action)
    {
        i64 before = GetThreadReadByteCount();
        action();
        return GetThreadReadByteCount() - before;
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

    // Master access logs use the structured JSON formatter and millisecond
    // precision. A payload field using the same key must not win over the
    // formatter's later system field.
    auto json = ParseLogLineTime(
        R"({"instant":"1999-01-01 00:00:00,000","instant":"2020-01-02 03:04:05,123","method":"PrepareUnmount"})");
    ASSERT_TRUE(json.has_value());
    EXPECT_EQ(query + TDuration::MicroSeconds(123000), *json);

    // Key-shaped text inside a payload string is not a structured timestamp.
    EXPECT_FALSE(ParseLogLineTime(
        R"({"message":"fake \"instant\":\"1999-01-01 00:00:00,000\""})").has_value());

    // Block probes may include more than one JSON line. Only the first record
    // describes the block's first timestamp; a later line must not replace it.
    auto jsonBlockPrefix = ParseLogLineTime(
        R"({"instant":"2020-01-02 03:04:05,123","method":"PrepareUnmount"}
{"instant":"2020-01-02 03:04:06,456","method":"CommitUnmount"})");
    ASSERT_TRUE(jsonBlockPrefix.has_value());
    EXPECT_EQ(query + TDuration::MicroSeconds(123000), *jsonBlockPrefix);

    auto yson = ParseLogLineTime(
        R"({"cluster"="watt";"timestamp"="2026-08-19T11:20:00.123456Z";"instant"="2026-08-19 14:20:00,123";};)");
    EXPECT_EQ(ParseQueryTime("2026-08-19T11:20:00.123456Z"), yson);
    EXPECT_FALSE(ParseLogLineTime(
        R"({"payload"={"timestamp"="2026-08-19T11:20:00Z";};};)").has_value());
    EXPECT_FALSE(ParseLogLineTime(
        R"({"message"="fake \"timestamp\"=\"2026-08-19T11:20:00Z\"";};)").has_value());
}

TEST_F(TLogSliceTest, ParseYsonInstant)
{
    auto expected = ParseQueryTime("2026-11-05 12:05:00,123456");
    auto record = R"({"bytes"=4096;"instant"="1999-01-01 00:00:00,000";"instant"="2026-11-05 12:05:00,123456";};)";
    EXPECT_EQ(ParseLogLineTime(record), expected);
    EXPECT_EQ(ParseLogLineTime(std::string(record) + "\n{\"instant\"=\"2026-11-05 12:06:00,000000\";};"), expected);
    for (auto invalid : {
        R"({"bytes"=4096;};)",
        R"({"instant"="not a timestamp";};)",
        R"({"instant"="2026-11-05";};)",
        R"({"instant"="2026-11-05 12:05:00,1234567";};)",
        R"({"payload"={"instant"="2026-11-05 12:05:00,123456";};};)",
        R"({"message"="fake \"instant\"=\"2026-11-05 12:05:00,123456\"";};)",
    }) {
        EXPECT_FALSE(ParseLogLineTime(invalid).has_value()) << invalid;
    }
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

    // Time-of-day forms share today's date; "HH:MM" is the "HH:MM:SS" with zero
    // seconds, and the seconds field offsets it as expected.
    EXPECT_EQ(ParseQueryTime("14:30:00"), ParseQueryTime("14:30"));
    EXPECT_EQ(ParseQueryTime("12:23:00") + TDuration::Seconds(34), ParseQueryTime("12:23:34"));

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

TEST_F(TLogSliceTest, ParseQueryTimeTruncated)
{
    // A bare date is that day's midnight; each coarser prefix matches the full
    // timestamp with the absent finer fields zeroed.
    EXPECT_EQ(ParseQueryTime("2026-06-19 00:00:00"), ParseQueryTime("2026-06-19"));
    EXPECT_EQ(ParseQueryTime("2026-06-19 15:00:00"), ParseQueryTime("2026-06-19 15"));
    EXPECT_EQ(ParseQueryTime("2026-06-19 15:52:00"), ParseQueryTime("2026-06-19 15:52"));

    // The time-of-day refines the date by exactly the expected offset.
    EXPECT_EQ(
        ParseQueryTime("2026-06-19") + TDuration::Hours(15) + TDuration::Minutes(52),
        ParseQueryTime("2026-06-19 15:52"));

    // A half-written field (too few digits, or a dangling separator) is rejected
    // rather than silently zero-padded. (A bare trailing space is not tested
    // here: ParseQueryTime strips it, leaving the valid bare date.)
    EXPECT_THROW(ParseQueryTime("2026-06-19 1"), std::exception);
    EXPECT_THROW(ParseQueryTime("2026-06-19 15:"), std::exception);
    EXPECT_THROW(ParseQueryTime("2026-06-19 15:5"), std::exception);
    EXPECT_THROW(ParseQueryTime("2026-06-19 15:52:"), std::exception);
    EXPECT_THROW(ParseQueryTime("2026-06-19 15:52:3"), std::exception);
    // A subsecond fraction still requires the seconds field to be present.
    EXPECT_THROW(ParseQueryTime("2026-06-19 15:52.5"), std::exception);
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

TEST_F(TLogSliceTest, SliceFindsLinesOutOfOrder)
{
    auto base = ParseQueryTime("2021-06-07T08:09:10.000000Z");
    auto at = [&] (int seconds) {
        return base + TDuration::Seconds(seconds);
    };
    auto half = TDuration::MilliSeconds(500);
    const char* logStart = "Logging started (Version: test)";
    auto blockPerLine = [] (const TLogBlock& lines) {
        TLogSession session;
        for (const auto& line : lines) {
            session.push_back({line});
        }

        return session;
    };
    auto clockJump = [&] (bool forward) {
        TLogBlock lines;
        for (int index = 0; index < 10; ++index) {
            auto instant = at(index);
            if (index >= 3 && index <= 6) {
                instant = forward ? instant + TDuration::Seconds(100) : instant - TDuration::Seconds(100);
            }
            lines.push_back({instant, Format("msg-%v", index)});
        }
        return blockPerLine(lines);
    };

    struct TCase
    {
        std::string Name;
        std::vector<TLogSession> Sessions;
        TInstant From;
        TInstant To;
        TDuration SeekMargin;
        std::vector<std::string> Expected;
    };
    std::vector<TCase> cases{
        {
            .Name = "LateAtStart",
            .Sessions = {blockPerLine({{at(0), "msg-0"}, {at(1), "msg-1"}, {at(3), "msg-3"}, {at(2), "msg-2"}, {at(4), "msg-4"}})},
            .From = at(2) + half,
            .To = at(4) + half,
            .SeekMargin = TDuration::Seconds(1),
            .Expected = {"msg-3", "msg-4"},
        },
        {
            .Name = "LateAtEnd",
            .Sessions = {blockPerLine({{at(0), "msg-0"}, {at(1), "msg-1"}, {at(2), "msg-2"}, {at(4), "msg-4"}, {at(3), "msg-3"}})},
            .From = at(0) + half,
            .To = at(3) + half,
            .SeekMargin = TDuration::Seconds(1),
            .Expected = {"msg-1", "msg-2", "msg-3"},
        },
        {
            .Name = "InOrderLogStart",
            .Sessions = {blockPerLine({{at(1), logStart}, {at(3), "msg-3"}})},
            .From = at(0) + half,
            .To = at(1) + half,
            .Expected = {logStart},
        },
        {
            .Name = "InOrderReopenedLogStart",
            .Sessions = {
                blockPerLine({{at(0), "msg-0"}}),
                blockPerLine({{at(2), logStart}, {at(4), "msg-4"}}),
            },
            .From = at(1) + half,
            .To = at(2) + half,
            .Expected = {logStart},
        },
        {
            .Name = "LogStartBeforeQueuedBlocks",
            .Sessions = {blockPerLine({{at(3), logStart}, {at(0), "msg-0"}, {at(1), "msg-1"}, {at(2), "msg-2"}, {at(4), "msg-4"}, {at(5), "msg-5"}})},
            .From = at(2) + half,
            .To = at(3) + half,
            .Expected = {logStart},
        },
        {
            .Name = "QueuedLinesInLogStartBlock",
            .Sessions = {{{{at(3), logStart}, {at(0), "msg-0"}, {at(1), "msg-1"}}, {{at(4), "msg-4"}}}},
            .From = at(0) + half,
            .To = at(1) + half,
            .Expected = {"msg-1"},
        },
        {
            .Name = "LogStartSharingBlockWithQueuedLines",
            .Sessions = {{{{at(3), logStart}, {at(0), "msg-0"}, {at(1), "msg-1"}}, {{at(4), "msg-4"}}}},
            .From = at(2) + half,
            .To = at(3) + half,
            .Expected = {logStart},
        },
        {
            .Name = "QueuedLinesInReopenedLogStartBlock",
            .Sessions = {
                blockPerLine({{at(0), "msg-0"}, {at(1), "msg-1"}}),
                {{{at(4), logStart}, {at(2), "msg-2"}, {at(3), "msg-3"}}, {{at(5), "msg-5"}}},
            },
            .From = at(1) + half,
            .To = at(2) + half,
            .Expected = {"msg-2"},
        },
        {
            .Name = "ForwardClockJumpInWholeFileWindow",
            .Sessions = {clockJump(/*forward*/ true)},
            .From = at(0) - half,
            .To = at(9) + half,
            .Expected = {"msg-0", "msg-1", "msg-2", "msg-7", "msg-8", "msg-9"},
        },
        {
            .Name = "BackwardClockJumpInWholeFileWindow",
            .Sessions = {clockJump(/*forward*/ false)},
            .From = at(0) - half,
            .To = at(9) + half,
            .Expected = {"msg-0", "msg-1", "msg-2", "msg-7", "msg-8", "msg-9"},
        },
    };

    for (const auto& testCase : cases) {
        for (const auto& [suffix, method] : std::vector<std::pair<TString, std::optional<ECompressionMethod>>>{
            {".log", std::nullopt},
            {".log.zst", ECompressionMethod::Zstd},
            {".log.gz", ECompressionMethod::Gzip},
        }) {
            SCOPED_TRACE(testCase.Name + std::string(suffix));
            TTempFile logFile(GenerateRandomFileName("log") + suffix);
            WriteLogSessions(logFile.Name(), method, testCase.Sessions);
            auto messages = MessagesOf(Slice(logFile.Name(), testCase.From, testCase.To, testCase.SeekMargin));
            EXPECT_EQ(testCase.Expected, messages);
        }
    }
}

TEST_F(TLogSliceTest, SliceStructuredWithQueuedLinesInLogStartBlock)
{
    auto base = ParseQueryTime("2021-06-07T08:09:10.000000Z");
    auto at = [&] (int seconds) {
        return base + TDuration::Seconds(seconds);
    };
    auto half = TDuration::MilliSeconds(500);
    std::vector<TLogSession> sessions{
        {{{at(3), "Logging started"}, {at(0), "msg-0"}, {at(1), "msg-1"}}, {{at(4), "msg-4"}}},
    };
    for (const auto& [format, extension] : std::vector<std::pair<ELogFormat, TString>>{
        {ELogFormat::Yson, ".yson"},
        {ELogFormat::Json, ".json"},
    }) {
        for (const auto& [suffix, method] : std::vector<std::pair<TString, std::optional<ECompressionMethod>>>{
            {"", std::nullopt},
            {".zst", ECompressionMethod::Zstd},
            {".gz", ECompressionMethod::Gzip},
        }) {
            SCOPED_TRACE(std::string(extension + suffix));
            TTempFile logFile(GenerateRandomFileName("log") + extension + suffix);
            WriteLogSessions(logFile.Name(), method, sessions, format);

            auto queued = Slice(logFile.Name(), at(0) + half, at(1) + half);
            ASSERT_EQ(queued.size(), 1u);
            EXPECT_NE(queued[0].find("msg-1"), std::string::npos);

            auto firstTime = FindFirstTime(logFile.Name());
            ASSERT_TRUE(firstTime.has_value());
            EXPECT_LT(*firstTime - at(0), TDuration::MilliSeconds(1));
            EXPECT_LT(at(0) - *firstTime, TDuration::MilliSeconds(1));

            auto logStart = Slice(logFile.Name(), at(2) + half, at(3) + half);
            ASSERT_EQ(logStart.size(), 1u);
            EXPECT_NE(logStart[0].find("Logging started"), std::string::npos);
        }
    }
}

TEST_F(TLogSliceTest, SliceYsonAndTimeRange)
{
    auto base = ParseQueryTime("2026-08-19T11:20:00.123456Z");
    TString snapshot(6_MB, 'x');
    std::vector<std::pair<TInstant, TString>> events = {
        {base, "before"},
        {base + TDuration::Seconds(1), snapshot},
        {base + TDuration::Seconds(2), "after"},
    };
    for (const auto& [suffix, method] : std::vector<std::pair<TString, std::optional<ECompressionMethod>>>{
        {".yson", std::nullopt},
        {".yson.zstd", ECompressionMethod::Zstd},
        {".yson.gz", ECompressionMethod::Gzip},
    }) {
        TTempFile logFile(GenerateRandomFileName("log") + suffix);
        WriteLog(logFile.Name(), method, events, /*structured*/ true);
        auto range = TimeRange(logFile.Name());
        ASSERT_TRUE(range.has_value());
        EXPECT_EQ(range->first, base);
        EXPECT_EQ(range->second, base + TDuration::Seconds(2));

        auto all = Slice(logFile.Name(), TInstant::Zero(), TInstant::Max());
        ASSERT_EQ(all.size(), 3u);
        EXPECT_NE(all[1].find(std::string(snapshot)), std::string::npos);
        auto middle = Slice(logFile.Name(), base + TDuration::Seconds(1), base + TDuration::Seconds(1));
        EXPECT_EQ(middle, (std::vector<std::string>{all[1]}));
    }
}

TEST_F(TLogSliceTest, SliceIOTrackerAndTimeRange)
{
    auto base = ParseQueryTime("2026-11-05 12:00:00");
    auto end = ParseQueryTime("2026-11-05 12:16:00");
    std::vector<std::pair<TInstant, TString>> events = {
        {base - TDuration::Seconds(1), "before"},
        {base + TDuration::Minutes(5), "inside"},
        {end + TDuration::Seconds(1), "after"},
    };
    for (const auto& channel : {"io_aggr", "io_raw"}) {
        for (const auto& [suffix, method] : std::vector<std::pair<TString, std::optional<ECompressionMethod>>>{
            {"", std::nullopt},
            {".2.zst", ECompressionMethod::Zstd},
            {".2026-11-05_12-16.gz", ECompressionMethod::Gzip},
        }) {
            SCOPED_TRACE(std::string(channel) + std::string(suffix));
            TTempFile logFile(GenerateRandomFileName("node") + "." + channel + ".yson.log" + suffix);
            {
                auto writer = CreateWriter(logFile.Name(), method, ELogFormat::Yson);
                for (const auto& [instant, message] : events) {
                    auto event = MakeEvent(instant, message);
                    event.Family = ELogFamily::Structured;
                    event.Payload = MakeStructuredPayloadFromYson(
                        NYTree::BuildYsonStringFluently<NYson::EYsonType::MapFragment>()
                            .Item("object_type").Value("read")
                            .Item("message").Value(message)
                            .Finish());
                    writer->Write(event);
                    writer->Flush();
                }
            }
            auto lines = Slice(logFile.Name(), base, end);
            ASSERT_EQ(lines.size(), 1u);
            EXPECT_NE(lines[0].find("inside"), std::string::npos);
            EXPECT_EQ(lines[0].find("timestamp"), std::string::npos);
            auto range = TimeRange(logFile.Name());
            ASSERT_TRUE(range.has_value());
            auto tolerance = TDuration::MilliSeconds(1);
            EXPECT_GT(range->first, events.front().first - tolerance);
            EXPECT_LT(range->first, events.front().first + tolerance);
            EXPECT_GT(range->second, events.back().first - tolerance);
            EXPECT_LT(range->second, events.back().first + tolerance);
        }
    }
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
        EXPECT_EQ((std::vector<std::string>{"msg-0", "msg-1", "msg-2", "msg-3"}), messages);
    }

    // Open end (read until the end of file): msg-6 onwards.
    {
        auto messages = MessagesOf(Slice(logFile.Name(), base + TDuration::Seconds(6) - half, TInstant::Max()));
        EXPECT_EQ((std::vector<std::string>{"msg-6", "msg-7", "msg-8", "msg-9"}), messages);
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

TEST_F(TLogSliceTest, FindFirstTime)
{
    auto base = ParseQueryTime("2021-06-07T08:09:10.000000Z");
    auto tolerance = TDuration::MilliSeconds(1);

    TTempFile plainFile(GenerateRandomFileName("log") + ".log");
    WriteLog(plainFile.Name(), /*compressionMethod*/ std::nullopt, MakeEvents(base));
    TTempFile zstdFile(GenerateRandomFileName("log") + ".zst");
    WriteLog(zstdFile.Name(), ECompressionMethod::Zstd, MakeEvents(base));
    TTempFile gzipFile(GenerateRandomFileName("log") + ".gz");
    WriteLog(gzipFile.Name(), ECompressionMethod::Gzip, MakeEvents(base));
    TTempFile zstdFrameFile(GenerateRandomFileName("log") + ".zst");
    CompressAsOneFrame(plainFile.Name(), zstdFrameFile.Name(), ECompressionMethod::Zstd);
    TTempFile gzipMemberFile(GenerateRandomFileName("log") + ".gz");
    CompressAsOneFrame(plainFile.Name(), gzipMemberFile.Name(), ECompressionMethod::Gzip);

    for (const auto* logFile : {&plainFile, &zstdFile, &gzipFile, &zstdFrameFile, &gzipMemberFile}) {
        auto firstTime = FindFirstTime(logFile->Name());
        ASSERT_TRUE(firstTime.has_value()) << logFile->Name();
        EXPECT_LT(*firstTime - base, tolerance) << logFile->Name();
        EXPECT_LT(base - *firstTime, tolerance) << logFile->Name();
    }
}

TEST_F(TLogSliceTest, FindFirstTimeWithoutTimestampedLines)
{
    TTempFile emptyFile(GenerateRandomFileName("log") + ".log");
    TFile(emptyFile.Name(), CreateAlways | WrOnly).Close();
    EXPECT_FALSE(FindFirstTime(emptyFile.Name()).has_value());

    TTempFile untimedFile(GenerateRandomFileName("log") + ".log");
    TUnbufferedFileOutput(untimedFile.Name()).Write("not a log line\nnor this one\n");
    EXPECT_FALSE(FindFirstTime(untimedFile.Name()).has_value());
}

TEST_F(TLogSliceTest, FindRangeWithoutTimestampedLines)
{
    TTempFile plainFile(GenerateRandomFileName("log") + ".log");
    WriteOtherFormatLines(plainFile.Name(), 4_MB);
    TTempFile zstdFile(GenerateRandomFileName("log") + ".zst");
    CompressAsOneFrame(plainFile.Name(), zstdFile.Name(), ECompressionMethod::Zstd);

    for (const auto* logFile : {&plainFile, &zstdFile}) {
        auto engine = CreateLogSliceEngine(ECompressionCodec::Auto, logFile->Name());
        TFile file(logFile->Name(), OpenExisting | RdOnly);
        std::optional<std::pair<i64, i64>> range;
        auto readBytes = CountReadBytes([&] {
            range = engine->FindRange(file, TInstant::Zero(), TInstant::Max(), /*seekMargin*/ TDuration::Zero());
        });
        EXPECT_FALSE(range.has_value()) << logFile->Name();
        EXPECT_LT(readBytes, 3_MBs) << logFile->Name();
        EXPECT_TRUE(Slice(logFile->Name(), TInstant::Zero(), TInstant::Max()).empty()) << logFile->Name();
    }
}

TEST_F(TLogSliceTest, FindRangeReadsLittleOfLargeFrameWithoutTimestamps)
{
    TTempFile plainFile(GenerateRandomFileName("log") + ".log");
    WriteLinesWithRandomTails(plainFile.Name(), 80_MB, [] (int index) {
        return Format("2021-06-07 08:00:00.000 INFO  worker(pid=1, tid=0x1) [default] main.cpp:49: line %v ", index);
    });
    TTempFile zstdFile(GenerateRandomFileName("log") + ".zst");
    CompressAsOneFrame(plainFile.Name(), zstdFile.Name(), ECompressionMethod::Zstd);

    auto engine = CreateLogSliceEngine(ECompressionCodec::Auto, zstdFile.Name());
    TFile file(zstdFile.Name(), OpenExisting | RdOnly);
    ASSERT_GT(file.GetLength(), 40_MBs);

    std::optional<std::pair<i64, i64>> range;
    auto readBytes = CountReadBytes([&] {
        range = engine->FindRange(file, TInstant::Zero(), TInstant::Max(), /*seekMargin*/ TDuration::Zero());
    });
    EXPECT_FALSE(range.has_value());
    EXPECT_LT(readBytes, 40_MBs);

    std::optional<TInstant> firstTime;
    auto firstTimeReadBytes = CountReadBytes([&] {
        firstTime = engine->FindFirstTime(file);
    });
    EXPECT_FALSE(firstTime.has_value());
    EXPECT_LT(firstTimeReadBytes, 40_MBs);
}

TEST_F(TLogSliceTest, SliceFrameLargerThanProbeReach)
{
    auto base = ParseQueryTime("2021-06-07T08:09:10.000000Z");
    TTempFile plainFile(GenerateRandomFileName("log") + ".log");
    int lineCount = WriteLinesWithRandomTails(plainFile.Name(), 40_MB, [&] (int index) {
        return Format("%v\tI\tTest\tline-%v\t", FormatLogTime(base + TDuration::MilliSeconds(index)), index);
    });
    TTempFile zstdFile(GenerateRandomFileName("log") + ".zst");
    CompressAsOneFrame(plainFile.Name(), zstdFile.Name(), ECompressionMethod::Zstd);

    auto engine = CreateLogSliceEngine(ECompressionCodec::Auto, zstdFile.Name());
    TFile file(zstdFile.Name(), OpenExisting | RdOnly);
    i64 fileSize = file.GetLength();
    constexpr i64 FrameProbeLimit = 16_MB;
    ASSERT_GT(fileSize, FrameProbeLimit);

    auto from = base + TDuration::MicroSeconds(99'500);
    auto to = base + TDuration::MicroSeconds(199'500);
    auto range = engine->FindRange(file, from, to, /*seekMargin*/ TDuration::Zero());
    ASSERT_TRUE(range.has_value());
    EXPECT_EQ(0, range->first);
    EXPECT_EQ(fileSize, range->second);
    EXPECT_EQ(100, std::ssize(Slice(zstdFile.Name(), from, to)));

    auto timeRange = engine->GetTimeRange(file);
    ASSERT_TRUE(timeRange.has_value());
    EXPECT_EQ(ParseLogLineTime(FormatLogTime(base)), timeRange->first);
    EXPECT_EQ(ParseLogLineTime(FormatLogTime(base + TDuration::MilliSeconds(lineCount - 1))), timeRange->second);
}

TEST_F(TLogSliceTest, SliceSkipsLinesOfOtherFormat)
{
    auto base = ParseQueryTime("2021-06-07T08:09:10.000000Z");
    constexpr i64 OtherFormatSize = 3_MB;
    TTempFile logFile(GenerateRandomFileName("log") + ".log");
    WriteOtherFormatLines(logFile.Name(), OtherFormatSize);
    {
        TUnbufferedFileOutput output(TFile(logFile.Name(), OpenExisting | WrOnly | ForAppend));
        for (const auto& [instant, message] : MakeEvents(base)) {
            output << FormatLogTime(instant) << "\tI\tTest\t" << message << "\n";
        }
    }

    CheckSliceRanges(logFile.Name(), base);

    auto engine = CreateLogSliceEngine(ECompressionCodec::Auto, logFile.Name());
    TFile file(logFile.Name(), OpenExisting | RdOnly);
    auto range = engine->FindRange(file, base + TDuration::Seconds(3), base + TDuration::Seconds(4), /*seekMargin*/ TDuration::Zero());
    ASSERT_TRUE(range.has_value());
    EXPECT_GT(range->first, OtherFormatSize);
}

TEST_F(TLogSliceTest, GetTimeRangeSkipsLongTailWithoutTimestamps)
{
    auto base = ParseQueryTime("2021-06-07T08:09:10.000000Z");
    TTempFile tail(GenerateRandomFileName("log") + ".log");
    WriteOtherFormatLines(tail.Name(), 3_MB);
    TTempFile logFile(GenerateRandomFileName("log") + ".log");
    {
        TUnbufferedFileOutput output(logFile.Name());
        for (const auto& [instant, message] : MakeEvents(base)) {
            output << FormatLogTime(instant) << "\tI\tTest\t" << message << "\n";
        }
        output << TUnbufferedFileInput(tail.Name()).ReadAll();
    }

    auto range = TimeRange(logFile.Name());
    ASSERT_TRUE(range.has_value());
    EXPECT_EQ(range->first, ParseLogLineTime(FormatLogTime(base)));
    EXPECT_EQ(range->second, ParseLogLineTime(FormatLogTime(base + TDuration::Seconds(9))));
}

TEST_F(TLogSliceTest, FindRangeEndsBeforeLongTailWithoutTimestamps)
{
    auto base = ParseQueryTime("2021-06-07T08:09:10.000000Z");
    TTempFile tail(GenerateRandomFileName("log") + ".log");
    WriteOtherFormatLines(tail.Name(), 3_MB / 2);
    TTempFile logFile(GenerateRandomFileName("log") + ".log");
    i64 timestampedSize = 0;
    {
        TUnbufferedFileOutput output(logFile.Name());
        for (int index = 0; timestampedSize < 4_MBs; ++index) {
            auto line = Format("%v\tI\tTest\tline-%v\n", FormatLogTime(base + TDuration::MilliSeconds(index)), index);
            output << line;
            timestampedSize += std::ssize(line);
        }
        output << TUnbufferedFileInput(tail.Name()).ReadAll();
    }

    auto from = base + TDuration::MicroSeconds(99'500);
    auto to = base + TDuration::MicroSeconds(199'500);
    auto engine = CreateLogSliceEngine(ECompressionCodec::Auto, logFile.Name());
    TFile file(logFile.Name(), OpenExisting | RdOnly);
    auto range = engine->FindRange(file, from, to, /*seekMargin*/ TDuration::Zero());
    ASSERT_TRUE(range.has_value());
    EXPECT_LT(range->second, timestampedSize / 2);
    EXPECT_EQ(std::ssize(Slice(logFile.Name(), from, to)), 100);
}

TEST_F(TLogSliceTest, GetTimeRangeFindsLastLineAtProbeWindowEdge)
{
    constexpr i64 LineProbeLimit = 1_MB;
    auto base = ParseQueryTime("2021-06-07T08:09:10.000000Z");
    auto first = FormatLogTime(base) + "\tI\tTest\tmsg-0\n";
    auto last = FormatLogTime(base + TDuration::Seconds(9)) + "\tI\tTest\tmsg-9\n";
    std::string gap;
    while (std::ssize(gap) < 5 * LineProbeLimit / 2) {
        gap += std::string(99, 'x') + "\n";
    }
    i64 tailSize = LineProbeLimit + 1 - std::ssize(last);
    std::string tail;
    while (std::ssize(tail) + 100 <= tailSize) {
        tail += std::string(99, 'x') + "\n";
    }
    tail += std::string(tailSize - std::ssize(tail) - 1, 'x') + "\n";
    TTempFile logFile(GenerateRandomFileName("log") + ".log");
    TUnbufferedFileOutput(logFile.Name()).Write(first + gap + last + tail);

    auto engine = CreateLogSliceEngine(ECompressionCodec::Auto, logFile.Name());
    TFile file(logFile.Name(), OpenExisting | RdOnly);
    std::optional<std::pair<TInstant, TInstant>> range;
    auto readBytes = CountReadBytes([&] {
        range = engine->GetTimeRange(file);
    });
    ASSERT_TRUE(range.has_value());
    EXPECT_EQ(range->second, ParseLogLineTime(last));
    EXPECT_LT(readBytes, 4_MBs);
}

TEST_F(TLogSliceTest, FindRangeStartsAtLargeLastFrame)
{
    auto base = ParseQueryTime("2021-06-07T08:09:10.000000Z");
    auto events = MakeEvents(base);
    ui64 state = 1;
    auto makePadding = [&] {
        std::string padding;
        for (int index = 0; index < 20'000; ++index) {
            state = state * 6364136223846793005ULL + 1442695040888963407ULL;
            padding += "0123456789abcdef"[state >> 60];
        }
        return padding;
    };
    auto writeFrame = [&] (int begin, int end, bool padded, const TString& fileName) {
        TTempFile text(GenerateRandomFileName("log") + ".log");
        {
            TUnbufferedFileOutput output(text.Name());
            for (int index = begin; index < end; ++index) {
                output << FormatLogTime(events[index].first) << "\tI\tTest\t" << events[index].second << "\t" << (padded ? makePadding() : "") << "\n";
            }
        }
        CompressAsOneFrame(text.Name(), fileName, ECompressionMethod::Zstd);
        return TUnbufferedFileInput(fileName).ReadAll();
    };
    TTempFile firstFrame(GenerateRandomFileName("log") + ".zst");
    TTempFile lastFrame(GenerateRandomFileName("log") + ".zst");
    auto firstFrameData = writeFrame(0, 5, /*padded*/ false, firstFrame.Name());
    auto lastFrameData = writeFrame(5, 10, /*padded*/ true, lastFrame.Name());
    ASSERT_GT(std::ssize(lastFrameData), 2 * std::ssize(firstFrameData));
    TTempFile logFile(GenerateRandomFileName("log") + ".zst");
    TUnbufferedFileOutput(logFile.Name()).Write(firstFrameData + lastFrameData);

    auto half = TDuration::MilliSeconds(500);
    auto engine = CreateLogSliceEngine(ECompressionCodec::Auto, logFile.Name());
    TFile file(logFile.Name(), OpenExisting | RdOnly);
    auto range = engine->FindRange(file, base + TDuration::Seconds(7) - half, base + TDuration::Seconds(7) + half, /*seekMargin*/ TDuration::Zero());
    ASSERT_TRUE(range.has_value());
    EXPECT_EQ(range->first, std::ssize(firstFrameData));
    EXPECT_EQ(
        MessagesOf(Slice(logFile.Name(), base + TDuration::Seconds(7) - half, base + TDuration::Seconds(7) + half)),
        (std::vector<std::string>{"msg-7"}));
}

TEST_F(TLogSliceTest, SliceTruncatedFileKeepsOutputErrors)
{
    constexpr i64 ZstdBlockSize = 128_KB;
    constexpr i64 DecodeChunkSize = 256_KB;

    auto base = ParseQueryTime("2021-06-07T08:09:10.000000Z");
    TTempFile plainFile(GenerateRandomFileName("log") + ".log");
    {
        TUnbufferedFileOutput output(plainFile.Name());
        for (int index = 0; index < 10; ++index) {
            output << FormatLogTime(base + TDuration::Seconds(index)) << "\tI\tTest\t" << std::string(20_KB, 'x') << "\n";
        }
    }
    i64 decodedSize = TFile(plainFile.Name(), OpenExisting | RdOnly).GetLength();
    ASSERT_LT(ZstdBlockSize, decodedSize);
    ASSERT_LE(decodedSize, DecodeChunkSize);
    TTempFile zstdFile(GenerateRandomFileName("log") + ".zst");
    CompressAsOneFrame(plainFile.Name(), zstdFile.Name(), ECompressionMethod::Zstd);
    {
        TFile file(zstdFile.Name(), OpenExisting | RdWr);
        file.Resize(file.GetLength() - 1);
    }
    EXPECT_FALSE(Slice(zstdFile.Name(), TInstant::Zero(), TInstant::Max()).empty());

    auto engine = CreateLogSliceEngine(ECompressionCodec::Auto, zstdFile.Name());
    TFile file(zstdFile.Name(), OpenExisting | RdOnly);
    TOutputFailingOnce output;
    EXPECT_THROW(engine->Slice(file, TInstant::Zero(), TInstant::Max(), /*seekMargin*/ TDuration::Zero(), output), std::runtime_error);
}

TEST_F(TLogSliceTest, SliceCutsLongLines)
{
    auto base = ParseQueryTime("2021-06-07T08:09:10.000000Z");
    TTempFile logFile(GenerateRandomFileName("log") + ".log");
    {
        TUnbufferedFileOutput output(logFile.Name());
        for (int index = 0; index < 3; ++index) {
            output << FormatLogTime(base + TDuration::Seconds(index)) << "\tI\tTest\t";
            output << (index == 1 ? std::string(17_MB, 'x') : std::string("msg")) << "\n";
        }
    }

    auto lines = Slice(logFile.Name(), TInstant::Zero(), TInstant::Max());
    ASSERT_EQ(3u, lines.size());
    EXPECT_EQ(16_MB, lines[1].size());
    EXPECT_EQ("msg", MessageOf(lines[2]));
}

TEST_F(TLogSliceTest, DecodeOfFoundRangeHoldsSlicedLines)
{
    auto base = ParseQueryTime("2021-06-07T08:09:10.000000Z");
    auto from = base + TDuration::MilliSeconds(2500);
    auto to = base + TDuration::MilliSeconds(6500);
    for (const auto& [suffix, method] : std::vector<std::pair<std::string, std::optional<ECompressionMethod>>>{
        {".log", std::nullopt},
        {".zst", ECompressionMethod::Zstd},
        {".gz", ECompressionMethod::Gzip},
    }) {
        TTempFile logFile(GenerateRandomFileName("log") + suffix);
        WriteLog(logFile.Name(), method, MakeEvents(base));

        auto engine = CreateLogSliceEngine(ECompressionCodec::Auto, logFile.Name());
        TFile file(logFile.Name(), OpenExisting | RdOnly);
        auto [start, end] = engine->FindRange(file, from, to, /*seekMargin*/ TDuration::Zero()).value();
        EXPECT_LT(start, end);
        EXPECT_LE(end, file.GetLength());

        std::string text;
        engine->Decode(file, start, end, [&] (TStringBuf chunk) {
            text += chunk;
        });
        std::vector<std::string> lines;
        for (TStringBuf line : StringSplitter(text).Split('\n').SkipEmpty()) {
            auto time = ParseLogLineTime(line);
            if (time && *time >= from && *time <= to) {
                lines.emplace_back(line);
            }
        }
        EXPECT_EQ(lines, Slice(logFile.Name(), from, to)) << suffix;
    }
}

TEST_F(TLogSliceTest, DecodeKeepsRecoveredTail)
{
    auto base = ParseQueryTime("2021-06-07T08:09:10.000000Z");
    TTempFile plainFile(GenerateRandomFileName("log") + ".log");
    {
        TUnbufferedFileOutput output(plainFile.Name());
        for (int index = 0; index < 10; ++index) {
            output << FormatLogTime(base + TDuration::Seconds(index)) << "\tI\tTest\t" << std::string(20_KB, 'x') << "\n";
        }
    }
    TTempFile zstdFile(GenerateRandomFileName("log") + ".zst");
    CompressAsOneFrame(plainFile.Name(), zstdFile.Name(), ECompressionMethod::Zstd);
    {
        TFile file(zstdFile.Name(), OpenExisting | RdWr);
        file.Resize(file.GetLength() - 1);
    }

    auto engine = CreateLogSliceEngine(ECompressionCodec::Auto, zstdFile.Name());
    TFile file(zstdFile.Name(), OpenExisting | RdOnly);
    auto [start, end] = engine->FindRange(file, TInstant::Zero(), TInstant::Max(), /*seekMargin*/ TDuration::Zero()).value();
    std::string text;
    engine->Decode(file, start, end, [&] (TStringBuf chunk) {
        text += chunk;
    });
    std::vector<std::string> lines;
    for (TStringBuf line : StringSplitter(text).Split('\n').SkipEmpty()) {
        if (ParseLogLineTime(line)) {
            lines.emplace_back(line);
        }
    }
    EXPECT_FALSE(lines.empty());
    EXPECT_EQ(lines, Slice(zstdFile.Name(), TInstant::Zero(), TInstant::Max()));
}

// Test wrapper: runs the streaming FilterWithGrep over an in-memory string and
// collects grep's output back into a string.
static std::string FilterWithGrep(const std::vector<std::string>& grepArgs, const std::string& input)
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
    std::string input = "alpha\nbeta\ngamma\n";

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
    EXPECT_EQ((std::vector<std::string>{"-o", "-E", "[0-9]+"}), SplitCommandLine("-o -E [0-9]+"));

    // A quoted multi-word pattern stays a single argument.
    EXPECT_EQ((std::vector<std::string>{"-E", "foo bar"}), SplitCommandLine("-E 'foo bar'"));
    EXPECT_EQ((std::vector<std::string>{"foo bar baz"}), SplitCommandLine("\"foo bar baz\""));

    // Surrounding whitespace and empty input.
    EXPECT_EQ((std::vector<std::string>{"a", "b"}), SplitCommandLine("  a   b  "));
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
