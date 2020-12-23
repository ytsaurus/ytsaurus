#include <yt/core/test_framework/framework.h>

#include <yt/core/logging/log.h>
#include <yt/core/logging/log_manager.h>
#include <yt/core/logging/writer.h>

#include <yt/core/json/json_parser.h>

#include <yt/core/tracing/trace_context.h>

#include <yt/core/ytree/fluent.h>

#include <yt/core/ytree/convert.h>

#include <yt/core/misc/range_formatters.h>

#include <util/system/fs.h>

#include <util/stream/zlib.h>

#ifdef _unix_
#include <unistd.h>
#endif

namespace NYT::NLogging {

using namespace NYTree;
using namespace NYson;
using namespace NJson;

////////////////////////////////////////////////////////////////////////////////

static const TLogger Logger("Test");

class TLoggingTest
    : public ::testing::Test
{
public:
    TLoggingTest()
        : SomeDate("2014-04-24 23:41:09,804")
        , DateLength(SomeDate.length())
    {
        Category.Name = "category";
    }

protected:
    TLoggingCategory Category;
    TString SomeDate;
    int DateLength;

    IMapNodePtr DeserializeJson(const TString& source)
    {
        auto builder = CreateBuilderFromFactory(GetEphemeralNodeFactory());
        builder->BeginTree();
        TStringStream stream(source);
        ParseJson(&stream, builder.get());
        return builder->EndTree()->AsMap();
    }

    void WritePlainTextEvent(ILogWriter* writer)
    {
        TLogEvent event;
        event.MessageFormat = ELogMessageFormat::PlainText;
        event.Category = &Category;
        event.Level = ELogLevel::Debug;
        event.Message = TSharedRef::FromString("message");
        event.ThreadId = 0xba;

        WriteEvent(writer, event);
    }

    void WriteEvent(ILogWriter* writer, const TLogEvent& event)
    {
        writer->Write(event);
        writer->Flush();
    }

    std::vector<TString> ReadFile(const TString& fileName, bool compressed = false)
    {
        std::vector<TString> lines;

        TString line;
        auto input = TUnbufferedFileInput(fileName);
        if (!compressed) {
            while (input.ReadLine(line)) {
                lines.push_back(line + "\n");
            }
        } else {
            TZLibDecompress decompressor(&input);
            while (decompressor.ReadLine(line)) {
                lines.push_back(line + "\n");
            }
        }

        return lines;
    }

    void Configure(const TString& configYson)
    {
        auto configNode = ConvertToNode(TYsonString(configYson));
        auto config = ConvertTo<TLogManagerConfigPtr>(configNode);
        TLogManager::Get()->Configure(config);
    }

    void DoTestCompression(size_t compressionLevel)
    {
        NFs::Remove("test.log.gz");

        auto writer = New<TFileLogWriter>(
            std::make_unique<TPlainTextLogFormatter>(),
            "test_writer",
            "test.log.gz",
            /* enableCompression */ true,
            compressionLevel);
        WritePlainTextEvent(writer.Get());

        writer->Reload();
        WritePlainTextEvent(writer.Get());

        {
            auto lines = ReadFile("test.log.gz", true);
            EXPECT_EQ(5, lines.size());
            EXPECT_TRUE(lines[0].find("Logging started") != -1);
            EXPECT_EQ("\tD\tcategory\tmessage\tba\t\t\n", lines[1].substr(DateLength, lines[1].size()));
        }

        NFs::Remove("test.log.gz");
    }
};

#ifdef _unix_

TEST_F(TLoggingTest, ReloadOnSighup)
{
    NFs::Remove("reload-on-sighup.log");
    NFs::Remove("reload-on-sighup.log.1");

    Configure(R"({
        rules = [
            {
                "min_level" = "info";
                "writers" = [ "info" ];
            };
        ];
        "writers" = {
            "info" = {
                "file_name" = "reload-on-sighup.log";
                "type" = "file";
            };
        };
    })");

    WaitForPredicate([&] {
        YT_LOG_INFO("Message1");
        return NFs::Exists("reload-on-sighup.log");
    });

    NFs::Rename("reload-on-sighup.log", "reload-on-sighup.log.1");

    ::kill(::getpid(), SIGHUP);

    WaitForPredicate([&] {
        YT_LOG_INFO("Message2");
        return NFs::Exists("reload-on-sighup.log");
    });
}

#endif

TEST_F(TLoggingTest, FileWriter)
{
    NFs::Remove("test.log");

    auto writer = New<TFileLogWriter>(std::make_unique<TPlainTextLogFormatter>(), "test_writer", "test.log", false);
    WritePlainTextEvent(writer.Get());

    {
        auto lines = ReadFile("test.log");
        EXPECT_EQ(2, lines.size());
        EXPECT_TRUE(lines[0].find("Logging started") != -1);
        EXPECT_EQ("\tD\tcategory\tmessage\tba\t\t\n", lines[1].substr(DateLength, lines[1].size()));
    }

    writer->Reload();
    WritePlainTextEvent(writer.Get());

    {
        auto lines = ReadFile("test.log");
        EXPECT_EQ(5, lines.size());
        EXPECT_TRUE(lines[0].find("Logging started") != -1);
        EXPECT_EQ("\tD\tcategory\tmessage\tba\t\t\n", lines[1].substr(DateLength));
        EXPECT_EQ("\n", lines[2]);
        EXPECT_TRUE(lines[3].find("Logging started") != -1);
        EXPECT_EQ("\tD\tcategory\tmessage\tba\t\t\n", lines[4].substr(DateLength));
    }

    NFs::Remove("test.log");
}

TEST_F(TLoggingTest, Compression)
{
    // No compression.
    DoTestCompression(/* compressionLevel */ 0);

    // Default compression.
    DoTestCompression(/* compressionLevel */ 6);

    // Maximum compression.
    DoTestCompression(/* compressionLevel */ 9);
}

TEST_F(TLoggingTest, StreamWriter)
{
    TStringStream stringOutput;
    auto writer = New<TStreamLogWriter>(&stringOutput, std::make_unique<TPlainTextLogFormatter>(), "test_writer");

    WritePlainTextEvent(writer.Get());

    EXPECT_EQ(
       "\tD\tcategory\tmessage\tba\t\t\n",
       stringOutput.Str().substr(DateLength));
}

TEST_F(TLoggingTest, Rule)
{
    auto rule = New<TRuleConfig>();
    rule->Load(ConvertToNode(TYsonString(
        R"({
            exclude_categories = [ bus ];
            min_level = info;
            writers = [ some_writer ];
        })")));

    EXPECT_TRUE(rule->IsApplicable("some_service", ELogMessageFormat::PlainText));
    EXPECT_FALSE(rule->IsApplicable("bus", ELogMessageFormat::PlainText));
    EXPECT_FALSE(rule->IsApplicable("bus", ELogLevel::Debug, ELogMessageFormat::PlainText));
    EXPECT_FALSE(rule->IsApplicable("some_service", ELogLevel::Debug, ELogMessageFormat::PlainText));
    EXPECT_TRUE(rule->IsApplicable("some_service", ELogLevel::Warning, ELogMessageFormat::PlainText));
    EXPECT_TRUE(rule->IsApplicable("some_service", ELogLevel::Info, ELogMessageFormat::PlainText));
}

TEST_F(TLoggingTest, LogManager)
{
    NFs::Remove("test.log");
    NFs::Remove("test.error.log");

    Configure(R"({
        rules = [
            {
                "min_level" = "info";
                "writers" = [ "info" ];
            };
            {
                "min_level" = "error";
                "writers" = [ "error" ];
            };
        ];
        "writers" = {
            "error" = {
                "file_name" = "test.error.log";
                "type" = "file";
            };
            "info" = {
                "file_name" = "test.log";
                "type" = "file";
            };
        };
    })");

    YT_LOG_DEBUG("Debug message");
    YT_LOG_INFO("Info message");
    YT_LOG_ERROR("Error message");

    TLogManager::Get()->Synchronize();

    auto infoLog = ReadFile("test.log");
    auto errorLog = ReadFile("test.error.log");

    EXPECT_EQ(3, infoLog.size());
    EXPECT_EQ(2, errorLog.size());

    NFs::Remove("test.log");
    NFs::Remove("test.error.log");
}

TEST_F(TLoggingTest, StructuredJsonLogging)
{
    NFs::Remove("test.log");

    TLogEvent event;
    event.MessageFormat = ELogMessageFormat::Structured;
    event.Category = &Category;
    event.Level = ELogLevel::Debug;
    event.StructuredMessage = NYTree::BuildYsonStringFluently<EYsonType::MapFragment>()
        .Item("message")
        .Value("test_message")
        .Finish();

    auto writer = New<TFileLogWriter>(std::make_unique<TJsonLogFormatter>(THashMap<TString, INodePtr>{}), "test_writer", "test.log");
    WriteEvent(writer.Get(), event);
    TLogManager::Get()->Synchronize();

    auto log = ReadFile("test.log");

    auto logStartedJson = DeserializeJson(log[0]);
    EXPECT_EQ(logStartedJson->GetChildOrThrow("message")->AsString()->GetValue(), "Logging started");
    EXPECT_EQ(logStartedJson->GetChildOrThrow("level")->AsString()->GetValue(), "info");
    EXPECT_EQ(logStartedJson->GetChildOrThrow("category")->AsString()->GetValue(), "Logging");

    auto contentJson = DeserializeJson(log[1]);
    EXPECT_EQ(contentJson->GetChildOrThrow("message")->AsString()->GetValue(), "test_message");
    EXPECT_EQ(contentJson->GetChildOrThrow("level")->AsString()->GetValue(), "debug");
    EXPECT_EQ(contentJson->GetChildOrThrow("category")->AsString()->GetValue(), "category");

    NFs::Remove("test.log");
}

// This test is for manual check of YT_LOG_FATAL
TEST_F(TLoggingTest, DISABLED_LogFatal)
{
    NFs::Remove("test.log");
    NFs::Remove("test.error.log");

    Configure(R"({
        rules = [
            {
                "min_level" = "info";
                "writers" = [ "info" ];
            };
        ];
        "writers" = {
            "info" = {
                "file_name" = "test.log";
                "type" = "file";
            };
        };
    })");

    YT_LOG_INFO("Info message");

    Sleep(TDuration::MilliSeconds(100));

    YT_LOG_INFO("Info message");
    YT_LOG_FATAL("FATAL");

    NFs::Remove("test.log");
    NFs::Remove("test.error.log");
}

TEST_F(TLoggingTest, RequestSuppression)
{
    NFs::Remove("test.log");

    Configure(R"({
        rules = [
            {
                "min_level" = "info";
                "writers" = [ "info" ];
            };
        ];
        "writers" = {
            "info" = {
                "file_name" = "test.log";
                "type" = "file";
            };
        };
        "request_suppression_timeout" = 100;
    })");

    {
        auto requestId = NTracing::TRequestId::Create();
        auto traceContext = NTracing::CreateRootTraceContext("Test", requestId);
        NTracing::TTraceContextGuard guard(traceContext);

        YT_LOG_INFO("Traced message");

        TLogManager::Get()->SuppressRequest(requestId);
    }

    YT_LOG_INFO("Info message");

    TLogManager::Get()->Synchronize();

    auto lines = ReadFile("test.log");

    EXPECT_EQ(2, lines.size());
    EXPECT_TRUE(lines[0].find("Logging started") != -1);
    EXPECT_TRUE(lines[1].find("Info message") != -1);

    NFs::Remove("test.log");
}

////////////////////////////////////////////////////////////////////////////////

class TLongMessagesLoggingTest
    : public TLoggingTest
{
protected:
    static constexpr int N = 500;
    std::vector<TString> Chunks_;

    TLongMessagesLoggingTest()
    {
        for (int i = 0; i < N; ++i) {
            Chunks_.push_back(Format("PayloadPayloadPayloadPayloadPayload%v", i));
        }
    }

    void ConfigureForLongMessages()
    {
        NFs::Remove("test.log");

        Configure(R"({
            rules = [
                {
                    "min_level" = "info";
                    "max_level" = "info";
                    "writers" = [ "info" ];
                };
            ];
            "writers" = {
                "info" = {
                    "file_name" = "test.log";
                    "type" = "file";
                };
            };
        })");
    }

    void LogLongMessages()
    {
        for (int i = 0; i < N; ++i) {
            YT_LOG_INFO("%v", MakeRange(Chunks_.data(), Chunks_.data() + i));
        }
    }

    void CheckLongMessages()
    {
        TLogManager::Get()->Synchronize();

        auto infoLog = ReadFile("test.log");
        EXPECT_EQ(N + 1, infoLog.size());
        for (int i = 0; i < N; ++i) {
            auto expected = Format("%v", MakeRange(Chunks_.data(), Chunks_.data() + i));
            auto actual = infoLog[i + 1];
            EXPECT_NE(TString::npos, actual.find(expected));
        }

        NFs::Remove("test.log");
    }
};

TEST_F(TLongMessagesLoggingTest, WithPerThreadCache)
{
    ConfigureForLongMessages();
    LogLongMessages();
    CheckLongMessages();
}

TEST_F(TLongMessagesLoggingTest, WithoutPerThreadCache)
{
    ConfigureForLongMessages();
    using TThis = typename std::remove_reference<decltype(*this)>::type;
    TThread thread([] (void* opaque) -> void* {
        auto this_ = static_cast<TThis*>(opaque);
        NLogging::NDetail::TMessageStringBuilder::DisablePerThreadCache();
        this_->LogLongMessages();
        return nullptr;
    }, this);
    thread.Start();
    thread.Join();
    CheckLongMessages();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NLogging
