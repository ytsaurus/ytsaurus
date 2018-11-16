#include <yt/core/test_framework/framework.h>

#include <yt/core/logging/log.h>
#include <yt/core/logging/log_manager.h>
#include <yt/core/logging/writer.h>

#include <yt/core/json/json_parser.h>

#include <yt/core/ytree/fluent.h>

#include <yt/core/ytree/convert.h>

#include <util/system/fs.h>

#ifdef _unix_
#include <unistd.h>
#endif

namespace NYT {
namespace NLogging {

using namespace NYTree;
using namespace NYson;
using namespace NJson;

////////////////////////////////////////////////////////////////////////////////

class TLoggingTest
    : public ::testing::Test
{
public:
    TLoggingTest()
        : SomeDate("2014-04-24 23:41:09,804")
        , DateLength(SomeDate.length())
        , Logger("Test")
    {
        Category.Name = "category";
    }

protected:
    TLoggingCategory Category;
    TString SomeDate;
    int DateLength;

    TLogger Logger;

    IMapNodePtr DeserializeJson(const TString& source)
    {
        auto builder = CreateBuilderFromFactory(GetEphemeralNodeFactory());
        builder->BeginTree();
        TStringStream stream(source);
        ParseJson(&stream, builder.get());
        return builder->EndTree()->AsMap();
    }

    void WritePlainTextEvent(ILogWriter* writer) {
        TLogEvent event;
        event.MessageFormat = ELogMessageFormat::PlainText;
        event.Category = &Category;
        event.Level = ELogLevel::Debug;
        event.Message = "message";
        event.ThreadId = 0xba;

        WriteEvent(writer, event);
    }

    void WriteEvent(ILogWriter* writer, const TLogEvent& event)
    {
        writer->Write(event);
        writer->Flush();
    }

    std::vector<TString> ReadFile(const TString& fileName)
    {
        std::vector<TString> lines;

        TString line;
        auto input = TUnbufferedFileInput(fileName);
        while (input.ReadLine(line)) {
            lines.push_back(line + "\n");
        }

        return lines;
    }
};

#ifndef _win_

TEST_F(TLoggingTest, ReloadsOnSigHup)
{
    LOG_INFO("Preparing logging thread");
    Sleep(TDuration::MilliSeconds(100)); // In sleep() we trust.

    int version = TLogManager::Get()->GetVersion();

    kill(getpid(), SIGHUP);

    LOG_INFO("Awaking logging thread");
    Sleep(TDuration::Seconds(1)); // In sleep() we trust.

    int newVersion = TLogManager::Get()->GetVersion();

    EXPECT_NE(version, newVersion);
}

#endif

TEST_F(TLoggingTest, FileWriter)
{
    NFs::Remove("test.log");

    TIntrusivePtr<TFileLogWriter> writer;
    writer = New<TFileLogWriter>(std::make_unique<TPlainTextLogFormatter>(), "test_writer", "test.log");
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

    auto configText = R"({
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
    })";

    auto configNode = ConvertToNode(TYsonString(configText));

    auto config = ConvertTo<TLogConfigPtr>(configNode);

    TLogManager::Get()->Configure(config);

    LOG_DEBUG("Debug message");
    LOG_INFO("Info message");
    LOG_ERROR("Error message");

    Sleep(TDuration::Seconds(1));

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

    auto writer = New<TFileLogWriter>(std::make_unique<TJsonLogFormatter>(), "test_writer", "test.log");
    WriteEvent(writer.Get(), event);
    Sleep(TDuration::MilliSeconds(100));

    auto log = ReadFile("test.log");

    auto logStartedJson = DeserializeJson(log[0]);
    EXPECT_EQ(logStartedJson->GetChild("message")->AsString()->GetValue(), "Logging started");
    EXPECT_EQ(logStartedJson->GetChild("level")->AsString()->GetValue(), "info");
    EXPECT_EQ(logStartedJson->GetChild("category")->AsString()->GetValue(), "Logging");

    auto contentJson = DeserializeJson(log[1]);
    EXPECT_EQ(contentJson->GetChild("message")->AsString()->GetValue(), "test_message");
    EXPECT_EQ(contentJson->GetChild("level")->AsString()->GetValue(), "debug");
    EXPECT_EQ(contentJson->GetChild("category")->AsString()->GetValue(), "category");

    NFs::Remove("test.log");
}

// This test is for manual check of LOG_FATAL
TEST_F(TLoggingTest, DISABLED_LogFatal)
{
    NFs::Remove("test.log");
    NFs::Remove("test.error.log");

    auto configText = R"({
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
    })";

    auto configNode = ConvertToNode(TYsonString(configText));

    auto config = ConvertTo<TLogConfigPtr>(configNode);

    TLogManager::Get()->Configure(config);

    LOG_INFO("Info message");

    Sleep(TDuration::MilliSeconds(100));

    LOG_INFO("Info message");
    LOG_FATAL("FATAL");

    NFs::Remove("test.log");
    NFs::Remove("test.error.log");
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NLogging
} // namespace NYT
