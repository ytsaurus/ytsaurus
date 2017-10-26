#include <yt/core/test_framework/framework.h>

#include <yt/core/logging/log.h>
#include <yt/core/logging/log_manager.h>
#include <yt/core/logging/writer.h>

#include <yt/core/ytree/convert.h>

#include <util/system/fs.h>

#ifdef _unix_
#include <unistd.h>
#endif

namespace NYT {
namespace NLogging {

using namespace NYTree;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

class TLoggingTest
    : public ::testing::Test
{
public:
    TLoggingTest()
        : SomeDate("2014-04-24 23:41:09,804")
        , DateLength(SomeDate.length())
        , Logger("Test")
    { }

protected:
    TString SomeDate;
    int DateLength;

    TLogger Logger;

    void WriteEvent(ILogWriter* writer)
    {
        TLoggingCategory category;
        category.Name = "category";

        TLogEvent event;
        event.Category = &category;
        event.Level = ELogLevel::Debug;
        event.Message = "message";
        event.ThreadId = 0xba;

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
    LOG_INFO("Prepaing logging thread");
    sleep(1.0); // In sleep() we trust.

    int version = TLogManager::Get()->GetVersion();

    kill(getpid(), SIGHUP);

    LOG_INFO("Awaking logging thread");
    sleep(1.0); // In sleep() we trust.

    int newVersion = TLogManager::Get()->GetVersion();

    EXPECT_NE(version, newVersion);
}

#endif

TEST_F(TLoggingTest, FileWriter)
{
    NFs::Remove("test.log");

    auto writer = New<TFileLogWriter>("test.log");
    WriteEvent(writer.Get());

    {
        auto lines = ReadFile("test.log");
        EXPECT_EQ(2, lines.size());
        EXPECT_TRUE(lines[0].find("Logging started") != -1);
        EXPECT_EQ("\tD\tcategory\tmessage\tba\t\t\n", lines[1].substr(DateLength, lines[1].size()));
    }

    writer->Reload();
    WriteEvent(writer.Get());

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
    auto writer = New<TStreamLogWriter>(&stringOutput);

    WriteEvent(writer.Get());

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

    EXPECT_TRUE(rule->IsApplicable("some_service"));
    EXPECT_FALSE(rule->IsApplicable("bus"));
    EXPECT_FALSE(rule->IsApplicable("bus", ELogLevel::Debug));
    EXPECT_FALSE(rule->IsApplicable("some_service", ELogLevel::Debug));
    EXPECT_TRUE(rule->IsApplicable("some_service", ELogLevel::Warning));
    EXPECT_TRUE(rule->IsApplicable("some_service", ELogLevel::Info));
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

    sleep(1.0);

    auto infoLog = ReadFile("test.log");
    auto errorLog = ReadFile("test.error.log");

    EXPECT_EQ(3, infoLog.size());
    EXPECT_EQ(2, errorLog.size());

    NFs::Remove("test.log");
    NFs::Remove("test.error.log");
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

    sleep(1);

    LOG_INFO("Info message");
    LOG_FATAL("FATAL");

    NFs::Remove("test.log");
    NFs::Remove("test.error.log");
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NLogging
} // namespace NYT
