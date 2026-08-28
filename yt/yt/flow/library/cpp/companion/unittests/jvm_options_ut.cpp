#include <yt/yt/core/test_framework/framework.h>
#include <yt/yt/flow/library/cpp/companion/jvm_options.h>

#include <util/system/env.h>

namespace NYT::NFlow::NCompanion {
namespace {

////////////////////////////////////////////////////////////////////////////////

class TJvmOptionsTest : public ::testing::Test
{
protected:
    void TearDown() override
    {
        SetEnv("YT_FLOW_COMPANION_JVM_EXTRA_OPTS", "");
        SetEnv("YT_FLOW_COMPANION_LOG_DIR", "");
        SetEnv("YT_FLOW_COMPANION_JFR_DISABLED", "");
        SetEnv("YT_FLOW_COMPANION_JFR_OPTS", "");
        SetEnv("YT_FLOW_COMPANION_GC_LOG_DISABLED", "");
        SetEnv("YT_FLOW_COMPANION_GC_LOG_OPTS", "");
    }
};

////////////////////////////////////////////////////////////////////////////////

TEST_F(TJvmOptionsTest, ResolveDefaultsContainAllGroups)
{
    auto result = ResolveJvmOptions();
    bool hasJfr = false;
    bool hasGcLog = false;
    bool hasExitOnOom = false;
    for (const auto& opt : result) {
        if (opt.find("FlightRecording") != std::string::npos) {
            hasJfr = true;
        }
        if (opt.find("-Xlog:gc") != std::string::npos) {
            hasGcLog = true;
        }
        if (opt.find("ExitOnOutOfMemoryError") != std::string::npos) {
            hasExitOnOom = true;
        }
    }
    EXPECT_TRUE(hasJfr) << "Default JFR options missing";
    EXPECT_TRUE(hasGcLog) << "Default GC log options missing";
    EXPECT_TRUE(hasExitOnOom) << "ExitOnOutOfMemoryError option missing";
}

TEST_F(TJvmOptionsTest, ResolveExtraOptsAppendsToDefaults)
{
    SetEnv("YT_FLOW_COMPANION_JVM_EXTRA_OPTS", "-Xmx1g -Dfoo=bar");
    auto result = ResolveJvmOptions();
    ASSERT_GE(result.size(), 2u);
    EXPECT_EQ("-Xmx1g", result[result.size() - 2]);
    EXPECT_EQ("-Dfoo=bar", result[result.size() - 1]);
}

TEST_F(TJvmOptionsTest, ResolveUsesCustomLogDir)
{
    SetEnv("YT_FLOW_COMPANION_LOG_DIR", "/tmp/custom_logs");
    auto options = ResolveJvmOptions();
    bool found = false;
    for (const auto& opt : options) {
        if (opt.find("/tmp/custom_logs") != std::string::npos) {
            found = true;
            break;
        }
    }
    EXPECT_TRUE(found) << "Custom log dir not found in any JVM option";
}

TEST_F(TJvmOptionsTest, JfrDisabled)
{
    SetEnv("YT_FLOW_COMPANION_JFR_DISABLED", "1");
    auto options = ResolveJvmOptions();
    for (const auto& opt : options) {
        EXPECT_EQ(std::string::npos, opt.find("FlightRecording")) << "JFR option found when YT_FLOW_COMPANION_JFR_DISABLED=1: " << opt;
        EXPECT_EQ(std::string::npos, opt.find("FlightRecorderOptions")) << "JFR option found when YT_FLOW_COMPANION_JFR_DISABLED=1: " << opt;
        EXPECT_EQ(std::string::npos, opt.find("DebugNonSafepoints")) << "JFR option found when YT_FLOW_COMPANION_JFR_DISABLED=1: " << opt;
    }

    bool hasExitOnOom = false;
    for (const auto& opt : options) {
        if (opt.find("ExitOnOutOfMemoryError") != std::string::npos) {
            hasExitOnOom = true;
            break;
        }
    }
    EXPECT_TRUE(hasExitOnOom) << "ExitOnOutOfMemoryError option missing when only JFR is disabled";
}

TEST_F(TJvmOptionsTest, JfrCustomOpts)
{
    SetEnv("YT_FLOW_COMPANION_JFR_OPTS", "-XX:StartFlightRecording=maxage=1h");
    auto options = ResolveJvmOptions();
    bool hasCustomJfr = false;
    bool hasDefaultJfr = false;
    for (const auto& opt : options) {
        if (opt == "-XX:StartFlightRecording=maxage=1h") {
            hasCustomJfr = true;
        }
        if (opt.find("maxage=24h") != std::string::npos) {
            hasDefaultJfr = true;
        }
    }
    EXPECT_TRUE(hasCustomJfr) << "Custom JFR option not found";
    EXPECT_FALSE(hasDefaultJfr) << "Default JFR option should be replaced by custom";
}

TEST_F(TJvmOptionsTest, JfrDisabledTakesPriorityOverCustomOpts)
{
    SetEnv("YT_FLOW_COMPANION_JFR_DISABLED", "1");
    SetEnv("YT_FLOW_COMPANION_JFR_OPTS", "-XX:StartFlightRecording=maxage=1h");
    auto options = ResolveJvmOptions();
    for (const auto& opt : options) {
        EXPECT_EQ(std::string::npos, opt.find("FlightRecording")) << "JFR option found when YT_FLOW_COMPANION_JFR_DISABLED=1: " << opt;
    }
}

TEST_F(TJvmOptionsTest, GcLogDisabled)
{
    SetEnv("YT_FLOW_COMPANION_GC_LOG_DISABLED", "1");
    auto options = ResolveJvmOptions();
    for (const auto& opt : options) {
        EXPECT_EQ(std::string::npos, opt.find("-Xlog:gc")) << "GC log option found when YT_FLOW_COMPANION_GC_LOG_DISABLED=1: " << opt;
    }

    bool hasJfr = false;
    for (const auto& opt : options) {
        if (opt.find("FlightRecording") != std::string::npos) {
            hasJfr = true;
            break;
        }
    }
    EXPECT_TRUE(hasJfr) << "JFR options missing when only GC log is disabled";
}

TEST_F(TJvmOptionsTest, GcLogCustomOpts)
{
    SetEnv("YT_FLOW_COMPANION_GC_LOG_OPTS", "-Xlog:gc:file=/custom/gc.log");
    auto options = ResolveJvmOptions();
    bool hasCustomGc = false;
    bool hasDefaultGc = false;
    for (const auto& opt : options) {
        if (opt == "-Xlog:gc:file=/custom/gc.log") {
            hasCustomGc = true;
        }
        if (opt.find("filecount=10") != std::string::npos) {
            hasDefaultGc = true;
        }
    }
    EXPECT_TRUE(hasCustomGc) << "Custom GC log option not found";
    EXPECT_FALSE(hasDefaultGc) << "Default GC log option should be replaced by custom";
}

TEST_F(TJvmOptionsTest, GcLogDisabledTakesPriorityOverCustomOpts)
{
    SetEnv("YT_FLOW_COMPANION_GC_LOG_DISABLED", "1");
    SetEnv("YT_FLOW_COMPANION_GC_LOG_OPTS", "-Xlog:gc:file=/custom/gc.log");
    auto options = ResolveJvmOptions();
    for (const auto& opt : options) {
        EXPECT_EQ(std::string::npos, opt.find("-Xlog:gc")) << "GC log option found when YT_FLOW_COMPANION_GC_LOG_DISABLED=1: " << opt;
    }
}

TEST_F(TJvmOptionsTest, BothJfrAndGcLogDisabled)
{
    SetEnv("YT_FLOW_COMPANION_JFR_DISABLED", "1");
    SetEnv("YT_FLOW_COMPANION_GC_LOG_DISABLED", "1");
    auto options = ResolveJvmOptions();
    // Only crash/OOM options should remain (3 options).
    ASSERT_EQ(3u, options.size());
    EXPECT_EQ("-XX:+ExitOnOutOfMemoryError", options[0]);
    EXPECT_EQ("-XX:+CreateCoredumpOnCrash", options[1]);
    EXPECT_NE(std::string::npos, options[2].find("hs_err_")) << "ErrorFile option missing";
}

TEST_F(TJvmOptionsTest, UsesDefaultLogDirWhenEnvNotSet)
{
    SetEnv("YT_FLOW_COMPANION_LOG_DIR", "");
    auto options = ResolveJvmOptions();
    bool found = false;
    for (const auto& opt : options) {
        if (opt.find("/logs/") != std::string::npos) {
            found = true;
            break;
        }
    }
    EXPECT_TRUE(found) << "Default log dir (absolute path containing /logs/) not found in any JVM option";
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow::NCompanion
