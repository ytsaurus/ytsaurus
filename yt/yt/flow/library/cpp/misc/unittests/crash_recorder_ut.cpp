#include <yt/yt/flow/library/cpp/misc/crash_recorder.h>

#include <yt/yt/core/misc/crash_handler.h>

#include <yt/yt/core/test_framework/framework.h>

#include <util/random/random.h>

namespace NYT::NFlow {
namespace {

////////////////////////////////////////////////////////////////////////////////

TEST(TCrashRecorderTest, Basic)
{
    // Other tests can call SetRandomSeed, so TGuid::Create can produce the same value across runs. So fix random state.
    ResetRandomState();

    auto crashText = TStringBuf("crash text");
    auto fingerprint = ToString(TGuid::Create());

    SetupCrashRecording(fingerprint);
    EXPECT_TRUE(GetPreviousCrashError().IsOK());
    WriteCrashHandlerOutput(crashText);
    EXPECT_TRUE(GetPreviousCrashError().IsOK());

    // As in the new process.
    SetupCrashRecording(fingerprint);
    auto error = GetPreviousCrashError();
    EXPECT_FALSE(error.IsOK());
    EXPECT_EQ(error.Attributes().Get<std::string>("crash_report"), crashText);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow
