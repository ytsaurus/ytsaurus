#include <yt/yt/server/controller_agent/config.h>

#include <yt/yt/server/controller_agent/controllers/journal_nbd_helpers.h>

#include <yt/yt/ytlib/scheduler/config.h>

#include <yt/yt/core/test_framework/framework.h>

#include <limits>

namespace NYT::NControllerAgent::NControllers {
namespace {

////////////////////////////////////////////////////////////////////////////////

NScheduler::TNbdDiskRequestPtr MakeDiskRequest()
{
    auto request = New<NScheduler::TNbdDiskRequest>();
    request->Account = "tmp";
    request->MediumName = "default";
    request->DiskSpace = 64_MBs + 1;
    return request;
}

TEST(TJournalNbdHelpersTest, InvalidBlockSize)
{
    auto options = New<TJournalNbdOptions>();
    auto request = MakeDiskRequest();

    for (i64 blockSize : {std::numeric_limits<i64>::min(), i64(-1), i64(0), i64(1536), 8_KBs}) {
        options->BlockSize = blockSize;
        EXPECT_THROW(ResolveJournalNbdSpec(options, request), std::exception);
    }
}

TEST(TJournalNbdHelpersTest, TruncateDeviceSize)
{
    auto options = New<TJournalNbdOptions>();
    auto request = MakeDiskRequest();

    for (i64 blockSize : {1_KBs, 2_KBs, 4_KBs}) {
        options->BlockSize = blockSize;
        auto spec = ResolveJournalNbdSpec(options, request);
        EXPECT_EQ(spec.DeviceSize, 64_MBs);
        EXPECT_EQ(spec.BlockSize, blockSize);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NControllerAgent::NControllers
