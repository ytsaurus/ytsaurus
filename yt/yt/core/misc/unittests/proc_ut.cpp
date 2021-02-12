#include <yt/core/test_framework/framework.h>

#include <yt/core/misc/proc.h>

namespace NYT {
namespace {

////////////////////////////////////////////////////////////////////////////////

TEST(TProcTest, TestParseMemoryMappings)
{
    const TString rawSMaps =
        "7fbb7b24d000-7fbb7b251000 rw-s 00000000 00:00 0 \n"
        "Size:                  1 kB\n"
        "KernelPageSize:        2 kB\n"
        "MMUPageSize:           3 kB\n"
        "Rss:                   4 kB\n"
        "Pss:                   5 kB\n"
        "Shared_Clean:          6 kB\n"
        "Shared_Dirty:          7 kB\n"
        "Private_Clean:         8 kB\n"
        "Private_Dirty:         9 kB\n"
        "Referenced:           10 kB\n"
        "Anonymous:            11 kB\n"
        "LazyFree:             12 kB\n"
        "AnonHugePages:        13 kB\n"
        "ShmemPmdMapped:       14 kB\n"
        "Shared_Hugetlb:       15 kB\n"
        "Private_Hugetlb:      16 kB\n"
        "Swap:                 17 kB\n"
        "SwapPss:              18 kB\n"
        "Locked:               19 kB\n"
        "ProtectionKey:        20\n"
        "VmFlags: rd wr mg\n"
        "7fbb7b251000-7fbb7b278000 r-xp 000000ff 00:13d 406536                     /lib/x86_64-linux-gnu/ld-2.28.so (deleted)\n"
        "Size:                156 kB\n"
        "KernelPageSize:        4 kB\n"
        "MMUPageSize:           4 kB\n"
        "ProtectionKey:         0\n"
        "VmFlags: \n";

    auto smaps = ParseMemoryMappings(rawSMaps);

    EXPECT_EQ(smaps.size(), 2);

    EXPECT_EQ(smaps[0].Start, 0x7fbb7b24d000);
    EXPECT_EQ(smaps[0].End, 0x7fbb7b251000);
    EXPECT_EQ(smaps[0].Permissions, EMemoryMappingPermission::Read | EMemoryMappingPermission::Write | EMemoryMappingPermission::Shared);
    EXPECT_EQ(smaps[0].Offset, 0);
    EXPECT_EQ(static_cast<bool>(smaps[0].DeviceId), false);
    EXPECT_EQ(static_cast<bool>(smaps[0].INode), false);
    EXPECT_EQ(static_cast<bool>(smaps[0].Path), false);
    EXPECT_EQ(smaps[0].Statistics.Size, 1_KB);
    EXPECT_EQ(smaps[0].Statistics.KernelPageSize, 2_KB);
    EXPECT_EQ(smaps[0].Statistics.MMUPageSize, 3_KB);
    EXPECT_EQ(smaps[0].Statistics.Rss, 4_KB);
    EXPECT_EQ(smaps[0].Statistics.Pss, 5_KB);
    EXPECT_EQ(smaps[0].Statistics.SharedClean, 6_KB);
    EXPECT_EQ(smaps[0].Statistics.SharedDirty, 7_KB);
    EXPECT_EQ(smaps[0].Statistics.PrivateClean, 8_KB);
    EXPECT_EQ(smaps[0].Statistics.PrivateDirty, 9_KB);
    EXPECT_EQ(smaps[0].Statistics.Referenced, 10_KB);
    EXPECT_EQ(smaps[0].Statistics.Anonymous, 11_KB);
    EXPECT_EQ(smaps[0].Statistics.LazyFree, 12_KB);
    EXPECT_EQ(smaps[0].Statistics.AnonHugePages, 13_KB);
    EXPECT_EQ(smaps[0].Statistics.ShmemPmdMapped, 14_KB);
    EXPECT_EQ(smaps[0].Statistics.SharedHugetlb, 15_KB);
    EXPECT_EQ(smaps[0].Statistics.PrivateHugetlb, 16_KB);
    EXPECT_EQ(smaps[0].Statistics.Swap, 17_KB);
    EXPECT_EQ(smaps[0].Statistics.SwapPss, 18_KB);
    EXPECT_EQ(smaps[0].Statistics.Locked, 19_KB);
    EXPECT_EQ(smaps[0].ProtectionKey, 20);
    EXPECT_EQ(smaps[0].VMFlags, EVMFlag::RD | EVMFlag::WR | EVMFlag::MG);

    EXPECT_EQ(smaps[1].Start, 0x7fbb7b251000);
    EXPECT_EQ(smaps[1].End, 0x7fbb7b278000);
    EXPECT_EQ(smaps[1].Permissions, EMemoryMappingPermission::Read | EMemoryMappingPermission::Execute | EMemoryMappingPermission::Private);
    EXPECT_EQ(smaps[1].Offset, 0xff);
    EXPECT_EQ(smaps[1].DeviceId, 1048637);
    EXPECT_EQ(*smaps[1].INode, 406536);
    EXPECT_EQ(*smaps[1].Path, "/lib/x86_64-linux-gnu/ld-2.28.so");
    EXPECT_EQ(smaps[1].Statistics.Size, 156_KB);
    EXPECT_EQ(smaps[1].Statistics.KernelPageSize, 4_KB);
    EXPECT_EQ(smaps[1].Statistics.MMUPageSize, 4_KB);
    EXPECT_EQ(smaps[1].Statistics.Rss, 0_KB);
    EXPECT_EQ(smaps[1].ProtectionKey, 0);
    EXPECT_EQ(smaps[1].VMFlags, EVMFlag::None);
}

TEST(TProcTest, TestGetSelfMemoryMappings)
{
    auto pid = GetCurrentProcessId();
    auto memoryMappings = GetProcessMemoryMappings(pid);

    TMemoryMappingStatistics statistics;
    for (const auto& mapping : memoryMappings) {
        statistics += mapping.Statistics;
    }

    auto memoryUsage = GetProcessMemoryUsage();

    // Memory usage could change slightly between measurings.
    EXPECT_LE(statistics.Rss, 1.1 * memoryUsage.Rss);
    EXPECT_GE(statistics.Rss, 0.9 * memoryUsage.Rss);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT
