#include <yt/server/lib/hydra/hydra_janitor_helpers.h>
#include <yt/server/lib/hydra/config.h>

#include <yt/core/test_framework/framework.h>

#include <limits>
#include <vector>

namespace NYT::NHydra {
namespace {

////////////////////////////////////////////////////////////////////////////////

TEST(TJanitorFileThresholdTest, EmptyVector)
{
    ASSERT_EQ(0, ComputeJanitorThresholdId({}, 1, 1));
}

TEST(TJanitorFileThresholdTest, ShortVector)
{
    std::vector<THydraFileInfo> files{
        {1, 10},
        {2, 11}
    };
    ASSERT_EQ(0, ComputeJanitorThresholdId(files, 3, std::nullopt));
}

TEST(TJanitorFileThresholdTest, LongVector)
{
    std::vector<THydraFileInfo> files{
        {1, 10},
        {2, 11},
        {9, 15}
    };
    ASSERT_EQ(2, ComputeJanitorThresholdId(files, 2, std::nullopt));
}

TEST(TJanitorFileThresholdTest, SmallSize)
{
    std::vector<THydraFileInfo> files{
        {1, 10},
        {2, 3},
        {3, 7}
    };
    ASSERT_LE(ComputeJanitorThresholdId(files, std::nullopt, 30), 1);
}

TEST(TJanitorFileThresholdTest, BigSize)
{
    std::vector<THydraFileInfo> files{
        {1, 10},
        {2, 4},
        {3, 15},
        {4, 7}
    };
    ASSERT_EQ(3, ComputeJanitorThresholdId(files, std::nullopt, 25));
}

TEST(TJanitorFileThresholdTest, AccurateSize)
{
    std::vector<THydraFileInfo> files{
        {1, 10},
        {2, 11},
        {3, 13},
        {4, 14}
    };
    ASSERT_EQ(2, ComputeJanitorThresholdId(files, std::nullopt, 38));
}

TEST(TJanitorFileThresholdTest, CountStronger)
{
    std::vector<THydraFileInfo> files{
        {1, 10},
        {2, 11},
        {3, 13},
        {4, 14}
    };
    ASSERT_EQ(4, ComputeJanitorThresholdId(files, 1, 30));
}

TEST(TJanitorFileThresholdTest, SizeStronger)
{
    std::vector<THydraFileInfo> files{
        {1, 10},
        {2, 11},
        {3, 13},
        {4, 14}
    };
    ASSERT_EQ(4, ComputeJanitorThresholdId(files, 3, 15));
}

TEST(TJanitorFileThresholdTest, ZeroCount)
{
    std::vector<THydraFileInfo> files{
        {1, 10},
        {2, 11},
        {3, 13}
    };
    ASSERT_EQ(3, ComputeJanitorThresholdId(files, 0, std::nullopt));
}

TEST(TJanitorFileThresholdTest, ZeroSize)
{
    std::vector<THydraFileInfo> files{
        {1, 10},
        {2, 11},
        {3, 13}
    };
    ASSERT_EQ(3, ComputeJanitorThresholdId(files, std::nullopt, 0));
}

////////////////////////////////////////////////////////////////////////////////

TEST(TJanitorSnapshotChangelogTest, TooManyChangelogs)
{
    std::vector<THydraFileInfo> snapshots{
        {3, 0}
    };
    std::vector<THydraFileInfo> changelogs{
        {1, 0},
        {2, 0},
        {3, 0}
    };
    auto config = New<THydraJanitorConfig>();
    config->MaxChangelogCountToKeep = 2;
    ASSERT_EQ(2, ComputeJanitorThresholdId(snapshots, changelogs, config));
}

TEST(TJanitorSnapshotChangelogTest, TooMuchChangelogSpace)
{
    std::vector<THydraFileInfo> snapshots{
        {3, 0}
    };
    std::vector<THydraFileInfo> changelogs{
        {1, 100},
        {2, 200},
        {3, 300}
    };
    auto config = New<THydraJanitorConfig>();
    config->MaxChangelogSizeToKeep = 550;
    ASSERT_EQ(2, ComputeJanitorThresholdId(snapshots, changelogs, config));
}

TEST(TJanitorSnapshotChangelogTest, TooManySnapshots)
{
    std::vector<THydraFileInfo> snapshots{
        {1, 0},
        {2, 0},
        {3, 0}
    };
    std::vector<THydraFileInfo> changelogs{
        {3, 0}
    };
    auto config = New<THydraJanitorConfig>();
    config->MaxSnapshotCountToKeep = 2;
    ASSERT_EQ(2, ComputeJanitorThresholdId(snapshots, changelogs, config));
}

TEST(TJanitorSnapshotChangelogTest, TooMuchSnapshotSpace)
{
    std::vector<THydraFileInfo> snapshots{
        {1, 100},
        {2, 200},
        {3, 300}
    };
    std::vector<THydraFileInfo> changelogs{
        {3, 0}
    };
    auto config = New<THydraJanitorConfig>();
    config->MaxSnapshotSizeToKeep = 550;
    ASSERT_EQ(2, ComputeJanitorThresholdId(snapshots, changelogs, config));
}

TEST(TJanitorSnapshotChangelogTest, NoChangelogCountCleanupPastLastSnapshot)
{
    std::vector<THydraFileInfo> snapshots{
        {1, 0}
    };
    std::vector<THydraFileInfo> changelogs{
        {1, 0},
        {2, 0},
        {3, 0},
    };
    auto config = New<THydraJanitorConfig>();
    config->MaxChangelogCountToKeep = 1;
    ASSERT_EQ(1, ComputeJanitorThresholdId(snapshots, changelogs, config));
}

TEST(TJanitorSnapshotChangelogTest, NoChangelogSizeCleanupPastLastSnapshot)
{
    std::vector<THydraFileInfo> snapshots{
        {1, 0}
    };
    std::vector<THydraFileInfo> changelogs{
        {1, 100},
        {2, 200},
        {3, 300},
    };
    auto config = New<THydraJanitorConfig>();
    config->MaxChangelogSizeToKeep = 350;
    ASSERT_EQ(1, ComputeJanitorThresholdId(snapshots, changelogs, config));
}

TEST(TJanitorSnapshotChangelogTest, NoSnapshotSizeCleanupPastLastSnapshot)
{
    std::vector<THydraFileInfo> snapshots{
        {1, 100},
        {2, 200},
        {3, 300}
    };
    std::vector<THydraFileInfo> changelogs{};
    auto config = New<THydraJanitorConfig>();
    config->MaxSnapshotSizeToKeep = 200;
    ASSERT_EQ(3, ComputeJanitorThresholdId(snapshots, changelogs, config));
}

TEST(TJanitorSnapshotChangelogTest, NoSnapshotsPresent)
{
    std::vector<THydraFileInfo> snapshots{};
    std::vector<THydraFileInfo> changelogs{
        {1, 100},
        {2, 200},
        {3, 300}
    };
    auto config = New<THydraJanitorConfig>();
    config->MaxChangelogCountToKeep = 2;
    ASSERT_EQ(0, ComputeJanitorThresholdId(snapshots, changelogs, config));
}

TEST(TJanitorSnapshotChangelogTest, JustMaxSnapshotCountToKeep1)
{
    std::vector<THydraFileInfo> snapshots{
        {1, 0},
        {2, 0},
        {3, 0}
    };
    std::vector<THydraFileInfo> changelogs{
        {1, 0},
        {2, 0},
        {3, 0}
    };
    auto config = New<THydraJanitorConfig>();
    config->MaxSnapshotCountToKeep = 2;
    ASSERT_EQ(2, ComputeJanitorThresholdId(snapshots, changelogs, config));
}

TEST(TJanitorSnapshotChangelogTest, JustMaxSnapshotCountToKeep2)
{
    std::vector<THydraFileInfo> snapshots{
        {1, 0},
        {2, 0},
        {3, 0},
        {4, 0},
        {5, 0}
    };
    std::vector<THydraFileInfo> changelogs{};
    auto config = New<THydraJanitorConfig>();
    config->MaxSnapshotCountToKeep = 3;
    ASSERT_EQ(3, ComputeJanitorThresholdId(snapshots, changelogs, config));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NHydra
