#include <yt/yt/library/tcmalloc/config.h>
#include <yt/yt/library/tcmalloc/memory_profile_retention.h>

#include <yt/yt/core/misc/fs.h>

#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/yson/string.h>

#include <yt/yt/core/ytree/convert.h>

#include <util/folder/tempdir.h>

#include <util/stream/file.h>

#include <util/system/fstat.h>
#include <util/system/utime.h>

namespace NYT::NTCMalloc {
namespace {

////////////////////////////////////////////////////////////////////////////////

class TMemoryProfileRetentionTest
    : public ::testing::Test
{
protected:
    const TTempDir Directory_;
    const TInstant Now_ = TInstant::Seconds(2'000'000'000);

    std::string GetPath(const std::string& fileName) const
    {
        return NFS::CombinePaths(Directory_.Name(), fileName);
    }

    void CreateFile(const std::string& fileName, TDuration age, int size = 1)
    {
        CreateFileAt(fileName, Now_ - age, size);
    }

    void CreateFileAt(const std::string& fileName, TInstant modificationTime, int size = 1)
    {
        auto path = GetPath(fileName);
        TFileOutput output{TString(path)};
        auto contents = std::string(size, 'x');
        output.Write(contents.data(), contents.size());
        output.Finish();

        auto modificationTimeSeconds = static_cast<time_t>(modificationTime.Seconds());
        ASSERT_EQ(SetModTime(path.c_str(), modificationTimeSeconds, modificationTimeSeconds), 0);
    }

    void CreateDump(const std::string& suffix, TDuration age, int profileSize = 1)
    {
        CreateFile(
            Format("%v%v%v", CurrentMemoryProfileFilePrefix, suffix, MemoryProfileFileExtension),
            age,
            profileSize);
        CreateFile(
            Format("%v%v%v", PeakMemoryProfileFilePrefix, suffix, MemoryProfileFileExtension),
            age,
            profileSize);
        CreateFile(
            Format("%v%v%v", OomMemoryProfileManifestFilePrefix, suffix, OomMemoryProfileManifestFileExtension),
            age);
    }

    bool Exists(const std::string& fileName) const
    {
        return NFS::Exists(GetPath(fileName));
    }

    void ExpectDumpExists(const std::string& suffix, bool expected) const
    {
        EXPECT_EQ(
            Exists(Format("%v%v%v", CurrentMemoryProfileFilePrefix, suffix, MemoryProfileFileExtension)),
            expected);
        EXPECT_EQ(
            Exists(Format("%v%v%v", PeakMemoryProfileFilePrefix, suffix, MemoryProfileFileExtension)),
            expected);
        EXPECT_EQ(
            Exists(Format(
                "%v%v%v",
                OomMemoryProfileManifestFilePrefix,
                suffix,
                OomMemoryProfileManifestFileExtension)),
            expected);
    }

    TMemoryProfileRetentionConfigPtr MakeConfig() const
    {
        return New<TMemoryProfileRetentionConfig>();
    }
};

TEST_F(TMemoryProfileRetentionTest, KeepsNewestDumpsByCount)
{
    CreateDump("20260101T000000", TDuration::Hours(3));
    CreateDump("20260101T010000", TDuration::Hours(2));
    CreateDump("20260101T020000", TDuration::Hours(1));
    CreateFile("unrelated", TDuration::Days(30));

    auto config = MakeConfig();
    config->MaxDumpCount = 2;
    auto result = CleanupMemoryProfiles(Directory_.Name(), std::nullopt, config, Now_);

    EXPECT_EQ(result.RemovedDumpCount, 1);
    ExpectDumpExists("20260101T000000", false);
    ExpectDumpExists("20260101T010000", true);
    ExpectDumpExists("20260101T020000", true);
    EXPECT_TRUE(Exists("unrelated"));
}

TEST_F(TMemoryProfileRetentionTest, RemovesExpiredDumps)
{
    CreateDump("20260101T000000", TDuration::Days(11));
    CreateDump("20260102T000000", TDuration::Days(10));

    auto config = MakeConfig();
    config->MaxDumpAge = TDuration::Days(7);
    auto result = CleanupMemoryProfiles(Directory_.Name(), std::nullopt, config, Now_);

    EXPECT_EQ(result.RemovedDumpCount, 2);
    ExpectDumpExists("20260101T000000", false);
    ExpectDumpExists("20260102T000000", false);
}

TEST_F(TMemoryProfileRetentionTest, KeepsNewestDumpWhenTotalSizeExceeded)
{
    CreateDump("20260101T000000", TDuration::Hours(2), 10);
    CreateDump("20260101T010000", TDuration::Hours(1), 10);

    auto config = MakeConfig();
    config->MaxTotalSize = 5;
    auto result = CleanupMemoryProfiles(Directory_.Name(), std::nullopt, config, Now_);

    EXPECT_EQ(result.RemovedDumpCount, 1);
    ExpectDumpExists("20260101T000000", false);
    ExpectDumpExists("20260101T010000", true);
}

TEST_F(TMemoryProfileRetentionTest, RemovesOnlyOldOrphans)
{
    CreateFile("current_old.pb.gz", TDuration::Days(2));
    CreateFile("peak_recent.pb.gz", TDuration::Hours(1));
    CreateFile("current_incomplete.pb.gz_incomplete", TDuration::Days(2));
    CreateFile("oom_profile_paths_old.yson_incomplete", TDuration::Days(2));
    CreateFileAt("peak_future.pb.gz", Now_ + TDuration::Days(2));
    CreateFile("unrelated", TDuration::Days(2));

    auto config = MakeConfig();
    auto result = CleanupMemoryProfiles(Directory_.Name(), std::nullopt, config, Now_);

    EXPECT_EQ(result.RemovedOrphanFileCount, 3);
    EXPECT_FALSE(Exists("current_old.pb.gz"));
    EXPECT_TRUE(Exists("peak_recent.pb.gz"));
    EXPECT_FALSE(Exists("current_incomplete.pb.gz_incomplete"));
    EXPECT_FALSE(Exists("oom_profile_paths_old.yson_incomplete"));
    EXPECT_TRUE(Exists("peak_future.pb.gz"));
    EXPECT_TRUE(Exists("unrelated"));
}

TEST_F(TMemoryProfileRetentionTest, FiltersByFilenameSuffix)
{
    CreateDump("first_20260101T000000", TDuration::Hours(2));
    CreateDump("first_20260101T010000", TDuration::Hours(1));
    CreateDump("second_20260101T000000", TDuration::Hours(2));
    CreateDump("second_20260101T010000", TDuration::Hours(1));

    auto config = MakeConfig();
    config->MaxDumpCount = 1;
    auto result = CleanupMemoryProfiles(Directory_.Name(), "first", config, Now_);

    EXPECT_EQ(result.RemovedDumpCount, 1);
    ExpectDumpExists("first_20260101T000000", false);
    ExpectDumpExists("first_20260101T010000", true);
    ExpectDumpExists("second_20260101T000000", true);
    ExpectDumpExists("second_20260101T010000", true);
}

TEST_F(TMemoryProfileRetentionTest, DoesNotMatchLongerFilenameSuffix)
{
    CreateDump("node_20260101T000000", TDuration::Days(3));
    CreateDump("node_1_20260101T000000", TDuration::Days(3));
    CreateFile("current_node_1_20260102T000000.pb.gz_incomplete", TDuration::Days(3));

    auto config = MakeConfig();
    config->MaxDumpAge = TDuration::Days(1);
    auto result = CleanupMemoryProfiles(Directory_.Name(), "node", config, Now_);

    EXPECT_EQ(result.RemovedDumpCount, 1);
    EXPECT_EQ(result.RemovedOrphanFileCount, 0);
    ExpectDumpExists("node_20260101T000000", false);
    ExpectDumpExists("node_1_20260101T000000", true);
    EXPECT_TRUE(Exists("current_node_1_20260102T000000.pb.gz_incomplete"));

    result = CleanupMemoryProfiles(Directory_.Name(), "node_1", config, Now_);
    EXPECT_EQ(result.RemovedDumpCount, 1);
    EXPECT_EQ(result.RemovedOrphanFileCount, 1);
    ExpectDumpExists("node_1_20260101T000000", false);
    EXPECT_FALSE(Exists("current_node_1_20260102T000000.pb.gz_incomplete"));
}

TEST_F(TMemoryProfileRetentionTest, UnsuffixedInstanceIgnoresSuffixedFiles)
{
    CreateDump("20260101T000000", TDuration::Days(3));
    CreateDump("node_20260101T000000", TDuration::Days(3));
    CreateFile("peak_node_20260102T000000.pb.gz", TDuration::Days(3));
    CreateFile("current_node_20260102T000000.pb.gz_incomplete", TDuration::Days(3));

    auto config = MakeConfig();
    config->MaxDumpAge = TDuration::Days(1);
    auto result = CleanupMemoryProfiles(Directory_.Name(), std::nullopt, config, Now_);

    EXPECT_EQ(result.RemovedDumpCount, 1);
    EXPECT_EQ(result.RemovedOrphanFileCount, 0);
    ExpectDumpExists("20260101T000000", false);
    ExpectDumpExists("node_20260101T000000", true);
    EXPECT_TRUE(Exists("peak_node_20260102T000000.pb.gz"));
    EXPECT_TRUE(Exists("current_node_20260102T000000.pb.gz_incomplete"));
}

TEST_F(TMemoryProfileRetentionTest, KeepsDumpsAtTotalSizeLimit)
{
    CreateDump("20260101T000000", TDuration::Hours(3), 10);
    CreateDump("20260101T010000", TDuration::Hours(2), 10);
    CreateDump("20260101T020000", TDuration::Hours(1), 10);

    auto config = MakeConfig();
    config->MaxTotalSize = 42;
    auto result = CleanupMemoryProfiles(Directory_.Name(), std::nullopt, config, Now_);

    EXPECT_EQ(result.RemovedDumpCount, 1);
    EXPECT_EQ(result.RemovedByteCount, 21);
    ExpectDumpExists("20260101T000000", false);
    ExpectDumpExists("20260101T010000", true);
    ExpectDumpExists("20260101T020000", true);
}

TEST_F(TMemoryProfileRetentionTest, IgnoresSymlinks)
{
    CreateFile("target", TDuration::Days(2));
    auto symlinkPath = GetPath("current_symlink.pb.gz");
    NFS::MakeSymbolicLink(GetPath("target"), symlinkPath);

    auto config = MakeConfig();
    auto result = CleanupMemoryProfiles(Directory_.Name(), std::nullopt, config, Now_);

    EXPECT_EQ(result.RemovedOrphanFileCount, 0);
    EXPECT_TRUE(Exists("target"));
    EXPECT_TRUE(TFileStat(TString(symlinkPath), /*nofollow*/ true).IsSymlink());
}

TEST(TMemoryProfileRetentionConfigTest, ParsesRetentionPolicy)
{
    auto config = NYTree::ConvertTo<THeapSizeLimitConfigPtr>(NYson::TYsonString(TStringBuf(R"(
        {
            memory_profile_dump_path = "/tmp/memory_profiles";
            memory_profile_retention = {
                max_dump_count = 3;
                max_dump_age = "7d";
                max_total_size = 1024;
                max_orphan_age = "2d";
            };
        }
    )")));

    ASSERT_TRUE(config->MemoryProfileRetention);
    EXPECT_EQ(config->MemoryProfileRetention->MaxDumpCount, 3);
    EXPECT_EQ(config->MemoryProfileRetention->MaxDumpAge, TDuration::Days(7));
    EXPECT_EQ(config->MemoryProfileRetention->MaxTotalSize, 1024);
    EXPECT_EQ(config->MemoryProfileRetention->MaxOrphanAge, TDuration::Days(2));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NTCMalloc
