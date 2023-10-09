#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/server/lib/hydra/changelog.h>
#include <yt/yt/server/lib/hydra/config.h>
#include <yt/yt/server/lib/hydra/local_changelog_store.h>

#include <yt/yt/ytlib/hydra/proto/hydra_manager.pb.h>

#include <yt/yt/core/misc/fs.h>

namespace NYT::NHydra {
namespace {

using namespace NConcurrency;
using namespace NHydra::NProto;

////////////////////////////////////////////////////////////////////////////////

class TChangelogTest
    : public ::testing::Test
{
protected:
    TFileChangelogStoreConfigPtr ChangelogStoreConfig_;
    IChangelogStoreFactoryPtr ChangelogStoreFactory_;
    IChangelogStorePtr ChangelogStore_;
    IChangelogPtr Changelog_;

    TFileChangelogStoreConfigPtr MakeConfig()
    {
        auto config = New<TFileChangelogStoreConfig>();
        InitConfig(config);
        config->Postprocess();
        return config;
    }

    virtual void InitConfig(const TFileChangelogStoreConfigPtr& config)
    {
        config->Path = "FileChangelog";
    }

    void SetUp() override
    {
        ChangelogStoreConfig_ = MakeConfig();

        ChangelogStoreFactory_ = CreateLocalChangelogStoreFactory(
            ChangelogStoreConfig_,
            "UTCLFlash");
        ChangelogStore_ = WaitFor(ChangelogStoreFactory_->Lock())
            .ValueOrThrow();

        Changelog_ = WaitFor(ChangelogStore_->CreateChangelog(/*id*/ 0, /*meta*/ {}))
            .ValueOrThrow();
    }

    void TearDown() override
    {
        NFS::RemoveRecursive(ChangelogStoreConfig_->Path);
    }


    static void CheckRecord(i32 data, const TSharedRef& record)
    {
        EXPECT_EQ(sizeof(data), record.Size());
        EXPECT_EQ(       data , *(reinterpret_cast<const i32*>(record.Begin())));
    }

    static void CheckRecords(const std::vector<TSharedRef>& records, int firstRecordIndex, int recordCount)
    {
        for (int recordIndex = firstRecordIndex; recordIndex < firstRecordIndex + recordCount; ++recordIndex) {
            CheckRecord(recordIndex, records[recordIndex - firstRecordIndex]);
        }
    }

    static TSharedRef MakeData(i32 data)
    {
        auto result = TSharedMutableRef::Allocate(sizeof(i32));
        *reinterpret_cast<i32*>(&*result.Begin()) = static_cast<i32>(data);
        return result;
    }
};

TEST_F(TChangelogTest, ReadWithRecordCountLimit)
{
    for (int recordIndex = 0; recordIndex < 40; ++recordIndex) {
        Changelog_->Append({MakeData(recordIndex)});
    }

    WaitFor(Changelog_->Flush())
        .ThrowOnError();

    auto check = [&] (int maxRecords) {
        auto records = WaitFor(Changelog_->Read(0, maxRecords, std::numeric_limits<i64>::max()))
            .ValueOrThrow();

        EXPECT_EQ(std::min(maxRecords, Changelog_->GetRecordCount()), std::ssize(records));
        CheckRecords(records, 0, std::ssize(records));
    };

    check(1);
    check(10);
    check(40);
    check(100);
}

TEST_F(TChangelogTest, ReadWithSizeLimit)
{
    for (int recordIndex = 0; recordIndex < 40; ++recordIndex) {
        Changelog_->Append({MakeData(recordIndex)});
    }

    WaitFor(Changelog_->Flush())
        .ThrowOnError();

    auto check = [&] (int maxBytes) {
        auto records = WaitFor(Changelog_->Read(0, 1000, maxBytes))
            .ValueOrThrow();
        EXPECT_EQ((maxBytes - 1) / sizeof(i32) + 1, records.size());
        CheckRecords(records, 0, std::ssize(records));
    };

    check(1);
    check(10);
    check(40);
    check(100);
}

TEST_F(TChangelogTest, Truncate)
{
    for (int recordIndex = 0; recordIndex < 40; ++recordIndex) {
        Changelog_->Append({MakeData(recordIndex)});
    }

    constexpr int NewRecordCount = 30;
    WaitFor(Changelog_->Truncate(NewRecordCount))
        .ThrowOnError();

    auto records = WaitFor(Changelog_->Read(0, std::numeric_limits<int>::max(), std::numeric_limits<i64>::max()))
        .ValueOrThrow();

    EXPECT_EQ(NewRecordCount, std::ssize(records));
    CheckRecords(records, 0, NewRecordCount);
}

////////////////////////////////////////////////////////////////////////////////

class TChangelogTestNoAutoFlush
    : public TChangelogTest
{
protected:
    void InitConfig(const TFileChangelogStoreConfigPtr& config) override
    {
        TChangelogTest::InitConfig(config);
        config->FlushPeriod = TDuration::Hours(1); // infinity
    }
};

TEST_F(TChangelogTestNoAutoFlush, ReadFromMemory)
{
    constexpr int RecordCount = 30;

    for (int recordIndex = 0; recordIndex < RecordCount; ++recordIndex) {
        Changelog_->Append({MakeData(recordIndex)});
    }

    auto records = WaitFor(Changelog_->Read(0, std::numeric_limits<int>::max(), std::numeric_limits<i64>::max()))
        .ValueOrThrow();

    EXPECT_EQ(RecordCount, std::ssize(records));
    CheckRecords(records, 0, RecordCount);
}

TEST_F(TChangelogTestNoAutoFlush, ReadFromDiskAndMemory)
{
    constexpr int DiskRecordCount = 10;
    constexpr int MemoryRecordCount = 20;
    constexpr int TotalRecordCount = DiskRecordCount + MemoryRecordCount;

    for (int recordIndex = 0; recordIndex < DiskRecordCount; ++recordIndex) {
        Changelog_->Append({MakeData(recordIndex)});
    }

    WaitFor(Changelog_->Flush())
        .ThrowOnError();

    for (int recordIndex = DiskRecordCount; recordIndex < DiskRecordCount + MemoryRecordCount; ++recordIndex) {
        Changelog_->Append({MakeData(recordIndex)});
    }

    auto records = WaitFor(Changelog_->Read(0, std::numeric_limits<int>::max(), std::numeric_limits<i64>::max()))
        .ValueOrThrow();

    EXPECT_EQ(TotalRecordCount, std::ssize(records));
    CheckRecords(records, 0, TotalRecordCount);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NHydra
