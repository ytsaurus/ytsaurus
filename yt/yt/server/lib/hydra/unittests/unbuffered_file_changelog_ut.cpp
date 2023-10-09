#include <yt/yt/server/lib/io/io_engine.h>

#include <yt/yt/server/lib/hydra/config.h>
#include <yt/yt/server/lib/hydra/format.h>
#include <yt/yt/server/lib/hydra/unbuffered_file_changelog.h>
#include <yt/yt/server/lib/hydra/file_changelog_index.h>

#include <yt/yt/ytlib/hydra/proto/hydra_manager.pb.h>

#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/misc/blob.h>
#include <yt/yt/core/misc/checksum.h>
#include <yt/yt/core/misc/fs.h>

#include <yt/yt/core/profiling/timing.h>

#include <util/random/random.h>

#include <util/system/tempfile.h>

#include <array>

namespace NYT::NHydra {
namespace {

using namespace NIO;
using namespace NHydra::NProto;

////////////////////////////////////////////////////////////////////////////////

class TUnbufferedFileChangelogTest
    : public ::testing::Test
    , public ::testing::WithParamInterface<EFileChangelogFormat>
{
protected:
    std::optional<TTempFile> TempFile_;
    std::optional<TTempFile> TempIndexFile_;

    TFileChangelogConfigPtr DefaultFileChangelogConfig_;

    IIOEnginePtr IOEngine_;

    EFileChangelogFormat GetFormatParam()
    {
        return GetParam();
    }

    void SetUp() override
    {
        TempFile_.emplace(GenerateRandomFileName("changelog"));
        TempIndexFile_.emplace((TempFile_->Name() + "." + ChangelogIndexExtension));

        DefaultFileChangelogConfig_ = New<TFileChangelogConfig>();
        DefaultFileChangelogConfig_->IndexFlushSize = 64;

        IOEngine_ = CreateIOEngine(EIOEngineType::ThreadPool, NYTree::INodePtr());
    }

    void TearDown() override
    {
        NFS::Remove(TempFile_->Name());
        NFS::Remove(TempIndexFile_->Name());
        TempFile_.reset();
        TempIndexFile_.reset();
    }

    IUnbufferedFileChangelogPtr CreateChangelog(int recordCount, TFileChangelogConfigPtr config = nullptr)
    {
        if (!config) {
            config = DefaultFileChangelogConfig_;
        }

        auto changelog = CreateUnbufferedFileChangelog(
            IOEngine_,
            /*memoryUsageTracker*/ nullptr,
            TempFile_->Name(),
            config);
        changelog->Create(/*meta*/ {}, GetFormatParam());
        AppendRecords(changelog, 0, recordCount);
        return changelog;
    }

    void AppendRecords(IUnbufferedFileChangelogPtr changelog, int firstRecordIndex, int recordCount)
    {
        auto records = MakeRecords(firstRecordIndex, firstRecordIndex + recordCount);
        changelog->Append(firstRecordIndex, records);
        changelog->Flush(/*withIndex*/ false);
    }

    // In sizeof(i32) units.
    static inline const std::vector<int> RecordSizes = {1, 50, 123, 4'000};

    static std::vector<TSharedRef> MakeRecords(int from, int to)
    {
        std::vector<TSharedRef> records(to - from);
        for (int recordIndex = from; recordIndex < to; ++recordIndex) {
            auto recordSize = RecordSizes[recordIndex % RecordSizes.size()];
            TBlob blob(GetRefCountedTypeCookie<TDefaultBlobTag>(), sizeof(i32) * recordSize);
            for (int i = 0; i < recordSize; ++i) {
                reinterpret_cast<i32*>(blob.Begin())[i] = recordIndex;
            }
            records[recordIndex - from] = TSharedRef::FromBlob(std::move(blob));
        }
        return records;
    }

    static void CheckRecord(TRef record, int recordIndex)
    {
        auto recordSize = RecordSizes[recordIndex % RecordSizes.size()];
        EXPECT_EQ(record.Size(), sizeof(i32) * recordSize);
        for (int i = 0; i < recordSize; ++i) {
            EXPECT_EQ(recordIndex, reinterpret_cast<const i32*>(record.Begin())[i]);
        }
    }

    i64 GetFileSize()
    {
        TFile file(TempFile_->Name(), RdWr);
        return file.GetLength();
    }

    IUnbufferedFileChangelogPtr OpenChangelog(TFileChangelogConfigPtr config = nullptr)
    {
        if (!config) {
            config = DefaultFileChangelogConfig_;
        }

        auto changelog = CreateUnbufferedFileChangelog(
            IOEngine_,
            /*memoryUsageTracker*/ nullptr,
            TempFile_->Name(),
            config);
        changelog->Open();
        return changelog;
    }

    static void CheckRead(
        IUnbufferedFileChangelogPtr changelog,
        int firstRecordIndex,
        int recordCount)
    {
        auto totalRecordCount = changelog->GetRecordCount();
        auto records = changelog->Read(firstRecordIndex, recordCount, std::numeric_limits<i64>::max());
        int expectedRecordCount = firstRecordIndex >= totalRecordCount ? 0 : Min(recordCount, totalRecordCount - firstRecordIndex);
        EXPECT_EQ(std::ssize(records), expectedRecordCount);
        for (int i = 0; i < std::ssize(records); ++i) {
            CheckRecord(records[i], firstRecordIndex + i);
        }
    }

    static void CheckReads(IUnbufferedFileChangelogPtr changelog)
    {
        auto totalRecordCount = changelog->GetRecordCount();
        for (int start = 0; start <= totalRecordCount + 1; ++start) {
            for (int end = start; end <= 2 * totalRecordCount + 1; ++end) {
                CheckRead(changelog, start, end - start);
            }
        }
    }

    void CorruptFile(const TString& fileName, i64 newFileSize)
    {
        if (newFileSize > TFile(fileName, RdOnly).GetLength()) {
            // Add trash to file.
            TFile file(fileName, RdWr);
            file.Seek(0, sEnd);
            TBlob data(GetRefCountedTypeCookie<TDefaultBlobTag>(), newFileSize - file.GetLength());
            std::fill(data.Begin(), data.End(), -1);
            file.Write(data.Begin(), data.Size());
        } else {
            // Truncate file.
            TFile file(fileName, RdWr);
            file.Resize(newFileSize);
        }
    }

    void TestCorruptedDataFile(i64 newFileSize, int initialRecordCount, int correctRecordCount)
    {
        NFS::Remove(TempIndexFile_->Name());

        CorruptFile(TempFile_->Name(), newFileSize);

        auto changelog = OpenChangelog();

        EXPECT_EQ(changelog->GetRecordCount(), correctRecordCount);
        CheckRead(changelog, 0, initialRecordCount);

        changelog->Append(correctRecordCount, MakeRecords(correctRecordCount, initialRecordCount));
        changelog->Flush(/*withIndex*/ false);

        EXPECT_EQ(changelog->GetRecordCount(), initialRecordCount);
        CheckRead(changelog, 0, initialRecordCount);
    }

    void TestCorruptedIndexFile(i64 newFileSize, int recordCount)
    {
        CorruptFile(TempIndexFile_->Name(), newFileSize);

        auto changelog = OpenChangelog();

        EXPECT_EQ(changelog->GetRecordCount(), recordCount);
        CheckRead(changelog, 0, recordCount);
    }

    void TestRecoverFromMissingIndex(i64 recoveryBufferSize)
    {
        constexpr int RecordCount = 256;
        CreateChangelog(RecordCount);

        auto config = New<TFileChangelogConfig>();
        config->RecoveryBufferSize = recoveryBufferSize;

        auto changelog = OpenChangelog(config);

        EXPECT_EQ(RecordCount, changelog->GetRecordCount());
        CheckRead(changelog, 0, RecordCount);
    }

    void TestForceIndexFlush(bool flushIndex)
    {
        auto config = New<TFileChangelogConfig>();
        config->IndexFlushSize = 1_MB;

        {
            auto changelog = CreateChangelog(1, config);
            changelog->Flush(flushIndex);
        }

        {
            auto index = New<TFileChangelogIndex>(
                IOEngine_,
                /*memoryUsageTracker*/ nullptr,
                TempIndexFile_->Name(),
                DefaultFileChangelogConfig_,
                EWorkloadCategory::Idle);
            EXPECT_EQ(EFileChangelogIndexOpenResult::ExistingOpened, index->Open());
            EXPECT_EQ(flushIndex ? 1 : 0, index->GetRecordCount());
        }
    }
};

TEST_P(TUnbufferedFileChangelogTest, Empty)
{
    {
        auto changelog = CreateUnbufferedFileChangelog(
            IOEngine_,
            /*memoryUsageTracker*/ nullptr,
            TempFile_->Name(),
            New<TFileChangelogConfig>());
        changelog->Create(/*meta*/ {}, GetFormatParam());
        EXPECT_EQ(0, changelog->GetRecordCount());
    }
    {
        auto changelog = CreateUnbufferedFileChangelog(
            IOEngine_,
            /*memoryUsageTracker*/ nullptr,
            TempFile_->Name(),
            New<TFileChangelogConfig>());
        changelog->Open();
        EXPECT_EQ(0, changelog->GetRecordCount());
    }
}

TEST_P(TUnbufferedFileChangelogTest, ReadWrite)
{
    constexpr int RecordCount = 16;
    {
        auto changelog = CreateChangelog(RecordCount);
        EXPECT_EQ(RecordCount, changelog->GetRecordCount());
        CheckReads(changelog);
    }
    {
        auto changelog = OpenChangelog();
        EXPECT_EQ(RecordCount, changelog->GetRecordCount());
        CheckReads(changelog);
    }
}

TEST_P(TUnbufferedFileChangelogTest, TestCorruptedDataFile)
{
    constexpr int RecordCount = 1024;
    CreateChangelog(RecordCount);
    auto fileSize = TFile(TempFile_->Name(), RdOnly).GetLength();
    TestCorruptedDataFile(fileSize - 1, RecordCount, RecordCount - 1);
    TestCorruptedDataFile(sizeof(TChangelogHeader_5), RecordCount, 0);
    TestCorruptedDataFile(fileSize + 1, RecordCount, RecordCount);
    TestCorruptedDataFile(fileSize + 1000, RecordCount, RecordCount);
    TestCorruptedDataFile(fileSize + 50000, RecordCount, RecordCount);
}

TEST_P(TUnbufferedFileChangelogTest, TestCorruptedIndexFile)
{
    constexpr int RecordCount = 1024;
    CreateChangelog(RecordCount);
    auto fileSize = TFile(TempIndexFile_->Name(), RdOnly).GetLength();
    TestCorruptedIndexFile(0, RecordCount);
    TestCorruptedIndexFile(1, RecordCount);
    TestCorruptedIndexFile(sizeof(TChangelogIndexHeader_5), RecordCount);
    TestCorruptedIndexFile(fileSize - 1, RecordCount);
    TestCorruptedIndexFile(fileSize + 1, RecordCount);
    TestCorruptedIndexFile(fileSize + 1000, RecordCount);
    TestCorruptedIndexFile(fileSize + 50000, RecordCount);
}

TEST_P(TUnbufferedFileChangelogTest, TruncateRead)
{
    constexpr int RecordCount = 16;

    {
        auto changelog = CreateChangelog(RecordCount);
        EXPECT_EQ(changelog->GetRecordCount(), RecordCount);
        CheckRead(changelog, 0, RecordCount);
    }

    for (int recordCount = RecordCount; recordCount >= 0; --recordCount) {
        {
            auto changelog = OpenChangelog();
            changelog->Truncate(recordCount);
            EXPECT_EQ(recordCount, changelog->GetRecordCount());
        }
        {
            auto changelog = OpenChangelog();
            EXPECT_EQ(recordCount, changelog->GetRecordCount());
            CheckRead(changelog, 0, recordCount);
        }
    }
}

TEST_P(TUnbufferedFileChangelogTest, RecoverFromMissingIndex1b)
{
    TestRecoverFromMissingIndex(1);
}

TEST_P(TUnbufferedFileChangelogTest, RecoverFromMissingIndex100b)
{
    TestRecoverFromMissingIndex(100);
}

TEST_P(TUnbufferedFileChangelogTest, RecoverFromMissingIndex16Mb)
{
    TestRecoverFromMissingIndex(16_MBs);
}

TEST_P(TUnbufferedFileChangelogTest, TruncateEmpty)
{
    auto changelog = CreateChangelog(0);
    changelog->Truncate(0);
    EXPECT_EQ(0, changelog->GetRecordCount());
}

TEST_P(TUnbufferedFileChangelogTest, TruncateWrite)
{
    constexpr int RecordCount1 = 100;
    auto changelog = CreateChangelog(0);
    AppendRecords(changelog, 0, RecordCount1);

    constexpr int RecordCount2 = 50;
    changelog->Truncate(RecordCount2);
    EXPECT_EQ(RecordCount2, changelog->GetRecordCount());
    CheckRead(changelog, 0, RecordCount2);

    constexpr int RecordCount3 = 250;
    AppendRecords(changelog, RecordCount2, RecordCount3 - RecordCount2);
    EXPECT_EQ(RecordCount3, changelog->GetRecordCount());
    CheckRead(changelog, 0, RecordCount3);
}

TEST_P(TUnbufferedFileChangelogTest, TestIndexFlushOnClose)
{
    constexpr int RecordCount = 1;

    {
        auto changelog = CreateChangelog(RecordCount);
        changelog->Close();
    }

    {
        auto index = New<TFileChangelogIndex>(
            IOEngine_,
            /*memoryUsageTracker*/ nullptr,
            TempIndexFile_->Name(),
            DefaultFileChangelogConfig_,
            EWorkloadCategory::Idle);
        EXPECT_EQ(EFileChangelogIndexOpenResult::ExistingOpened, index->Open());
        EXPECT_EQ(RecordCount, index->GetRecordCount());
    }
}

TEST_P(TUnbufferedFileChangelogTest, DoForceIndexFlush)
{
    TestForceIndexFlush(true);
}

TEST_P(TUnbufferedFileChangelogTest, DontForceIndexFlush)
{
    TestForceIndexFlush(false);
}

INSTANTIATE_TEST_SUITE_P(
    TUnbufferedFileChangelogTest,
    TUnbufferedFileChangelogTest,
    ::testing::Values(
        EFileChangelogFormat::V5));

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NHydra
