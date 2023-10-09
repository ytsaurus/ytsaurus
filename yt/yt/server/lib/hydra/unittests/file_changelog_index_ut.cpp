#include <yt/yt/server/lib/hydra/config.h>
#include <yt/yt/server/lib/hydra/format.h>
#include <yt/yt/server/lib/hydra/file_changelog_index.h>

#include <yt/yt/core/test_framework/framework.h>

#include <util/system/tempfile.h>

namespace NYT::NHydra {
namespace {

using namespace NConcurrency;
using namespace NIO;

////////////////////////////////////////////////////////////////////////////////

class TFileChangelogIndexTest
    : public ::testing::Test
    , public ::testing::WithParamInterface<std::tuple<
        NIO::EIOEngineType,
        const char*
    >>
{
protected:
    IIOEnginePtr IOEngine_;
    std::unique_ptr<TTempFile> TempFile_;

    TFileChangelogIndexPtr CreateIndex()
    {
        return New<TFileChangelogIndex>(
            IOEngine_,
            /*memoryUsageTracker*/ nullptr,
            TempFile_->Name(),
            New<TFileChangelogConfig>(),
            EWorkloadCategory::Idle);
    }

    void AppendRecords(
        const TFileChangelogIndexPtr& index,
        int startRecordIndex = 0,
        int recordCount = 10)
    {
        for (int i = startRecordIndex; i < startRecordIndex + recordCount; ++i) {
            EXPECT_EQ(i, index->GetRecordCount());
            index->AppendRecord(i, {333 + i * 100, 433 + i * 100});
            EXPECT_EQ(i + 1, index->GetRecordCount());
        }
        index->SetFlushedDataRecordCount(startRecordIndex + recordCount);
    }

    void ValidateRecordRanges(
        const TFileChangelogIndexPtr& index,
        int startRecordIndex = 0,
        int recordCount = 10)
    {
        for (int i = startRecordIndex; i < startRecordIndex + recordCount; ++i) {
            auto range = index->GetRecordRange(i);
            EXPECT_EQ(333 + i * 100, range.first);
            EXPECT_EQ(333 + i * 100 + 100, range.second);
        }
    }

    void TruncateFile(i64 position)
    {
        TFile file(TempFile_->Name(), RdWr | OpenExisting);
        file.Resize(position);
    }

    void DamageFile(i64 position)
    {
        TFile file(TempFile_->Name(), RdWr | OpenExisting);

        file.Seek(position, sSet);

        ui8 ch;
        EXPECT_EQ(1U, file.Read(&ch, 1));

        file.Seek(position, sSet);

        ++ch;
        file.Write(&ch, 1);
    }

public:
    void SetUp() override
    {
        const auto& args = GetParam();
        const auto& type = std::get<0>(args);

        auto config = NYTree::ConvertTo<NYTree::INodePtr>(
            NYson::TYsonString(TString(std::get<1>(args))));

        IOEngine_ = CreateIOEngine(type, config);

        TempFile_ = std::make_unique<TTempFile>(GenerateRandomFileName("TFileChangelogIndexTest.index"));
    }

    void TearDown() override
    {
        TempFile_.reset();
    }
};

TEST_P(TFileChangelogIndexTest, CreateMissing)
{
    {
        auto index = CreateIndex();
        EXPECT_EQ(EFileChangelogIndexOpenResult::MissingCreated, index->Open());
        index->Close();
    }

    {
        auto index = CreateIndex();
        EXPECT_EQ(EFileChangelogIndexOpenResult::ExistingOpened, index->Open());
        index->Close();
    }
}

TEST_P(TFileChangelogIndexTest, Append)
{
    auto index = CreateIndex();
    EXPECT_EQ(EFileChangelogIndexOpenResult::MissingCreated, index->Open());

    AppendRecords(index);

    ValidateRecordRanges(index);

    index->Close();
}

TEST_P(TFileChangelogIndexTest, FindRecordsRange)
{
    auto index = CreateIndex();
    EXPECT_EQ(EFileChangelogIndexOpenResult::MissingCreated, index->Open());

    AppendRecords(index);

    EXPECT_EQ(std::nullopt, index->FindRecordsRange(/*firstRecordIndex*/ 10, /*maxRecords*/ 100, /*maxBytes*/ 1e9));
    EXPECT_EQ(std::make_pair(static_cast<i64>(333), static_cast<i64>(333)), index->FindRecordsRange(/*firstRecordIndex*/ 0, /*maxRecords*/ 0, /*maxBytes*/ 1e9));
    EXPECT_EQ(std::make_pair(static_cast<i64>(433), static_cast<i64>(533)), index->FindRecordsRange(/*firstRecordIndex*/ 1, /*maxRecords*/ 1, /*maxBytes*/ 1e9));
    EXPECT_EQ(std::make_pair(static_cast<i64>(433), static_cast<i64>(633)), index->FindRecordsRange(/*firstRecordIndex*/ 1, /*maxRecords*/ 2, /*maxBytes*/ 1e9));
    EXPECT_EQ(std::make_pair(static_cast<i64>(433), static_cast<i64>(533)), index->FindRecordsRange(/*firstRecordIndex*/ 1, /*maxRecords*/ 2, /*maxBytes*/ 1));
    EXPECT_EQ(std::nullopt, index->FindRecordsRange(/*firstRecordIndex*/ 1e9, /*maxRecords*/ 2, /*maxBytes*/ 1));

    index->Close();
}

TEST_P(TFileChangelogIndexTest, Flush)
{
    {
        auto index = CreateIndex();
        EXPECT_EQ(EFileChangelogIndexOpenResult::MissingCreated, index->Open());

        AppendRecords(index);

        index->SyncFlush();
        index->Close();
    }

    {
        auto index = CreateIndex();
        EXPECT_EQ(EFileChangelogIndexOpenResult::ExistingOpened, index->Open());

        ValidateRecordRanges(index);

        index->Close();
    }
}

TEST_P(TFileChangelogIndexTest, BrokenIndexHeader1)
{
    {
        auto index = CreateIndex();
        EXPECT_EQ(EFileChangelogIndexOpenResult::MissingCreated, index->Open());
        index->Close();
    }

    DamageFile(0);

    {
        auto index = CreateIndex();
        EXPECT_EQ(EFileChangelogIndexOpenResult::ExistingRecreatedBrokenIndexHeader, index->Open());
        EXPECT_EQ(0, index->GetRecordCount());
        index->Close();
    }
}

TEST_P(TFileChangelogIndexTest, BrokenIndexHeader2)
{
    {
        auto index = CreateIndex();
        EXPECT_EQ(EFileChangelogIndexOpenResult::MissingCreated, index->Open());
        index->Close();
    }

    TruncateFile(5);

    {
        auto index = CreateIndex();
        EXPECT_EQ(EFileChangelogIndexOpenResult::ExistingRecreatedBrokenIndexHeader, index->Open());
        EXPECT_EQ(0, index->GetRecordCount());
        index->Close();
    }
}

TEST_P(TFileChangelogIndexTest, BrokenSegment1)
{
    {
        auto index = CreateIndex();
        EXPECT_EQ(EFileChangelogIndexOpenResult::MissingCreated, index->Open());

        AppendRecords(index);

        index->SyncFlush();
        index->Close();
    }

    DamageFile(50);

    {
        auto index = CreateIndex();
        EXPECT_EQ(EFileChangelogIndexOpenResult::ExistingRecreatedBrokenSegmentChecksum, index->Open());
        EXPECT_EQ(0, index->GetRecordCount());
        index->Close();
    }
}

TEST_P(TFileChangelogIndexTest, BrokenSegment2)
{
    {
        auto index = CreateIndex();
        EXPECT_EQ(EFileChangelogIndexOpenResult::MissingCreated, index->Open());

        AppendRecords(index, 0, 10);

        index->SyncFlush();

        AppendRecords(index, 10, 10);

        index->SyncFlush();
        index->Close();
    }

    DamageFile(110);

    {
        auto index = CreateIndex();
        EXPECT_EQ(EFileChangelogIndexOpenResult::ExistingRecreatedBrokenSegmentChecksum, index->Open());
        EXPECT_EQ(0, index->GetRecordCount());
        index->Close();
    }
}

TEST_P(TFileChangelogIndexTest, BrokenSegment3)
{
    {
        auto index = CreateIndex();
        EXPECT_EQ(EFileChangelogIndexOpenResult::MissingCreated, index->Open());

        AppendRecords(index, 0, 10);

        index->SyncFlush();

        AppendRecords(index, 10, 10);

        index->SyncFlush();
        index->Close();
    }

    TruncateFile(200);

    {
        auto index = CreateIndex();
        EXPECT_EQ(EFileChangelogIndexOpenResult::ExistingTruncatedBrokenSegment, index->Open());
        EXPECT_EQ(10, index->GetRecordCount());
        ValidateRecordRanges(index, 0, 10);
        index->Close();
    }
}

INSTANTIATE_TEST_SUITE_P(
    TFileChangelogIndexTest,
    TFileChangelogIndexTest,
    ::testing::Values(
        std::make_tuple(NIO::EIOEngineType::ThreadPool, "{ }")));

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NHydra
