#include <yt/core/test_framework/framework.h>

#include <yt/server/lib/hydra/changelog.h>
#include <yt/server/lib/hydra/config.h>
#include <yt/server/lib/hydra/format.h>
#include <yt/server/lib/hydra/file_helpers.h>
#include <yt/server/lib/hydra/sync_file_changelog.h>

#include <yt/ytlib/chunk_client/io_engine.h>
#include <yt/ytlib/hydra/proto/hydra_manager.pb.h>

#include <yt/core/misc/checksum.h>
#include <yt/core/misc/fs.h>

#include <yt/core/profiling/timing.h>

#include <util/random/random.h>

#include <util/system/tempfile.h>

#include <array>

namespace NYT::NHydra {
namespace {

using namespace NHydra::NProto;

////////////////////////////////////////////////////////////////////////////////

class TSyncFileChangelogTest
    : public ::testing::Test
    , public ::testing::WithParamInterface<EFileChangelogFormat>
{
protected:
    std::unique_ptr<TTempFile> TemporaryFile;
    std::unique_ptr<TTempFile> TemporaryIndexFile;
    TFileChangelogConfigPtr DefaultFileChangelogConfig;
    NChunkClient::IIOEnginePtr IOEngine;

    EFileChangelogFormat GetFormatParam() const
    {
        return GetParam();
    }

    virtual void SetUp()
    {
        TemporaryFile.reset(new TTempFile(GenerateRandomFileName("Changelog")));
        TemporaryIndexFile.reset(new TTempFile(TemporaryFile->Name() + ".index"));
        DefaultFileChangelogConfig = New<TFileChangelogConfig>();
        DefaultFileChangelogConfig->IndexBlockSize = 64;
        IOEngine = NChunkClient::CreateIOEngine(NChunkClient::EIOEngineType::ThreadPool, NYTree::INodePtr());
    }

    virtual void TearDown()
    {
        NFS::Remove(TemporaryFile->Name());
        NFS::Remove(TemporaryIndexFile->Name());
        TemporaryFile.reset();
        TemporaryIndexFile.reset();
    }

    template <class TRecordType>
    TSyncFileChangelogPtr CreateChangelog(size_t recordCount, TFileChangelogConfigPtr fileChangelogConfig = nullptr) const
    {
        if (!fileChangelogConfig) {
            fileChangelogConfig = DefaultFileChangelogConfig;
        }

        auto changelog = New<TSyncFileChangelog>(IOEngine, TemporaryFile->Name(), fileChangelogConfig);
        changelog->Create(GetFormatParam());
        auto records = MakeRecords<TRecordType>(0, recordCount);
        changelog->Append(0, records);
        changelog->Flush();
        return changelog;
    }

    template <class TRecordType>
    std::vector<TSharedRef> MakeRecords(int from, int to) const
    {
        std::vector<TSharedRef> records(to - from);
        for (int recordId = from; recordId < to; ++recordId) {
            TBlob blob(TDefaultBlobTag(), sizeof(TRecordType));
            *reinterpret_cast<TRecordType*>(blob.Begin()) = static_cast<TRecordType>(recordId);
            records[recordId - from] = TSharedRef::FromBlob(std::move(blob));
        }
        return records;
    }

    i64 GetFileSize() const
    {
        TFile file(TemporaryFile->Name(), RdWr);
        return file.GetLength();
    }

    size_t GetRecordHeaderSize() const
    {
        switch (GetFormatParam()) {
            case EFileChangelogFormat::V4: return sizeof(TChangelogRecordHeader_4);
            case EFileChangelogFormat::V5: return sizeof(TChangelogRecordHeader_5);
            default:                       YT_ABORT();
        }
    }

    TSyncFileChangelogPtr OpenChangelog() const
    {
        auto changelog = New<TSyncFileChangelog>(IOEngine, TemporaryFile->Name(), DefaultFileChangelogConfig);
        changelog->Open();
        return changelog;
    }

    template <class T>
    static void CheckRecord(const T& data, TRef record)
    {
        EXPECT_EQ(record.Size(), sizeof(data));
        EXPECT_EQ(*reinterpret_cast<const T*>(record.Begin()), data);
    }


    template <class T>
    static void CheckRead(
        TSyncFileChangelogPtr changelog,
        int firstRecordId,
        int recordCount,
        int logRecordCount)
    {
        auto records = changelog->Read(firstRecordId, recordCount, std::numeric_limits<i64>::max());
        int expectedRecordCount = firstRecordId >= logRecordCount ? 0 : Min(recordCount, logRecordCount - firstRecordId);
        EXPECT_EQ(records.size(), expectedRecordCount);
        for (int i = 0; i < records.size(); ++i) {
            CheckRecord<T>(static_cast<T>(firstRecordId + i), records[i]);
        }
    }

    template <class T>
    static void CheckReads(TSyncFileChangelogPtr changelog, int logRecordCount)
    {
        for (int start = 0; start <= logRecordCount; ++start) {
            for (int end = start; end <= 2 * logRecordCount + 1; ++end) {
                CheckRead<T>(changelog, start, end - start, logRecordCount);
            }
        }
    }

    void TestCorrupted(i64 newFileSize, int initialRecordCount, int correctRecordCount) const
    {
        if (newFileSize > GetFileSize()) {
            // Add trash to file.
            TFile file(TemporaryFile->Name(), RdWr);
            file.Seek(0, sEnd);
            TBlob data(TDefaultBlobTag(), newFileSize - file.GetLength());
            std::fill(data.Begin(), data.End(), -1);
            file.Write(data.Begin(), data.Size());
        } else {
            // Truncate file.
            TFile file(TemporaryFile->Name(), RdWr);
            file.Resize(newFileSize);
        }

        auto changelog = OpenChangelog();

        EXPECT_EQ(changelog->GetRecordCount(), correctRecordCount);
        CheckRead<ui32>(changelog, 0, initialRecordCount, correctRecordCount);

        changelog->Append(correctRecordCount, MakeRecords<ui32>(correctRecordCount, initialRecordCount));
        changelog->Flush();

        EXPECT_EQ(changelog->GetRecordCount(), initialRecordCount);
        CheckRead<ui32>(changelog, 0, initialRecordCount, initialRecordCount);
    }
};

TEST_P(TSyncFileChangelogTest, EmptyChangelog)
{
    {
        auto changelog = New<TSyncFileChangelog>(IOEngine, TemporaryFile->Name(), New<TFileChangelogConfig>());
        changelog->Create(GetFormatParam());
    }
    {
        auto changelog = New<TSyncFileChangelog>(IOEngine, TemporaryFile->Name(), New<TFileChangelogConfig>());
        changelog->Open();
    }
}

TEST_P(TSyncFileChangelogTest, ReadWrite)
{
    const int logRecordCount = 16;
    {
        auto changelog = CreateChangelog<ui32>(logRecordCount);
        EXPECT_EQ(logRecordCount, changelog->GetRecordCount());
        CheckReads<ui32>(changelog, logRecordCount);
    }
    {
        auto changelog = OpenChangelog();
        EXPECT_EQ(logRecordCount, changelog->GetRecordCount());
        CheckReads<ui32>(changelog, logRecordCount);
    }
}

TEST_P(TSyncFileChangelogTest, TestCorrupted)
{
    const int LogRecordCount = 1024;
    CreateChangelog<ui32>(LogRecordCount);
    i64 fileSize = GetFileSize();
    TestCorrupted(fileSize - 1, LogRecordCount, LogRecordCount - 1);
    TestCorrupted(30, LogRecordCount, 0);
    TestCorrupted(fileSize + 1, LogRecordCount, LogRecordCount);
    TestCorrupted(fileSize + 1000, LogRecordCount, LogRecordCount);
    TestCorrupted(fileSize + 50000, LogRecordCount, LogRecordCount);
}

TEST_P(TSyncFileChangelogTest, Truncate)
{
    const int logRecordCount = 16;

    {
        auto changelog = CreateChangelog<ui32>(logRecordCount);
        EXPECT_EQ(changelog->GetRecordCount(), logRecordCount);
        CheckRead<ui32>(changelog, 0, logRecordCount, logRecordCount);
    }

    for (int recordId = logRecordCount; recordId >= 0; --recordId) {
        {
            auto changelog = OpenChangelog();
            changelog->Truncate(recordId);
        }
        {
            auto changelog = OpenChangelog();
            EXPECT_EQ(recordId, changelog->GetRecordCount());
            CheckRead<ui32>(changelog, 0, recordId, recordId);
        }
    }
}

TEST_P(TSyncFileChangelogTest, UnalignedChecksum)
{
    const int logRecordCount = 256;

    {
        auto changelog = CreateChangelog<ui8>(logRecordCount);
    }
    {
        auto changelog = OpenChangelog();
        CheckRead<ui8>(changelog, 0, logRecordCount, logRecordCount);
    }
}

TEST_P(TSyncFileChangelogTest, MissingIndex)
{
    const int LogRecordCount = 256;

    CreateChangelog<ui8>(LogRecordCount);

    {
        NFS::Remove(TemporaryIndexFile->Name());
        auto changelog = OpenChangelog();
        CheckRead<ui8>(changelog, 0, LogRecordCount, LogRecordCount);
    }
}

TEST_P(TSyncFileChangelogTest, Padding)
{
    {
        const int Alignment = 4_KB;
        auto changelog = CreateChangelog<ui8>(0);

        TFileWrapper file(TemporaryFile->Name(), RdOnly);

        {
            auto record = TSharedMutableRef::Allocate(12, false);
            changelog->Append(0, {record});
            changelog->Flush();

            auto records = changelog->Read(0, std::numeric_limits<int>::max(), std::numeric_limits<i64>::max());
            EXPECT_EQ(records.size(), 1);

            auto paddingSize = Alignment - AlignUp(record.Size()) - AlignUp(GetRecordHeaderSize());

            TChangelogRecordHeader_4 header;
            file.Seek(-Alignment, sEnd);
            EXPECT_EQ(sizeof(header), file.Load(&header, sizeof(header)));

            EXPECT_EQ(header.RecordId, 0);
            EXPECT_EQ(header.DataSize, record.Size());
            EXPECT_EQ(header.PaddingSize, paddingSize);
        }

        {
            auto record = TSharedMutableRef::Allocate(Alignment - 2 * GetRecordHeaderSize(), false);
            changelog->Append(1, {record});
            changelog->Flush();

            auto paddingSize = Alignment - AlignUp(record.Size()) - AlignUp(GetRecordHeaderSize());

            TChangelogRecordHeader_4 header;
            file.Seek(-Alignment, sEnd);
            EXPECT_EQ(file.Load(&header, sizeof(header)), sizeof(header));

            EXPECT_EQ(header.RecordId, 1);
            EXPECT_EQ(header.DataSize, record.Size());
            EXPECT_EQ(header.PaddingSize, paddingSize);

            changelog->Append(2, {record});
            changelog->Flush();
        }

        {
            auto records = changelog->Read(0, std::numeric_limits<int>::max(), std::numeric_limits<i64>::max());
            EXPECT_EQ(records.size(), 3);
        }
    }

    {
        auto changelog = OpenChangelog();
        auto records = changelog->Read(0, std::numeric_limits<int>::max(), std::numeric_limits<i64>::max());
        EXPECT_EQ(records.size(), 3);
    }
}

TEST_P(TSyncFileChangelogTest, SealEmptyChangelog)
{
    auto changelog = CreateChangelog<int>(0);
    changelog->Truncate(0);
}

INSTANTIATE_TEST_SUITE_P(
    TSyncFileChangelogTest,
    TSyncFileChangelogTest,
    ::testing::Values(
        EFileChangelogFormat::V4,
        EFileChangelogFormat::V5
    )
);

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NHydra
