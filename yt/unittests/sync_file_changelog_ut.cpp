#include "stdafx.h"
#include "framework.h"

#include <server/hydra/config.h>
#include <server/hydra/changelog.h>
#include <server/hydra/sync_file_changelog.h>
#include <server/hydra/sync_file_changelog_impl.h>

#include <core/profiling/scoped_timer.h>

#include <core/misc/fs.h>

#include <util/random/random.h>
#include <util/system/tempfile.h>

namespace NYT {
namespace NHydra {
namespace {

////////////////////////////////////////////////////////////////////////////////

class TSyncFileChangelogTest
    : public ::testing::Test
{
protected:
    std::unique_ptr<TTempFile> TemporaryFile;
    std::unique_ptr<TTempFile> TemporaryIndexFile;
    TFileChangelogConfigPtr DefaultFileChangelogConfig;

    virtual void SetUp()
    {
        TemporaryFile.reset(new TTempFile(GenerateRandomFileName("Changelog")));
        TemporaryIndexFile.reset(new TTempFile(TemporaryFile->Name() + ".index"));
        DefaultFileChangelogConfig = New<TFileChangelogConfig>();
        DefaultFileChangelogConfig->IndexBlockSize = 64;
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

        auto changelog = New<TSyncFileChangelog>(TemporaryFile->Name(), fileChangelogConfig);

        changelog->Create(TSharedRef());
        auto records = MakeRecords<TRecordType>(0, recordCount);
        changelog->Append(0, records);
        changelog->Flush();
        return changelog;
    }

    template <class TRecordType>
    std::vector<TSharedRef> MakeRecords(i32 from, i32 to) const
    {
        std::vector<TSharedRef> records(to - from);
        for (i32 recordId = from; recordId < to; ++recordId) {
            TBlob blob(sizeof(TRecordType));
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

    TSyncFileChangelogPtr OpenChangelog() const
    {
        auto changelog = New<TSyncFileChangelog>(TemporaryFile->Name(), DefaultFileChangelogConfig);
        changelog->Open();
        return changelog;
    }

    void Finalize(TSyncFileChangelogPtr changelog)
    {
        changelog->Seal(changelog->GetRecordCount());
    }

    bool IsFinalized(TSyncFileChangelogPtr changelog)
    {
        return changelog->IsSealed();
    }


    template <class T>
    static void CheckRecord(const T& data, const TRef& record)
    {
        EXPECT_EQ(record.Size(), sizeof(data));
        EXPECT_EQ(*(reinterpret_cast<const T*>(record.Begin())), data);
    }


    template <class T>
    static void CheckRead(
        TSyncFileChangelogPtr changelog,
        i32 firstRecordId,
        i32 recordCount,
        i32 logRecordCount)
    {
        std::vector<TSharedRef> records = changelog->Read(firstRecordId, recordCount, std::numeric_limits<i64>::max());

        i32 expectedRecordCount =
            firstRecordId >= logRecordCount ?
            0 : Min(recordCount, logRecordCount - firstRecordId);

        EXPECT_EQ(records.size(), expectedRecordCount);
        for (i32 i = 0; i < records.size(); ++i) {
            CheckRecord<T>(static_cast<T>(firstRecordId + i), records[i]);
        }
    }

    template <class T>
    static void CheckReads(TSyncFileChangelogPtr changelog, i32 logRecordCount)
    {
        for (i32 start = 0; start <= logRecordCount; ++start) {
            for (i32 end = start; end <= 2 * logRecordCount + 1; ++end) {
                CheckRead<T>(changelog, start, end - start, logRecordCount);
            }
        }
    }

    void TestCorrupted(i64 newFileSize, i32 initialRecordCount, i32 correctRecordCount) const
    {
        if (newFileSize > GetFileSize())
        {
            // Add trash to file
            TFile file(TemporaryFile->Name(), RdWr);
            file.Seek(0, sEnd);
            TBlob data(newFileSize - file.GetLength());
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

    void TestSealedTrimmed(i64 sealedRecordCount)
    {
        {
            TChangelogHeader header;
            TFile file(TemporaryFile->Name(), RdWr);
            TFileInput input(file);
            TFileOutput output(file);
            ReadPod(input, header);
            header.SealedRecordCount = sealedRecordCount;
            file.Seek(0, sSet);
            WritePod(output, header);
        }

        auto changelog = OpenChangelog();

        EXPECT_EQ(changelog->GetRecordCount(), sealedRecordCount);
        CheckRead<ui32>(changelog, 0, sealedRecordCount, sealedRecordCount);

        changelog->Unseal();
        changelog->Append(sealedRecordCount, MakeRecords<ui32>(changelog->GetRecordCount(), changelog->GetRecordCount() + 1));
        changelog->Flush();

        EXPECT_EQ(sealedRecordCount + 1, changelog->GetRecordCount());
        CheckRead<ui32>(changelog, 0, changelog->GetRecordCount(), changelog->GetRecordCount());
    }
};

TEST_F(TSyncFileChangelogTest, EmptyChangelog)
{
    {
        auto changelog = New<TSyncFileChangelog>(TemporaryFile->Name(), New<TFileChangelogConfig>());
        changelog->Create(TSharedRef());
    }
    {
        auto changelog = New<TSyncFileChangelog>(TemporaryFile->Name(), New<TFileChangelogConfig>());
        changelog->Open();
    }
}

TSharedRef GenerateBlob(size_t size)
{
    auto blob = TSharedRef::Allocate(size);
    for (int i = 0; i < size; ++i) {
        blob.Begin()[i] = static_cast<char>(i % 256);
    }
    return blob;
}

TEST_F(TSyncFileChangelogTest, Meta)
{
    auto meta = GenerateBlob(1000);
    auto record = GenerateBlob(2000);

    {
        auto changelog = New<TSyncFileChangelog>(TemporaryFile->Name(), New<TFileChangelogConfig>());
        changelog->Create(meta);
        changelog->Append(0, std::vector<TSharedRef>(1, record));
        changelog->Flush();
    }
    {
        auto changelog = New<TSyncFileChangelog>(TemporaryFile->Name(), New<TFileChangelogConfig>());
        changelog->Open();
        EXPECT_TRUE(TRef::AreBitwiseEqual(meta, changelog->GetMeta()));
        EXPECT_EQ(1, changelog->GetRecordCount());
        EXPECT_TRUE(TRef::AreBitwiseEqual(record, changelog->Read(0, 1, std::numeric_limits<i64>::max())[0]));
    }
}

TEST_F(TSyncFileChangelogTest, Finalized)
{
    const int logRecordCount = 256;
    {
        auto changelog = CreateChangelog<ui32>(logRecordCount);
        EXPECT_FALSE(IsFinalized(changelog));
        Finalize(changelog);
        EXPECT_TRUE(IsFinalized(changelog));
    }
    {
        auto changelog = OpenChangelog();
        EXPECT_TRUE(IsFinalized(changelog));
    }
}


TEST_F(TSyncFileChangelogTest, ReadWrite)
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

TEST_F(TSyncFileChangelogTest, TestCorrupted)
{
    const int logRecordCount = 1024;
    {
        auto changelog = CreateChangelog<ui32>(logRecordCount);
    }

    i64 fileSize = GetFileSize();
    TestCorrupted(fileSize - 1, logRecordCount, logRecordCount - 1);
    TestCorrupted(30, logRecordCount, 0);
    TestCorrupted(fileSize + 1, logRecordCount, logRecordCount);
    TestCorrupted(fileSize + 1000, logRecordCount, logRecordCount);
    TestCorrupted(fileSize + 50000, logRecordCount, logRecordCount);
}

TEST_F(TSyncFileChangelogTest, TestSealedCorrupted)
{
    const int logRecordCount = 1024;
    {
        auto changelog = CreateChangelog<ui32>(logRecordCount);
    }

    TestSealedTrimmed(512);
    ASSERT_THROW(TestSealedTrimmed(1024), std::exception);
    TestSealedTrimmed(3);
    TestSealedTrimmed(0);
}

TEST_F(TSyncFileChangelogTest, Truncate)
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
            changelog->Seal(recordId);
        }
        {
            auto changelog = OpenChangelog();
            EXPECT_EQ(recordId, changelog->GetRecordCount());
            CheckRead<ui32>(changelog, 0, recordId, recordId);
            changelog->Unseal();
        }
    }
}

TEST_F(TSyncFileChangelogTest, TruncateAppend)
{
    const int logRecordCount = 256;

    {
        auto changelog = CreateChangelog<ui32>(logRecordCount);
        EXPECT_EQ(logRecordCount, changelog->GetRecordCount());
        CheckRead<ui32>(changelog, 0, logRecordCount, logRecordCount);
    }

    int truncatedRecordId = logRecordCount / 2;
    {
        // Truncate
        auto changelog = OpenChangelog();
        changelog->Seal(truncatedRecordId);
        CheckRead<ui32>(changelog, 0, truncatedRecordId, truncatedRecordId);
    }
    {
        // Append
        auto changelog = OpenChangelog();
        changelog->Unseal();
        changelog->Append(truncatedRecordId, MakeRecords<ui32>(truncatedRecordId, logRecordCount));
    }
    {
        // Check
        auto changelog = OpenChangelog();
        CheckRead<ui32>(changelog, 0, logRecordCount, logRecordCount);
    }
}

TEST_F(TSyncFileChangelogTest, UnalignedChecksum)
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

TEST_F(TSyncFileChangelogTest, MissingIndex)
{
    const int logRecordCount = 256;

    {
        auto changelog = CreateChangelog<ui8>(logRecordCount);
    }
    {
        NFS::Remove(TemporaryIndexFile->Name());
        auto changelog = OpenChangelog();
        CheckRead<ui8>(changelog, 0, logRecordCount, logRecordCount);
    }
}

// This structure is hack to make changelog with huge blobs.
// Remove it with refactoring.
class BigStruct {
public:
    BigStruct(i32 num) {
        str.fill(0);
    }

private:
    std::array<char, 1000000> str;
};

TEST_F(TSyncFileChangelogTest, DISABLED_Profiling)
{
    auto fileChangelogConfig = New<TFileChangelogConfig>();
    fileChangelogConfig->IndexBlockSize = 1024 * 1024;
    for (int i = 0; i < 2; ++i) {
        int recordsCount;
        if (i == 0) { // A lot of small records
            recordsCount = 10000000;
            NProfiling::TScopedTimer timer;
            TSyncFileChangelogPtr changelog = CreateChangelog<ui32>(recordsCount, fileChangelogConfig);
            std::cerr << "Make changelog of size " << recordsCount <<
                ", with blob of size " << sizeof(ui32) <<
                ", time " << ToString(timer.GetElapsed()) << std::endl;
        } else {
            recordsCount = 50;
            NProfiling::TScopedTimer timer;
            TSyncFileChangelogPtr changelog = CreateChangelog<BigStruct>(recordsCount, fileChangelogConfig);
            std::cerr << "Make changelog of size " << recordsCount <<
                ", with blob of size " << sizeof(BigStruct) <<
                ", time " << ToString(timer.GetElapsed()) << std::endl;
        }

        {
            NProfiling::TScopedTimer timer;
            TSyncFileChangelogPtr changelog = OpenChangelog();
            std::cerr << "Open changelog of size " << recordsCount <<
                ", time " << ToString(timer.GetElapsed()) << std::endl;
        }
        {
            TSyncFileChangelogPtr changelog = OpenChangelog();
            NProfiling::TScopedTimer timer;
            std::vector<TSharedRef> records = changelog->Read(0, recordsCount, std::numeric_limits<i64>::max());
            std::cerr << "Read full changelog of size " << recordsCount <<
                ", time " << ToString(timer.GetElapsed()) << std::endl;

            timer.Restart();
            changelog->Seal(recordsCount / 2);
            std::cerr << "Sealing changelog of size " << recordsCount <<
                ", time " << ToString(timer.GetElapsed()) << std::endl;
        }
    }
    SUCCEED();
}

TEST_F(TSyncFileChangelogTest, SealEmptyChangelog)
{
    auto changelog = CreateChangelog<int>(0);
    changelog->Seal(0);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NHydra
} // namespace NYT
