#include "stdafx.h"

#include <ytlib/meta_state/change_log.h>
#include <ytlib/profiling/single_timer.h>

#include <util/random/random.h>
#include <util/system/tempfile.h>

#include <contrib/testing/framework.h>

namespace NYT {
namespace NMetaState {

////////////////////////////////////////////////////////////////////////////////

class TChangeLogTest
    : public ::testing::Test
{
protected:
    THolder<TTempFile> TemporaryFile;
    THolder<TTempFile> TemporaryIndexFile;
    i64 IndexSize;

    virtual void SetUp()
    {
        TemporaryFile.Reset(new TTempFile(GenerateRandomFileName("ChangeLog")));
        TemporaryIndexFile.Reset(new TTempFile(TemporaryFile->Name() + ".index"));
        IndexSize = 64;
    }

    virtual void TearDown()
    {
        TemporaryFile.Reset(0);
        TemporaryIndexFile.Reset(0);
    }

    template <class RecordType>
    TChangeLogPtr CreateChangeLog(size_t recordsCount) const
    {
        TChangeLogPtr changeLog = New<TChangeLog>(TemporaryFile->Name(), 0, IndexSize);
        changeLog->Create(0);
        std::vector<TSharedRef> records = MakeRecords<RecordType>(0, recordsCount);
        changeLog->Append(0, records);
        changeLog->Flush();
        return changeLog;
    }

    template <class RecordType>
    std::vector<TSharedRef> MakeRecords(i32 from, i32 to) const
    {
        std::vector<TSharedRef> records(to - from);
        for (i32 recordId = from; recordId < to; ++recordId) {
            TBlob blob(sizeof(RecordType));
            *reinterpret_cast<RecordType*>(blob.begin()) = static_cast<RecordType>(recordId);
            records[recordId - from] = MoveRV(blob);
        }
        return records;
    }

    i64 GetFileSize() const {
        TFile changeLogFile(TemporaryFile->Name(), RdWr);
        return changeLogFile.GetLength();
    }

    TChangeLogPtr OpenChangeLog() const
    {
        TChangeLogPtr changeLog = New<TChangeLog>(TemporaryFile->Name(), 0, IndexSize);
        changeLog->Open();
        return changeLog;
    }

    template <class T>
    static void CheckRecord(const T& data, const TRef& record)
    {
        EXPECT_EQ(record.Size(), sizeof(data));
        EXPECT_EQ(*(reinterpret_cast<const T*>(record.Begin())), data);
    }


    template <class T>
    static void CheckRead(
        TChangeLogPtr changeLog,
        i32 firstRecordId,
        i32 recordCount,
        i32 logRecordCount)
    {
        std::vector<TSharedRef> records;
        changeLog->Read(firstRecordId, recordCount, &records);

        i32 expectedRecordCount =
            firstRecordId >= logRecordCount ?
            0 : Min(recordCount, logRecordCount - firstRecordId);

        EXPECT_EQ(records.size(), expectedRecordCount);
        for (i32 i = 0; i < records.size(); ++i) {
            CheckRecord<T>(static_cast<T>(firstRecordId + i), records[i]);
        }
    }

    template <class T>
    static void CheckReads(TChangeLogPtr changeLog, i32 logRecordCount)
    {
        for (i32 start = 0; start <= logRecordCount; ++start) {
            for (i32 end = start; end <= 2 * logRecordCount + 1; ++end) {
                CheckRead<T>(changeLog, start, end - start, logRecordCount);
            }
        }
    }

    void TestCorrupted(i64 newFileSize, i32 initialRecordCount, i32 correctRecordCount) const
    {
        if (newFileSize > GetFileSize())
        {
            // Add trash to file
            TFile changeLogFile(TemporaryFile->Name(), RdWr);
            changeLogFile.Seek(0, sEnd);
            TBlob data(newFileSize - changeLogFile.GetLength(), -1);
            changeLogFile.Write(&(*data.begin()), data.size());
        }
        else {
            // Truncate file.
            TFile changeLogFile(TemporaryFile->Name(), RdWr);
            changeLogFile.Resize(newFileSize);
        }

        TChangeLogPtr changeLog = OpenChangeLog();

        EXPECT_EQ(changeLog->GetRecordCount(), correctRecordCount);
        CheckRead<ui32>(changeLog, 0, initialRecordCount, correctRecordCount);

        changeLog->Append(correctRecordCount, MakeRecords<ui32>(correctRecordCount, initialRecordCount));
        changeLog->Flush();

        EXPECT_EQ(changeLog->GetRecordCount(), initialRecordCount);
        CheckRead<ui32>(changeLog, 0, initialRecordCount, initialRecordCount);
    }
};

TEST_F(TChangeLogTest, EmptyChangeLog)
{
    ASSERT_NO_THROW({
        TChangeLogPtr changeLog = New<TChangeLog>(TemporaryFile->Name(), 0);
        changeLog->Create(0);
    });

    ASSERT_NO_THROW({
        TChangeLogPtr changeLog = New<TChangeLog>(TemporaryFile->Name(), 0);
        changeLog->Open();
    });
}


TEST_F(TChangeLogTest, Finalized)
{
    const int logRecordCount = 256;
    {
        TChangeLogPtr changeLog = CreateChangeLog<ui32>(logRecordCount);
        EXPECT_EQ(changeLog->IsFinalized(), false);
        changeLog->Finalize();
        EXPECT_EQ(changeLog->IsFinalized(), true);
    }
    {
        TChangeLogPtr changeLog = OpenChangeLog();
        EXPECT_EQ(changeLog->IsFinalized(), true);
    }
}


TEST_F(TChangeLogTest, ReadWrite)
{
    const int logRecordCount = 16;
    {
        TChangeLogPtr changeLog = CreateChangeLog<ui32>(logRecordCount);
        EXPECT_EQ(changeLog->GetRecordCount(), logRecordCount);
        CheckReads<ui32>(changeLog, logRecordCount);
    }
    {
        TChangeLogPtr changeLog = OpenChangeLog();
        EXPECT_EQ(changeLog->GetRecordCount(), logRecordCount);
        CheckReads<ui32>(changeLog, logRecordCount);
    }
}

TEST_F(TChangeLogTest, TestCorrupted)
{
    const int logRecordCount = 1024;
    {
        TChangeLogPtr changeLog = CreateChangeLog<ui32>(logRecordCount);
    }

    i64 fileSize = GetFileSize();
    TestCorrupted(fileSize - 1, logRecordCount, logRecordCount - 1);
    TestCorrupted(30, logRecordCount, 0);
    TestCorrupted(fileSize + 1, logRecordCount, logRecordCount);
    TestCorrupted(fileSize + 1000, logRecordCount, logRecordCount);
    TestCorrupted(fileSize + 50000, logRecordCount, logRecordCount);
}

TEST_F(TChangeLogTest, Truncate)
{
    const int logRecordCount = 256;

    {
        TChangeLogPtr changeLog = CreateChangeLog<ui32>(logRecordCount);
        EXPECT_EQ(changeLog->GetRecordCount(), logRecordCount);
        CheckRead<ui32>(changeLog, 0, logRecordCount, logRecordCount);
    }

    for (int recordId = logRecordCount; recordId >= 0; --recordId) {
        {
            TChangeLogPtr changeLog = OpenChangeLog();
            changeLog->Truncate(recordId);
        }
        {
            TChangeLogPtr changeLog = OpenChangeLog();
            EXPECT_EQ(changeLog->GetRecordCount(), recordId);
            CheckRead<ui32>(changeLog, 0, recordId, recordId);
        }
    }
}

TEST_F(TChangeLogTest, TruncateAppend)
{
    const int logRecordCount = 256;

    {
        TChangeLogPtr changeLog = CreateChangeLog<ui32>(logRecordCount);
        EXPECT_EQ(changeLog->GetRecordCount(), logRecordCount);
        CheckRead<ui32>(changeLog, 0, logRecordCount, logRecordCount);
    }

    int truncatedRecordId = logRecordCount / 2;
    {
        // Truncate
        TChangeLogPtr changeLog = OpenChangeLog();
        changeLog->Truncate(truncatedRecordId);
        CheckRead<ui32>(changeLog, 0, truncatedRecordId, truncatedRecordId);
    }
    {
        // Append
        TChangeLogPtr changeLog = OpenChangeLog();
        changeLog->Append(truncatedRecordId, MakeRecords<ui32>(truncatedRecordId, logRecordCount));
    }
    {
        // Check
        TChangeLogPtr changeLog = OpenChangeLog();
        CheckRead<ui32>(changeLog, 0, logRecordCount, logRecordCount);
    }
}

TEST_F(TChangeLogTest, UnalighnedChecksum)
{
    const int logRecordCount = 256;

    {
        TChangeLogPtr changeLog = CreateChangeLog<ui8>(logRecordCount);
    }
    {
        TChangeLogPtr changeLog = OpenChangeLog();
        CheckRead<ui8>(changeLog, 0, logRecordCount, logRecordCount);
    }
}

// This structure is hack to make changelog with huge blobs.
// Remove it with refactoring.
class BigStruct {
public:
    BigStruct(i32 num) {
        memset(str, 0, sizeof(str));
    }

private:
    char str[1000000];
};

TEST_F(TChangeLogTest, DISABLED_Profiling)
{
    IndexSize = 1024 * 1024;
    for (int i = 0; i < 2; ++i) {
        int recordsCount;
        if (i == 0) { // A lot of small records
            recordsCount = 10000000;
            NProfiling::TSingleTimer timer;
            TChangeLogPtr changeLog = CreateChangeLog<ui32>(recordsCount);
            std::cerr << "Make changelog of size " << recordsCount <<
                ", with blob of size " << sizeof(ui32) <<
                ", time " << timer.ElapsedTimeAsString() << std::endl;
        }
        else {
            recordsCount = 50;
            NProfiling::TSingleTimer timer;
            TChangeLogPtr changeLog = CreateChangeLog<BigStruct>(recordsCount);
            std::cerr << "Make changelog of size " << recordsCount <<
                ", with blob of size " << sizeof(BigStruct) <<
                ", time " << timer.ElapsedTimeAsString() << std::endl;
        }

        {
            NProfiling::TSingleTimer timer;
            TChangeLogPtr changeLog = OpenChangeLog();
            std::cerr << "Open changelog of size " << recordsCount <<
                ", time " << timer.ElapsedTimeAsString() << std::endl;
        }
        {
            TChangeLogPtr changeLog = OpenChangeLog();
            NProfiling::TSingleTimer timer;
            std::vector<TSharedRef> records;
            changeLog->Read(0, recordsCount, &records);
            std::cerr << "Read full changelog of size " << recordsCount <<
                ", time " << timer.ElapsedTimeAsString() << std::endl;

            timer.Restart();
            changeLog->Truncate(recordsCount / 2);
            std::cerr << "Truncating changelog of size " << recordsCount <<
                ", time " << timer.ElapsedTimeAsString() << std::endl;

            timer.Restart();
            changeLog->Finalize();
            std::cerr << "Finalizing changelog of size " << recordsCount / 2 <<
                ", time " << timer.ElapsedTimeAsString() << std::endl;
        }
    }
    SUCCEED();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NMetaState
} // namespace NYT
