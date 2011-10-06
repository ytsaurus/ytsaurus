#include "../ytlib/meta_state/change_log.h"

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


    virtual void SetUp()
    {
        TemporaryFile.Reset(new TTempFile("tmp"));
        TemporaryIndexFile.Reset(new TTempFile("tmp.index"));
    }

    virtual void TearDown()
    {
        TemporaryFile.Reset(0);
        TemporaryIndexFile.Reset(0);
    }

    template<class T>
    static void CheckRecord(T data, TRef record)
    {
        EXPECT_EQ(record.Size(), sizeof(data));
        EXPECT_EQ(*(reinterpret_cast<T*>(record.Begin())), data);
    }

    template<class T>
    static void CheckRead(
        TChangeLog::TPtr changeLog,
        i32 firstRecordId,
        i32 recordCount,
        i32 logRecordCount)
    {
        yvector<TSharedRef> records;
        changeLog->Read(firstRecordId, recordCount, &records);

        i32 expectedRecordCount =
            firstRecordId >= logRecordCount ?
            0 : Min(recordCount, logRecordCount - firstRecordId);

        EXPECT_EQ(records.ysize(), expectedRecordCount);
        for (i32 i = 0; i < records.ysize(); ++i) {
            CheckRecord<T>(static_cast<T>(firstRecordId + i), records[i]);
        }
    }

    template<class T>
    static void CheckReads(TChangeLog::TPtr changeLog, i32 logRecordCount)
    {
        for (i32 start = 0; start <= logRecordCount; ++start) {
            for (i32 end = start; end <= 2 * logRecordCount + 1; ++end) {
                CheckRead<T>(changeLog, start, end - start, logRecordCount);
            }
        }
    }
};


TEST_F(TChangeLogTest, EmptyChangeLog)
{
    ASSERT_NO_THROW({
        TChangeLog::TPtr changeLog = New<TChangeLog>(TemporaryFile->Name(), 0, 64);
        changeLog->Create(0);
    });

    ASSERT_NO_THROW({
        TChangeLog::TPtr changeLog = New<TChangeLog>(TemporaryFile->Name(), 0, 64);
        changeLog->Open();
    });
}


TEST_F(TChangeLogTest, Finalized)
{
    const int logRecordCount = 256;

    {
        TChangeLog::TPtr changeLog = New<TChangeLog>(TemporaryFile->Name(), 0, 64);
        changeLog->Create(0);

        for (i32 recordId = 0; recordId < logRecordCount; ++recordId) {
            TBlob blob(sizeof(ui32));
            *reinterpret_cast<ui32*>(blob.begin()) = static_cast<ui32>(recordId);
            changeLog->Append(recordId, TSharedRef(blob));
        }

        changeLog->Flush();
        EXPECT_EQ(changeLog->IsFinalized(), false);

        changeLog->Finalize();
        EXPECT_EQ(changeLog->IsFinalized(), true);
    }

    {
        TChangeLog::TPtr changeLog = New<TChangeLog>(TemporaryFile->Name(), 0, 64);
        changeLog->Open();

        EXPECT_EQ(changeLog->IsFinalized(), true);
    }
}


TEST_F(TChangeLogTest, ReadWrite)
{
    const int logRecordCount = 64;

    {
        TChangeLog::TPtr changeLog = New<TChangeLog>(TemporaryFile->Name(), 0, 64);
        changeLog->Create(0);

        for (i32 recordId = 0; recordId < logRecordCount; ++recordId) {
            TBlob blob(sizeof(ui32));
            *reinterpret_cast<ui32*>(blob.begin()) = static_cast<ui32>(recordId);
            changeLog->Append(recordId, TSharedRef(blob));
        }

        changeLog->Flush();
        EXPECT_EQ(changeLog->GetRecordCount(), logRecordCount);
        CheckReads<ui32>(changeLog, logRecordCount);
    }

    {
        TChangeLog::TPtr changeLog = New<TChangeLog>(TemporaryFile->Name(), 0, 64);
        changeLog->Open();

        EXPECT_EQ(changeLog->GetRecordCount(), logRecordCount);
        CheckReads<ui32>(changeLog, logRecordCount);
    }
}

TEST_F(TChangeLogTest, TestCorrupted)
{
    const int logRecordCount = 1024;

    {
        TChangeLog::TPtr changeLog = New<TChangeLog>(TemporaryFile->Name(), 0, 64);
        changeLog->Create(0);

        for (i32 recordId = 0; recordId < logRecordCount; ++recordId) {
            TBlob blob(sizeof(ui32));
            *reinterpret_cast<ui32*>(blob.begin()) = static_cast<ui32>(recordId);
            changeLog->Append(recordId, TSharedRef(blob));
        }

        changeLog->Flush();
    }

    {
        // TODO: fix this! (couldn't find tmp in current dir)
        // Truncate file!
        TFile changeLogFile("tmp", RdWr);
        changeLogFile.Resize(changeLogFile.GetLength() - 1);
    }

    {
        TChangeLog::TPtr changeLog = New<TChangeLog>(TemporaryFile->Name(), 0, 64);
        changeLog->Open();

        EXPECT_EQ(changeLog->GetRecordCount(), logRecordCount - 1);
        CheckRead<ui32>(changeLog, 0, logRecordCount, logRecordCount - 1);

        TBlob blob(sizeof(i32));
        *reinterpret_cast<i32*>(blob.begin()) = static_cast<i32>(logRecordCount - 1);
        changeLog->Append(logRecordCount - 1, TSharedRef(blob));
        changeLog->Flush();

        EXPECT_EQ(changeLog->GetRecordCount(), logRecordCount);
        CheckRead<ui32>(changeLog, 0, logRecordCount, logRecordCount);
    }

    {
        TChangeLog::TPtr changeLog = New<TChangeLog>(TemporaryFile->Name(), 0, 64);
        changeLog->Open();

        EXPECT_EQ(changeLog->GetRecordCount(), logRecordCount);
        CheckRead<ui32>(changeLog, 0, logRecordCount, logRecordCount);
    }
}

TEST_F(TChangeLogTest, Truncate)
{
    const int logRecordCount = 256;

    {
        TChangeLog::TPtr changeLog = New<TChangeLog>(TemporaryFile->Name(), 0, 64);
        changeLog->Open();

        for (i32 recordId = 0; recordId < logRecordCount; ++recordId) {
            TBlob blob(sizeof(ui32));
            *reinterpret_cast<ui32*>(blob.begin()) = static_cast<ui32>(recordId);
            changeLog->Append(recordId, TSharedRef(blob));
        }

        changeLog->Flush();
        EXPECT_EQ(changeLog->GetRecordCount(), logRecordCount);
        CheckRead<ui32>(changeLog, 0, logRecordCount, logRecordCount);
    }

    for (int recordId = logRecordCount; recordId >= 0; --recordId) {
        {
            TChangeLog::TPtr changeLog = New<TChangeLog>(TemporaryFile->Name(), 0, 64);
            changeLog->Open();
            changeLog->Truncate(recordId);
        }
        {
            TChangeLog::TPtr changeLog = New<TChangeLog>(TemporaryFile->Name(), 0, 64);
            changeLog->Open();

            EXPECT_EQ(changeLog->GetRecordCount(), recordId);
            CheckRead<ui32>(changeLog, 0, recordId, recordId);
        }
    }
}

TEST_F(TChangeLogTest, TruncateAppend)
{
    const int logRecordCount = 256;

    {
        TChangeLog::TPtr changeLog = New<TChangeLog>(TemporaryFile->Name(), 0, 64);
        changeLog->Create(0);

        for (i32 recordId = 0; recordId < logRecordCount; ++recordId) {
            TBlob blob(sizeof(ui32));
            *reinterpret_cast<ui32*>(blob.begin()) = static_cast<ui32>(recordId);
            changeLog->Append(recordId, TSharedRef(blob));
        }

        changeLog->Flush();
        EXPECT_EQ(changeLog->GetRecordCount(), logRecordCount);
        CheckRead<ui32>(changeLog, 0, logRecordCount, logRecordCount);
    }

    int recordId = logRecordCount / 2;
    {
        // Truncate
        TChangeLog::TPtr changeLog = New<TChangeLog>(TemporaryFile->Name(), 0, 64);
        changeLog->Open();
        changeLog->Truncate(recordId);

        CheckRead<ui32>(changeLog, 0, recordId, recordId);
    }
    {
        // Append
        TChangeLog::TPtr changeLog = New<TChangeLog>(TemporaryFile->Name(), 0, 64);
        changeLog->Open();

        for (i32 i = recordId; i < logRecordCount; ++i) {
            TBlob blob(sizeof(ui32));
            *reinterpret_cast<ui32*>(blob.begin()) = static_cast<ui32>(i);
            changeLog->Append(i, TSharedRef(blob));
        }
    }
    {
        // Check
        TChangeLog::TPtr changeLog = New<TChangeLog>(TemporaryFile->Name(), 0, 64);
        changeLog->Open();

        CheckRead<ui32>(changeLog, 0, recordId, recordId);
    }
}

TEST_F(TChangeLogTest, UnalighnedChecksum)
{
    const int logRecordCount = 256;

    {
        TChangeLog::TPtr changeLog = New<TChangeLog>(TemporaryFile->Name(), 0, 64);
        changeLog->Create(0);

        for (i32 recordId = 0; recordId < logRecordCount; ++recordId) {
            TBlob blob(sizeof(ui8));
            *reinterpret_cast<ui8*>(blob.begin()) = static_cast<ui8>(recordId);
            changeLog->Append(recordId, TSharedRef(blob));
        }
    }
    {
        TChangeLog::TPtr changeLog = New<TChangeLog>(TemporaryFile->Name(), 0, 64);
        changeLog->Open();

        CheckRead<ui8>(changeLog, 0, logRecordCount, logRecordCount);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NMetaState
} // namespace NYT
