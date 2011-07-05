#include "../ytlib/master/change_log.h"

#include <library/unittest/registar.h>
#include <util/system/tempfile.h>

namespace NYT {

class TChangeLogTest
    : public TTestBase
{
    UNIT_TEST_SUITE(TChangeLogTest);
        UNIT_TEST(TestEmptyChangeLog);
        UNIT_TEST(TestFinalized);
        UNIT_TEST(TestReadWrite);
        UNIT_TEST(TestCorrupted);
        UNIT_TEST(TestTruncate);
        UNIT_TEST(TestTruncateAppend);
        UNIT_TEST(TestUnalighnedChecksum);
    UNIT_TEST_SUITE_END();

public:
    template<class T>
    static void CheckRecord(T data, TRef record)
    {
        UNIT_ASSERT_EQUAL(record.Size(), sizeof(data));
        UNIT_ASSERT_EQUAL(*(reinterpret_cast<T*>(record.Begin())), data);
    }

    template<class T>
    static void CheckRead(
        TChangeLog* changeLog,
        i32 startRecordId,
        i32 recordCount,
        i32 logRecordCount)
    {
        yvector<TSharedRef> records;
        changeLog->Read(startRecordId, recordCount, &records);
        i32 expectedRecordCount = startRecordId >= logRecordCount
            ? 0 : Min(recordCount, logRecordCount - startRecordId);
        UNIT_ASSERT_EQUAL(records.ysize(), expectedRecordCount);
        for (i32 i = 0; i < records.ysize(); ++i) {
            CheckRecord<T>(static_cast<T>(startRecordId + i), records[i]);
        }
    }

    template<class T>
    static void CheckReads(TChangeLog* changeLog, i32 logRecordCount)
    {
        for (i32 start = 0; start <= logRecordCount; ++start) {
            for (i32 end = start; end <= 2 * logRecordCount + 1; ++end) {
                CheckRead<T>(changeLog, start, end - start, logRecordCount);
            }
        }
    }

    void TestEmptyChangeLog()
    {
        TTempFile tempFile("tmp");
        TTempFile tempIndexFile("tmp.index");
        {
            TChangeLog changeLog(tempFile.Name(), 0, 64);
            changeLog.Create(0);
        }
        {
            TChangeLog changeLog(tempFile.Name(), 0, 64);
            changeLog.Open();
        }
    }

    void TestFinalized()
    {
        TTempFile tempFile("tmp");
        TTempFile tempIndexFile("tmp.index");
        const int logRecordCount = 256;
        {
            TChangeLog changeLog(tempFile.Name(), 0, 64);
            changeLog.Create(0);
            for (i32 recordId = 0; recordId < logRecordCount; ++recordId) {
                TBlob blob(sizeof(ui32));
                *reinterpret_cast<ui32*>(blob.begin()) = static_cast<ui32>(recordId);
                changeLog.Append(recordId, TSharedRef(blob));
            }
            changeLog.Flush();
            UNIT_ASSERT_EQUAL(changeLog.IsFinalized(), false);
            changeLog.Finalize();
            UNIT_ASSERT_EQUAL(changeLog.IsFinalized(), true);
        }
        {
            TChangeLog changeLog(tempFile.Name(), 0, 64);
            changeLog.Open();
            UNIT_ASSERT_EQUAL(changeLog.IsFinalized(), true);
        }
    }

    void TestReadWrite()
    {
        TTempFile tempFile("tmp");
        TTempFile tempIndexFile("tmp.index");
        const int logRecordCount = 256;
        {
            TChangeLog changeLog(tempFile.Name(), 0, 128);
            changeLog.Create(0);
            for (i32 recordId = 0; recordId < logRecordCount; ++recordId) {
                TBlob blob(sizeof(ui32));
                *reinterpret_cast<ui32*>(blob.begin()) = static_cast<ui32>(recordId);
                changeLog.Append(recordId, TSharedRef(blob));
            }
            changeLog.Flush();
            UNIT_ASSERT_EQUAL(changeLog.GetRecordCount(), logRecordCount);
            CheckReads<ui32>(&changeLog, logRecordCount);
        }
        {
            TChangeLog changeLog(tempFile.Name(), 0, 128);
            changeLog.Open();
            UNIT_ASSERT_EQUAL(changeLog.GetRecordCount(), logRecordCount);
            CheckReads<ui32>(&changeLog, logRecordCount);
        }
    }

    void TestCorrupted()
    {
        TTempFile tempFile("tmp");
        TTempFile tempIndexFile("tmp.index");
        const int logRecordCount = 1024;
        {
            TChangeLog changeLog(tempFile.Name(), 0, 128);
            changeLog.Create(0);
            for (i32 recordId = 0; recordId < logRecordCount; ++recordId) {
                TBlob blob(sizeof(ui32));
                *reinterpret_cast<ui32*>(blob.begin()) = static_cast<ui32>(recordId);
                changeLog.Append(recordId, TSharedRef(blob));
            }
            changeLog.Flush();
        }
        {   // truncate file
            TFile changeLogFile("tmp", RdWr);
            changeLogFile.Resize(changeLogFile.GetLength() - 1);
        }
        {
            TChangeLog changeLog(tempFile.Name(), 0, 128);
            changeLog.Open();

            UNIT_ASSERT_EQUAL(changeLog.GetRecordCount(), logRecordCount - 1);
            CheckRead<ui32>(&changeLog, 0, logRecordCount, logRecordCount - 1);

            TBlob blob(sizeof(i32));
            *reinterpret_cast<i32*>(blob.begin()) = static_cast<i32>(logRecordCount - 1);
            changeLog.Append(logRecordCount - 1, TSharedRef(blob));
            changeLog.Flush();

            UNIT_ASSERT_EQUAL(changeLog.GetRecordCount(), logRecordCount);
            CheckRead<ui32>(&changeLog, 0, logRecordCount, logRecordCount);
        }
        {
            TChangeLog changeLog(tempFile.Name(), 0, 128);
            changeLog.Open();

            UNIT_ASSERT_EQUAL(changeLog.GetRecordCount(), logRecordCount);
            CheckRead<ui32>(&changeLog, 0, logRecordCount, logRecordCount);
        }
    }

    void TestTruncate()
    {
        TTempFile tempFile("tmp");
        TTempFile tempIndexFile("tmp.index");
        const int logRecordCount = 256;
        {
            TChangeLog changeLog(tempFile.Name(), 0, 128);
            changeLog.Create(0);
            for (i32 recordId = 0; recordId < logRecordCount; ++recordId) {
                TBlob blob(sizeof(ui32));
                *reinterpret_cast<ui32*>(blob.begin()) = static_cast<ui32>(recordId);
                changeLog.Append(recordId, TSharedRef(blob));
            }
            changeLog.Flush();
            UNIT_ASSERT_EQUAL(changeLog.GetRecordCount(), logRecordCount);
            CheckRead<ui32>(&changeLog, 0, logRecordCount, logRecordCount);
        }
        for (int recordId = logRecordCount; recordId >= 0; --recordId) {
            {
                TChangeLog changeLog(tempFile.Name(), 0, 128);
                changeLog.Open();
                changeLog.Truncate(recordId);
            }
            {
                TChangeLog changeLog(tempFile.Name(), 0, 128);
                changeLog.Open();
                UNIT_ASSERT_EQUAL(changeLog.GetRecordCount(), recordId);
                CheckRead<ui32>(&changeLog, 0, recordId, recordId);
            }
        }
    }

    void TestTruncateAppend()
    {
        TTempFile tempFile("tmp");
        TTempFile tempIndexFile("tmp.index");
        const int logRecordCount = 256;
        {
            TChangeLog changeLog(tempFile.Name(), 0, 128);
            changeLog.Create(0);
            for (i32 recordId = 0; recordId < logRecordCount; ++recordId) {
                TBlob blob(sizeof(ui32));
                *reinterpret_cast<ui32*>(blob.begin()) = static_cast<ui32>(recordId);
                changeLog.Append(recordId, TSharedRef(blob));
            }
            changeLog.Flush();
            UNIT_ASSERT_EQUAL(changeLog.GetRecordCount(), logRecordCount);
            CheckRead<ui32>(&changeLog, 0, logRecordCount, logRecordCount);
        }
        int recordId = logRecordCount / 2;
        {
            //truncate
            TChangeLog changeLog(tempFile.Name(), 0, 128);
            changeLog.Open();
            changeLog.Truncate(recordId);
            CheckRead<ui32>(&changeLog, 0, recordId, recordId);
        }
        {
            //append
            TChangeLog changeLog(tempFile.Name(), 0, 128);
            changeLog.Open();
            for (i32 i = recordId; i < logRecordCount; ++i) {
                TBlob blob(sizeof(ui32));
                *reinterpret_cast<ui32*>(blob.begin()) = static_cast<ui32>(i);
                changeLog.Append(i, TSharedRef(blob));
            }
        }
        {
            //check
            TChangeLog changeLog(tempFile.Name(), 0, 128);
            changeLog.Open();
            CheckRead<ui32>(&changeLog, 0, recordId, recordId);
        }
    }

    void TestUnalighnedChecksum()
    {
        TTempFile tempFile("tmp");
        TTempFile tempIndexFile("tmp.index");
        const int logRecordCount = 256;
        {
            TChangeLog changeLog(tempFile.Name(), 0, 128);
            changeLog.Create(0);
            for (i32 recordId = 0; recordId < logRecordCount; ++recordId) {
                TBlob blob(sizeof(ui8));
                *reinterpret_cast<ui8*>(blob.begin()) = static_cast<ui8>(recordId);
                changeLog.Append(recordId, TSharedRef(blob));
            }
        }
        {
            TChangeLog changeLog(tempFile.Name(), 0, 128);
            changeLog.Open();
            CheckRead<ui8>(&changeLog, 0, logRecordCount, logRecordCount);
        }
    }
};

UNIT_TEST_SUITE_REGISTRATION(TChangeLogTest);

} // namespace NYT
