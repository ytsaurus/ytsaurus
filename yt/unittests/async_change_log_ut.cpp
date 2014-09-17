#include "stdafx.h"
#include "framework.h"

#include <ytlib/meta_state/async_change_log.h>
#include <ytlib/meta_state/change_log.h>

#include <util/random/random.h>
#include <util/system/tempfile.h>

namespace NYT {
namespace NMetaState {
namespace {

using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

class TAsyncChangeLogTest
    : public ::testing::Test
{
protected:
    std::unique_ptr<TTempFile> TemporaryFile;
    std::unique_ptr<TTempFile> TemporaryIndexFile;

    TChangeLogPtr ChangeLog;
    std::unique_ptr<TAsyncChangeLog> AsyncChangeLog;

    TActionQueuePtr ActionQueue;
    IInvokerPtr Invoker;

    virtual void SetUp()
    {
        TemporaryFile.reset(new TTempFile(GenerateRandomFileName("AsyncChangeLog")));
        TemporaryIndexFile.reset(new TTempFile(TemporaryFile->Name() + ".index"));

        ChangeLog = New<TChangeLog>(TemporaryFile->Name(), 0, /*index block size*/ 64);
        ChangeLog->Create(0, TEpochId());

        AsyncChangeLog.reset(new TAsyncChangeLog(ChangeLog));

        ActionQueue = New<TActionQueue>();
        Invoker = ActionQueue->GetInvoker();
    }

    virtual void TearDown()
    {
        NFS::Remove(TemporaryFile->Name());
        NFS::Remove(TemporaryIndexFile->Name());
    }

};

namespace {

static void CheckRecord(i32 data, const TSharedRef& record)
{
    EXPECT_EQ(sizeof(data), record.Size());
    EXPECT_EQ(       data , *(reinterpret_cast<const i32*>(record.Begin())));
}

void ReadRecord(TAsyncChangeLog* asyncChangeLog, i32 recordIndex)
{
    std::vector<TSharedRef> result;
    result.clear();
    asyncChangeLog->Read(recordIndex, 1, std::numeric_limits<i64>::max(), &result);
    EXPECT_EQ(1, result.size());
    CheckRecord(recordIndex, result[0]);
}

TSharedRef MakeData(i32 data)
{
    auto result = TSharedRef::Allocate(sizeof(i32));
    *reinterpret_cast<i32*>(&*result.Begin()) = static_cast<i32>(data);
    return result;
}

} // namespace

TEST_F(TAsyncChangeLogTest, ReadTrailingRecords)
{
    const int recordCount = 10000;
    TFuture<void> readResult;
    for (int recordIndex = 0; recordIndex < recordCount; ++recordIndex) {
        auto flushResult = AsyncChangeLog->Append(recordIndex, MakeData(recordIndex));
        if (recordIndex % 1000 == 0) {
            flushResult.Get();
        }
        if (recordIndex % 10 == 0) {
            readResult = BIND(&ReadRecord, ~AsyncChangeLog, recordIndex).AsyncVia(Invoker).Run();
        }
    }
    readResult.Get();
}

TEST_F(TAsyncChangeLogTest, ReadWithSizeLimit)
{
    for (int recordIndex = 0; recordIndex < 40; ++recordIndex) {
        AsyncChangeLog->Append(recordIndex, MakeData(recordIndex));
    }

    auto check = [&] (int maxSize) {
        std::vector<TSharedRef> records;
        AsyncChangeLog->Read(0, 1000, maxSize, &records);
        EXPECT_EQ(records.size(), maxSize / sizeof(i32) + 1);
        for (int recordIndex = 0; recordIndex < static_cast<int>(records.size()); ++recordIndex) {
            CheckRecord(recordIndex, records[recordIndex]);
        }
    };

    check(1);
    check(10);
    check(40);
    check(100);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NMetaState
} // namespace NYT
