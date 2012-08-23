#include "stdafx.h"

#include <ytlib/meta_state/async_change_log.h>
#include <ytlib/meta_state/change_log.h>

#include <util/random/random.h>
#include <util/system/tempfile.h>

#include <contrib/testing/framework.h>

namespace NYT {
namespace NMetaState {

////////////////////////////////////////////////////////////////////////////////

class TAsyncChangeLogTest
    : public ::testing::Test
{
protected:
    THolder<TTempFile> TemporaryFile;
    THolder<TTempFile> TemporaryIndexFile;

    TChangeLogPtr ChangeLog;
    THolder<TAsyncChangeLog> AsyncChangeLog;

    TActionQueuePtr ActionQueue;
    IInvokerPtr Invoker;

    virtual void SetUp()
    {
        TemporaryFile.Reset(new TTempFile(GenerateRandomFileName("AsyncChangeLog")));
        TemporaryIndexFile.Reset(new TTempFile(TemporaryFile->Name() + ".index"));

        ChangeLog = New<TChangeLog>(TemporaryFile->Name(), 0, /*index block size*/ 64);
        ChangeLog->Create(0, TEpochId());

        AsyncChangeLog.Reset(new TAsyncChangeLog(ChangeLog));

        ActionQueue = New<TActionQueue>();
        Invoker = ActionQueue->GetInvoker();
    }

    virtual void TearDown()
    { }

public:
    static void CheckRecord(ui32 data, std::vector<TSharedRef>& result)
    {
        EXPECT_EQ(1, result.size());
        TSharedRef record = result[0];
        EXPECT_EQ(sizeof(data), record.Size());
        EXPECT_EQ(       data , *(reinterpret_cast<ui32*>(record.Begin())));
    }
};

TVoid ReadRecord(TAsyncChangeLog* asyncChangeLog, ui32 recordId) {
    std::vector<TSharedRef> result;
    result.clear();
    asyncChangeLog->Read(recordId, 1, &result);
    TAsyncChangeLogTest::CheckRecord(recordId, result);
    return TVoid();
}

TSharedRef CreateSharedRef(ui32 data)
{
    TBlob blob(sizeof(ui32));
    *reinterpret_cast<ui32*>(&*blob.begin()) = static_cast<ui32>(data);
    return TSharedRef(MoveRV(blob));
}

TEST_F(TAsyncChangeLogTest, ReadLastOnes)
{
    ui32 recordCount = 10000;
    TFuture<TVoid> result;
    for (ui32 recordId = 0; recordId < recordCount; ++recordId) {
        auto flushResult = AsyncChangeLog->Append(recordId, CreateSharedRef(recordId));
        if (recordId % 1000 == 0) {
            flushResult.Get();
        }
        if (recordId % 10 == 0) {
            result = BIND(&ReadRecord, ~AsyncChangeLog, recordId).AsyncVia(Invoker).Run();
        }
    }
    result.Get();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NMetaState
} // namespace NYT
