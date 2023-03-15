#include <yt/cpp/roren/bigrt/bigrt.h>
#include <yt/cpp/roren/bigrt/bigrt_executor.h>
#include <yt/cpp/roren/bigrt/bigrt_execution_context.h>

#include <yt/cpp/roren/interface/roren.h>

#include <bigrt/lib/queue/message_batch/message_batch.h>
#include <bigrt/lib/supplier/config/supplier_config.pb.h>

#include <library/cpp/testing/gtest/gtest.h>

using namespace NRoren;

NBigRT::TMessageBatch MakeDataMessageWithNParts(int partsCount)
{
    NBigRT::TMessageBatch result;
    result.Messages.resize(partsCount);
    return result;
}

TEST(ResharderExecutor, Simple)
{
    auto tag = TTypeTag<int>{"out"};

    auto pipeline = MakeBigRtDummyPipeline();

    pipeline | ReadMessageBatch() | ParDo([] (const NBigRT::TMessageBatch& input) {
        return static_cast<int>(input.Messages.size());
    }) | ResharderMemoryWrite(tag);

    auto executorPool = MakeExecutorPool(pipeline);
    auto executor = executorPool->AcquireExecutor();
    executor.Start(NRoren::NPrivate::CreateDummyBigRtExecutionContext());
    executor.Do(MakeDataMessageWithNParts(3));
    executor.Do(MakeDataMessageWithNParts(6));
    auto memoryStorage = executor.Finish();

    const auto& actualResult = memoryStorage->GetRowList(tag);
    const auto& expectedResult = std::vector<int>{3, 6};
    GTEST_ASSERT_EQ(actualResult, expectedResult);
}

TEST(ResharderExecutor, MultipleOutput)
{
    auto mainTag = TTypeTag<int>{"mainTag"};
    auto doubledTag = TTypeTag<int>{"doubledTag"};

    auto pipeline = MakeBigRtDummyPipeline();

    auto textLengths = pipeline | ReadMessageBatch() | ParDo([] (const NBigRT::TMessageBatch& input) {
        return static_cast<int>(input.Messages.size());
    });
    textLengths | ResharderMemoryWrite(mainTag);
    textLengths | ParDo([] (const int& v) {
        return v * 2;
    }) | ResharderMemoryWrite(doubledTag);

    auto executorPool = MakeExecutorPool(pipeline);
    auto executor = executorPool->AcquireExecutor();
    executor.Start(NRoren::NPrivate::CreateDummyBigRtExecutionContext());
    executor.Do(MakeDataMessageWithNParts(3));
    executor.Do(MakeDataMessageWithNParts(6));
    auto memoryStorage = executor.Finish();

    const auto& actualMainResult = memoryStorage->GetRowList(mainTag);
    const auto& expectedMainResult = std::vector<int>{3, 6};
    GTEST_ASSERT_EQ(actualMainResult, expectedMainResult);

    const auto& actualDoubledResult = memoryStorage->GetRowList(doubledTag);
    const auto& expectedDoubledResult = std::vector<int>{6, 12};
    GTEST_ASSERT_EQ(actualDoubledResult, expectedDoubledResult);
}
