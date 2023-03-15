#include <yt/cpp/roren/interface/roren.h>
#include <yt/cpp/roren/local/local.h>
#include <yt/cpp/roren/library/text_lenval/text_lenval.h>

#include <bigrt/lib/queue/message_batch/message_batch.h>

#include <library/cpp/testing/gtest/gtest.h>


using namespace NRoren;

std::vector<int> GetChunkLengthsViaPipeline(TString input)
{
    auto pipeline = MakeLocalPipeline();

    std::vector<int> result;
    pipeline | ReadTextLenvalFromMemory(input) | ParDo([] (const NBigRT::TMessageBatch& batch, TOutput<int>& output) {
        for (auto&& message : batch.Messages) {
            output.Add(message.Data.size());
        }
    }) | VectorWrite<int>(&result);

    pipeline.Run();

    return result;
}

TEST(TextLenval, Simple) {
    EXPECT_EQ(
        GetChunkLengthsViaPipeline(
            "4\n"
            "1234\n"
            "3\n"
            "abc\n"
        ), std::vector({4, 3})
    );
}

TEST(TextLenval, BadFormat) {
    EXPECT_THAT(
        [] {
            GetChunkLengthsViaPipeline(
                "4\n"
                "12"
            );
        }, testing::ThrowsMessage<std::exception>(testing::HasSubstr("premature end of stream")));
}
