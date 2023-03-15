#include <yt/cpp/roren/interface/roren.h>
#include <yt/cpp/roren/local/local.h>
#include <yt/cpp/roren/local/vector_io.h>
#include <yt/cpp/roren/transforms/sum.h>

#include <library/cpp/testing/gtest/gtest.h>


using namespace NRoren;

TEST(Flatten, Simple)
{
    auto pipeline = MakeLocalPipeline();

    auto input1 = pipeline | VectorRead(std::vector<int>{1, 3, 5, 7});
    auto input2 = pipeline | VectorRead(std::vector<int>{2, 4, 6, 8, 10, 12});

    auto flattened = Flatten(std::vector{input1, input2});

    std::vector<int> result;
    flattened | VectorWrite(& result);

    pipeline.Run();

    std::sort(result.begin(), result.end());

    const auto expected = std::vector<int>{1, 2, 3, 4, 5, 6, 7, 8, 10, 12};
    ASSERT_EQ(result, expected);
}
