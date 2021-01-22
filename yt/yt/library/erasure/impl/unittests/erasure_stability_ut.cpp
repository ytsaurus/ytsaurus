#include <yt/core/test_framework/framework.h>

#include <yt/library/erasure/impl/codec.h>

#include <util/random/random.h>

namespace NYT::NErasure {
namespace {

////////////////////////////////////////////////////////////////////////////////

class TErasureStabilityTest
    : public ::testing::Test
    , public ::testing::WithParamInterface<
        std::tuple<ECodec, std::vector<unsigned char>>>
{
public:
    TCodecTraits::TBufferType GenerateDataBuffer(int iter, int wordSize)
    {
        std::vector<unsigned char> data(wordSize);
        for (int i = 0; i < wordSize; ++i) {
            data[i] = RandomNumber<unsigned char>();
        }

        return TCodecTraits::TBufferType(0, data.data(), data.size());
    }
};

TEST_P(TErasureStabilityTest, TErasureStabilityTest)
{
    SetRandomSeed(42);
    const auto& params = GetParam();

    auto codec = GetCodec(std::get<0>(params));

    std::vector<TCodecTraits::TBlobType> dataParts;
    for (int i = 0; i < codec->GetDataPartCount(); ++i) {
        dataParts.push_back(TCodecTraits::FromBufferToBlob(GenerateDataBuffer(i, codec->GetWordSize())));
    }

    auto parities = codec->Encode(dataParts);
    auto expected = std::get<1>(params);

    EXPECT_EQ(expected.size(), parities.size());
    for (int i = 0; i < expected.size(); ++i) {
        // Check only the first element.
        EXPECT_EQ(static_cast<char>(expected[i]), *parities[i].Begin());
    }
}

INSTANTIATE_TEST_SUITE_P(
    TErasureStabilityTest,
    TErasureStabilityTest,
    ::testing::Values(
        std::make_tuple(
            TCodecTraits::ECodecType::IsaReedSolomon_3_3,
            std::vector<unsigned char>{59, 252, 207}),
        std::make_tuple(
            TCodecTraits::ECodecType::ReedSolomon_6_3,
            std::vector<unsigned char>{194, 8, 51}),
        std::make_tuple(
            TCodecTraits::ECodecType::JerasureLrc_12_2_2,
            std::vector<unsigned char>{194, 201, 87, 67}),
        std::make_tuple(
            TCodecTraits::ECodecType::IsaLrc_12_2_2,
            std::vector<unsigned char>{194, 201, 104, 219}),
        std::make_tuple(
            TCodecTraits::ECodecType::IsaReedSolomon_6_3,
            std::vector<unsigned char>{194, 60, 234})));

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NErasure
