#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/flow/library/cpp/delta_codecs/codec.h>
#include <yt/yt/flow/library/cpp/delta_codecs/state.h>

namespace NYT::NFlow::NDeltaCodecs {
namespace {

////////////////////////////////////////////////////////////////////////////////

TEST(TVCDiffTest, Basic)
{
    auto data = TSharedRef::FromString(std::string("abracabra"));
    auto newData = TSharedRef::FromString(std::string("bracacabra"));
    auto mutation = GetCodec(ECodec::VCDiff)->TryComputePatch(data, newData);
    ASSERT_TRUE(mutation);
    EXPECT_FALSE(mutation->ToStringBuf().empty());
    auto result = GetCodec(ECodec::VCDiff)->ApplyPatch(data, *mutation);
    EXPECT_EQ(newData.ToStringBuf(), result.ToStringBuf());
}

TEST(TVCDiffTest, Equal)
{
    auto data = TSharedRef::FromString(std::string("abracabra"));
    auto newData = TSharedRef::FromString(std::string("abracabra"));
    auto mutation = GetCodec(ECodec::VCDiff)->TryComputePatch(data, newData);
    ASSERT_TRUE(mutation);
    EXPECT_TRUE(mutation->ToStringBuf().empty());
}

TEST(TVCDiffTest, EmptyPatch)
{
    auto data = TSharedRef::FromString(std::string("abracdabra"));
    auto result = GetCodec(ECodec::VCDiff)->ApplyPatch(data, TSharedRef::MakeEmpty());
    EXPECT_EQ(data.ToStringBuf(), result.ToStringBuf());
}

TEST(TVCDiffTest, EmptyBase)
{
    auto newData = TSharedRef::FromString(std::string("abracabra"));
    auto mutation = GetCodec(ECodec::VCDiff)->TryComputePatch(TSharedRef::MakeEmpty(), newData);
    ASSERT_TRUE(mutation);
    auto result = GetCodec(ECodec::VCDiff)->ApplyPatch(TSharedRef::MakeEmpty(), *mutation);
    EXPECT_EQ(newData.ToStringBuf(), result.ToStringBuf());
}

TEST(TVCDiffTest, EmptyValue)
{
    auto data = TSharedRef::FromString(std::string("abracabra"));
    auto mutation = GetCodec(ECodec::VCDiff)->TryComputePatch(data, TSharedRef::MakeEmpty());
    ASSERT_TRUE(mutation);
    auto result = GetCodec(ECodec::VCDiff)->ApplyPatch(data, *mutation);
    EXPECT_TRUE(result.ToStringBuf().empty());
}

TEST(TVCDiffTest, RawStreamWithoutFlowFraming)
{
    // The wire format is frozen: a raw open-vcdiff stream starting with the
    // VCDiff magic, without the size prefix flow xdelta patches carry.
    auto data = TSharedRef::FromString(std::string("abracabra"));
    auto newData = TSharedRef::FromString(std::string("bracacabra"));
    auto mutation = GetCodec(ECodec::VCDiff)->TryComputePatch(data, newData);
    ASSERT_TRUE(mutation);
    auto patch = mutation->ToStringBuf();
    ASSERT_GE(patch.size(), 3u);
    EXPECT_EQ(static_cast<unsigned char>(patch[0]), 0xD6);
    EXPECT_EQ(static_cast<unsigned char>(patch[1]), 0xC3);
    EXPECT_EQ(static_cast<unsigned char>(patch[2]), 0xC4);
}

TEST(TVCDiffTest, CorruptPatch)
{
    auto data = TSharedRef::FromString(std::string("abracabra"));
    auto patch = TSharedRef::FromString(std::string("\x01\x02\x03garbage"));
    EXPECT_THROW_WITH_SUBSTRING(
        GetCodec(ECodec::VCDiff)->ApplyPatch(data, patch),
        "Failed to decode by VCDiff");
}

TEST(TVCDiffTest, LargeRoundTrip)
{
    std::string base;
    std::string value;
    for (int i = 0; i < 10000; ++i) {
        base += "prefix" + std::to_string(i * 17 % 1000);
        value += "prefix" + std::to_string(i * 17 % 1000);
        if (i % 100 == 0) {
            value += "-edited";
        }
    }
    auto baseRef = TSharedRef::FromString(std::string(base));
    auto valueRef = TSharedRef::FromString(std::string(value));
    auto mutation = GetCodec(ECodec::VCDiff)->TryComputePatch(baseRef, valueRef);
    ASSERT_TRUE(mutation);
    EXPECT_LT(mutation->Size(), valueRef.Size());
    auto result = GetCodec(ECodec::VCDiff)->ApplyPatch(baseRef, *mutation);
    EXPECT_EQ(valueRef.ToStringBuf(), result.ToStringBuf());
}

////////////////////////////////////////////////////////////////////////////////

TEST(TVCDiffStateTest, MutateRoundTrip)
{
    const auto initialValue = TSharedRef::FromString(std::string("dracabara"));
    const auto initialBase = TSharedRef::FromString(std::string("abracabra"));
    const auto initialPatch = GetCodec(ECodec::VCDiff)->TryComputePatch(initialBase, initialValue);
    ASSERT_TRUE(initialPatch);
    auto state = TState{.Base = initialBase, .Patch = *initialPatch};

    const auto newValue = TSharedRef::FromString(std::string("darabara"));

    auto mutation = MutateState(GetCodec(ECodec::VCDiff), state, newValue, EAlgorithm::ForcePatch);
    EXPECT_FALSE(mutation.Base);
    ASSERT_TRUE(mutation.Patch);
    auto merged = GetCodec(ECodec::VCDiff)->ApplyPatch(state.Base, *mutation.Patch);
    EXPECT_EQ(merged.ToStringBuf(), newValue.ToStringBuf());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow::NDeltaCodecs
