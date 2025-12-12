#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/flow/lib/delta_codecs/codec.h>
#include <yt/yt/flow/lib/delta_codecs/state.h>

namespace NYT::NFlow::NDeltaCodecs {
namespace {

////////////////////////////////////////////////////////////////////////////////

TEST(TXDeltaTest, Basic)
{
    auto data = TSharedRef::FromString(std::string("abracabra"));
    auto newData = TSharedRef::FromString(std::string("bracacabra"));
    auto mutation = GetCodec(ECodec::XDelta)->TryComputePatch(data, newData);
    ASSERT_TRUE(mutation);
    EXPECT_FALSE(mutation->ToStringBuf().empty());
    auto result = GetCodec(ECodec::XDelta)->ApplyPatch(data, *mutation);
    EXPECT_EQ(newData.ToStringBuf(), result.ToStringBuf());
}

TEST(TXDeltaTest, Equal)
{
    auto data = TSharedRef::FromString(std::string("abracabra"));
    auto newData = TSharedRef::FromString(std::string("abracabra"));
    auto mutation = GetCodec(ECodec::XDelta)->TryComputePatch(data, newData);
    ASSERT_TRUE(mutation);
    EXPECT_TRUE(mutation->ToStringBuf().empty());
}

TEST(TXDeltaTest, EmptyPatch)
{
    auto data = TSharedRef::FromString(std::string("abracdabra"));
    auto result = GetCodec(ECodec::XDelta)->ApplyPatch(data, TSharedRef::MakeEmpty());
    EXPECT_EQ(data.ToStringBuf(), result.ToStringBuf());
}

////////////////////////////////////////////////////////////////////////////////

TEST(TXDeltaStateTest, EmptyState)
{
    const auto initialBase = TSharedRef::MakeEmpty();
    const auto initialPatch = TSharedRef::MakeEmpty();
    const auto state = TState{.Base = initialBase, .Patch = initialPatch};
    const auto newValue = TSharedRef::FromString(std::string("abracabra"));

    for (auto algo : {EAlgorithm::ForcePatch, EAlgorithm::ZeroPatch, EAlgorithm::SizeHeuristics}) {
        auto mutation = MutateState(GetCodec(ECodec::XDelta), state, newValue, algo);
        ASSERT_TRUE(mutation.Base);
        EXPECT_EQ(mutation.Base->ToStringBuf(), newValue.ToStringBuf());
        EXPECT_FALSE(mutation.Patch);
    }

    for (auto algo : {EAlgorithm::ForcePatch, EAlgorithm::ZeroPatch, EAlgorithm::SizeHeuristics}) {
        auto mutation = MutateState(GetCodec(ECodec::XDelta), state, TSharedRef::MakeEmpty(), algo);
        ASSERT_FALSE(mutation.Base);
        ASSERT_FALSE(mutation.Patch);
    }
}

TEST(TXDeltaStateTest, EmptyPatch)
{
    const auto initialBase = TSharedRef::FromString(std::string("abracabra"));
    const auto initialPatch = TSharedRef::MakeEmpty();
    const auto state = TState{.Base = initialBase, .Patch = initialPatch};
    const auto newValue = TSharedRef::FromString(std::string("dracabara"));

    {
        auto mutation = MutateState(GetCodec(ECodec::XDelta), state, newValue, EAlgorithm::ZeroPatch);
        ASSERT_TRUE(mutation.Base);
        EXPECT_EQ(mutation.Base->ToStringBuf(), newValue.ToStringBuf());
        EXPECT_FALSE(mutation.Patch);
    }
    {
        auto mutation = MutateState(GetCodec(ECodec::XDelta), state, newValue, EAlgorithm::ForcePatch);
        EXPECT_FALSE(mutation.Base);
        ASSERT_TRUE(mutation.Patch);
        auto merged = GetCodec(ECodec::XDelta)->ApplyPatch(state.Base, *mutation.Patch);
        EXPECT_EQ(merged.ToStringBuf(), newValue.ToStringBuf());
    }
    for (auto algo : {EAlgorithm::ForcePatch, EAlgorithm::ZeroPatch, EAlgorithm::SizeHeuristics}) {
        auto mutation = MutateState(GetCodec(ECodec::XDelta), state, TSharedRef::MakeEmpty(), algo);
        ASSERT_TRUE(mutation.Base);
        EXPECT_TRUE(mutation.Base->ToStringBuf().empty());
        ASSERT_FALSE(mutation.Patch);
    }
}

TEST(TXDeltaStateTest, NonEmptyState)
{
    const auto initialValue = TSharedRef::FromString(std::string("dracabara"));
    const auto initialBase = TSharedRef::FromString(std::string("abracabra"));
    const auto initialPatch = GetCodec(ECodec::XDelta)->TryComputePatch(initialBase, initialValue);
    ASSERT_TRUE(initialPatch);
    auto state = TState{.Base = initialBase, .Patch = *initialPatch};

    const auto newValue = TSharedRef::FromString(std::string("darabara"));

    {
        auto mutation = MutateState(GetCodec(ECodec::XDelta), state, newValue, EAlgorithm::ZeroPatch);
        ASSERT_TRUE(mutation.Base);
        EXPECT_EQ(mutation.Base->ToStringBuf(), newValue.ToStringBuf());
        ASSERT_TRUE(mutation.Patch);
        EXPECT_TRUE(mutation.Patch->ToStringBuf().empty());
    }
    {
        auto mutation = MutateState(GetCodec(ECodec::XDelta), state, newValue, EAlgorithm::ForcePatch);
        EXPECT_FALSE(mutation.Base);
        ASSERT_TRUE(mutation.Patch);
        auto merged = GetCodec(ECodec::XDelta)->ApplyPatch(state.Base, *mutation.Patch);
        EXPECT_EQ(merged.ToStringBuf(), newValue.ToStringBuf());
    }
    for (auto algo : {EAlgorithm::ForcePatch, EAlgorithm::ZeroPatch, EAlgorithm::SizeHeuristics}) {
        auto mutation = MutateState(GetCodec(ECodec::XDelta), state, TSharedRef::MakeEmpty(), algo);
        ASSERT_TRUE(mutation.Base);
        EXPECT_TRUE(mutation.Base->ToStringBuf().empty());
        ASSERT_TRUE(mutation.Patch);
        EXPECT_TRUE(mutation.Patch->ToStringBuf().empty());
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow::NDeltaCodecs
