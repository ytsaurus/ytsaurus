#include <yt/cpp/roren/interface/private/fn_attributes_ops.h>

#include <library/cpp/testing/gtest/gtest.h>

namespace NRoren {

using TFnAttributesOps = NPrivate::TFnAttributesOps;

TFnAttributes Merge(TFnAttributes a, TFnAttributes b)
{
    TFnAttributesOps::Merge(a, b);
    return a;
}

////////////////////////////////////////////////////////////////////////////////

TEST(TFnAttributesOps, Simple)
{
    const auto empty = TFnAttributes{};
    const auto pure = TFnAttributes{}.SetIsPure(true);
    const auto notPure = TFnAttributes{}.SetIsPure(false);
    const TString res1 = "res1";
    const TString res2 = "res2";
    const auto withRes1 = TFnAttributes{}.AddResourceFile(res1);
    const auto withRes2 = TFnAttributes{}.AddResourceFile(res2);

    EXPECT_EQ(TFnAttributesOps::GetIsPure(empty), false); // Empty is not pure, because pureness is not default feature.
    EXPECT_EQ(TFnAttributesOps::GetIsPure(pure), true);
    EXPECT_EQ(TFnAttributesOps::GetIsPure(notPure), false);

    EXPECT_EQ(TFnAttributesOps::GetIsPure(Merge(pure, pure)), true); // Result is pure only if every part is pure.
    EXPECT_EQ(TFnAttributesOps::GetIsPure(Merge(notPure, pure)), false);
    EXPECT_EQ(TFnAttributesOps::GetIsPure(Merge(pure, notPure)), false);
    EXPECT_EQ(TFnAttributesOps::GetIsPure(Merge(notPure, notPure)), false);

    EXPECT_THAT(TFnAttributesOps::GetResourceFileList(empty), ::testing::ElementsAre());
    EXPECT_THAT(TFnAttributesOps::GetResourceFileList(withRes1), ::testing::ElementsAre(res1));
    EXPECT_THAT(TFnAttributesOps::GetResourceFileList(Merge(empty, withRes1)), ::testing::ElementsAre(res1));
    EXPECT_THAT(TFnAttributesOps::GetResourceFileList(Merge(withRes1, empty)), ::testing::ElementsAre(res1));
    EXPECT_THAT(TFnAttributesOps::GetResourceFileList(Merge(withRes1, withRes2)), ::testing::ElementsAre(res1, res2));

    EXPECT_THAT(TFnAttributesOps::GetResourceFileList(Merge(withRes1, withRes1)), ::testing::ElementsAre(res1, res1)); // It is not desired, but actual behaviour.
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NRoren
