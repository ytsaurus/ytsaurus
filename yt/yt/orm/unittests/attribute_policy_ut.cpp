#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/orm/server/objects/attribute_policy.h>
#include <yt/yt/orm/server/objects/config.h>
#include <yt/yt/orm/server/objects/transaction.h>

namespace NYT::NOrm::NServer::NObjects::NTests {

using namespace NYT::NOrm::NServer::NObjects;

////////////////////////////////////////////////////////////////////////////////

namespace {

////////////////////////////////////////////////////////////////////////////////

TEST(TAttributePolicyTest, IsRandomStringAttributePolicyCorrect)
{
    const TString attributeName = "testRandomStringPolicyAttribute";
    const auto policy = CreateStringAttributePolicy(
        EAttributeGenerationPolicy::Random,
        1,
        16,
        "0123456789abcdefghijklmnopqrstuvwxyz#");
    policy->Validate("123", attributeName);
    policy->Validate("123abcd", attributeName);
    policy->Validate("a", attributeName);
    ASSERT_THROW(policy->Validate("", attributeName), TErrorException);
    ASSERT_THROW(policy->Validate("0123456789abcdefg", attributeName), TErrorException);
    ASSERT_THROW(policy->Validate("@", attributeName), TErrorException);
    for (size_t i = 0; i != 100000; ++i) {
        policy->Validate(policy->Generate(nullptr, attributeName), attributeName);
    }
}

TEST(TAttributePolicyTest, IsManualStringAttributePolicyCorrect)
{
    const TString attributeName = "testManualStringPolicyAttribute";
    const auto policy = CreateStringAttributePolicy(
        EAttributeGenerationPolicy::Manual,
        1,
        8,
        "abcd123");
    policy->Validate("123", attributeName);
    policy->Validate("123abcd", attributeName);
    policy->Validate("a", attributeName);
    ASSERT_THROW(policy->Validate("", attributeName), TErrorException);
    ASSERT_THROW(policy->Validate("123123123", attributeName), TErrorException);
    ASSERT_THROW(policy->Validate("x", attributeName), TErrorException);
    ASSERT_THROW(policy->Generate(nullptr, attributeName), TErrorException);
}

TEST(TAttributePolicyTest, IsManualInt32AttributePolicyCorrect) {
    const TString attributeName = "testManualInt32PolicyAttribute";
    const auto policy = CreateIntegerAttributePolicy<i32>(
        EAttributeGenerationPolicy::Manual,
        nullptr, // This attribute policy does not use bootstrap anyways.
        0,
        1000);
    for (i64 value = 0; value <= 1000; ++value) {
        policy->Validate(value, attributeName);
    }
    ASSERT_THROW(policy->Validate(-1, attributeName), TErrorException);
    ASSERT_THROW(policy->Validate(1001, attributeName), TErrorException);
    ASSERT_THROW(policy->Generate(nullptr, attributeName), TErrorException);
}

TEST(TAttributePolicyTest, IsRandomInt64AttributePolicyCorrect) {
    const TString attributeName = "testRandomInt64PolicyAttribute";
    const auto policy = CreateIntegerAttributePolicy<i64>(
        EAttributeGenerationPolicy::Random,
        nullptr, // This attribute policy does not use bootstrap anyways.
        0,
        1ULL << 60ULL);
    for (size_t i = 0; i != 100'000; ++i) {
        policy->Validate(policy->Generate(nullptr, attributeName), attributeName);
    }
}

} // namespace

} // namespace NYT::NOrm::NServer::NObjects::NTests
