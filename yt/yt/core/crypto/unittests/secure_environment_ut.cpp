#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/crypto/secure_environment.h>

#include <yt/yt/library/undumpable/undumpable.h>

#include <util/system/env.h>

namespace NYT::NCrypto {
namespace {

////////////////////////////////////////////////////////////////////////////////

TEST(TSecureEnvironmentTest, Singleton)
{
    auto env1 = GetSecureEnvironment();
    auto env2 = GetSecureEnvironment();
    EXPECT_EQ(env1.Get(), env2.Get());
}

TEST(TSecureEnvironmentTest, SetFindRemove)
{
    auto env = GetSecureEnvironment();
    env->Set("test_key_1", "test_value_1");

    auto value = env->Find<TString>("test_key_1");
    ASSERT_TRUE(value.has_value());
    EXPECT_EQ(*value, "test_value_1");

    // Cleanup.
    EXPECT_TRUE(env->Remove("test_key_1"));
    ASSERT_FALSE(env->Contains("test_key_1"));

    // Removing again returns false.
    EXPECT_FALSE(env->Remove("test_key_1"));

    ASSERT_FALSE(env->Contains("nonexistent_key"));
    EXPECT_FALSE(env->Find<TString>("nonexistent_key").has_value());
}

TEST(TSecureEnvironmentTest, ListKeys)
{
    auto env = GetSecureEnvironment();
    env->Set("list_key_1", "v1");
    env->Set("list_key_2", "v2");

    auto keys = env->ListKeys();
    EXPECT_TRUE(std::find(keys.begin(), keys.end(), "list_key_1") != keys.end());
    EXPECT_TRUE(std::find(keys.begin(), keys.end(), "list_key_2") != keys.end());

    env->Remove("list_key_1");
    env->Remove("list_key_2");
}

TEST(TSecureEnvironmentTest, Zeroing)
{
    auto env = GetSecureEnvironment();
    env->Set("test_key_2", "test_value_1");
    auto value1 = env->FindYson("test_key_2").AsStringBuf();
    EXPECT_EQ(value1, "\x1\x18test_value_1");

    // Overwrite.
    env->Set("test_key_2", "test_value_2");
    auto value2 = env->FindYson("test_key_2").AsStringBuf();
    EXPECT_EQ(value2, "\x1\x18test_value_2");

    // Previous value is zeroed.
    EXPECT_NE(value1.data(), value2.data());
    EXPECT_EQ(value1.find_first_not_of('\0'), std::string_view::npos);

    auto value = env->Find<TString>("test_key_2");
    ASSERT_TRUE(value.has_value());
    EXPECT_EQ(*value, "test_value_2");

    // Cleanup.
    env->Remove("test_key_2");
    EXPECT_EQ(value2.find_first_not_of('\0'), std::string_view::npos);
}

TEST(TSecureEnvironmentTest, UndumpableMemory)
{
    auto env = GetSecureEnvironment();
    auto sizeBefore = GetUndumpableMemorySize();

    // Insert a value large enough to trigger a new pool chunk allocation.
    TString largeValue(1<<20, 'X');
    env->Set("undumpable_key", largeValue);
    auto sizeAfter = GetUndumpableMemorySize();
    EXPECT_GE(sizeAfter, sizeBefore + largeValue.size());

    env->Remove("undumpable_key");

    auto sizeAfterRemove = GetUndumpableMemorySize();
    EXPECT_GE(sizeAfterRemove, sizeAfter);
}

TEST(TSecureEnvironmentTest, MoveEnv)
{
    // Move undefined variable.
    MoveToSecureEnvironment(std::vector<TStringBuf>{"YT_SECURE_TEST_MISSING"});
    EXPECT_FALSE(GetSecureEnvironment()->Find<TString>("YT_SECURE_TEST_MISSING").has_value());

    // Move defined variables.
    SetEnv("YT_SECURE_TEST_EXACT", "secret_value");
    SetEnv("YT_SECURE_TEST_PREFIX_TEST", "secret_value_2");

    MoveToSecureEnvironment(std::vector<TStringBuf>{"YT_SECURE_TEST_EXACT"}, std::vector<TStringBuf>{"YT_SECURE_TEST_PREFIX_"});

    // Value is now in secure env.
    auto secureValue = GetSecureEnvironment()->Find<TString>("YT_SECURE_TEST_EXACT");
    ASSERT_TRUE(secureValue.has_value());
    EXPECT_EQ(*secureValue, "secret_value");

    auto secureValue2 = GetSecureEnvironment()->Find<TString>("YT_SECURE_TEST_PREFIX_TEST");
    ASSERT_TRUE(secureValue2.has_value());
    EXPECT_EQ(*secureValue2, "secret_value_2");

    // Original env string has been filled with '*'.
    EXPECT_EQ(GetEnv("YT_SECURE_TEST_EXACT"), std::string(secureValue->size(), '*'));
    EXPECT_EQ(GetEnv("YT_SECURE_TEST_PREFIX_TEST"), std::string(secureValue2->size(), '*'));

    // Cleanup secure env.
    GetSecureEnvironment()->Remove("YT_SECURE_TEST_EXACT");
    GetSecureEnvironment()->Remove("YT_SECURE_TEST_PREFIX_TEST");
    UnsetEnv("YT_SECURE_TEST_EXACT");
    UnsetEnv("YT_SECURE_TEST_PREFIX_TEST");
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NCrypto
