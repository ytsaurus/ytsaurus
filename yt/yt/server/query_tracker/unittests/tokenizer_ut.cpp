#include <yt/yt/server/query_tracker/config.h>
#include <yt/yt/server/query_tracker/query_tracker_proxy.h>
#include <yt/yt/server/query_tracker/tokenizer.h>

#include <yt/yt/ytlib/query_tracker_client/records/query.record.h>

#include <yt/yt/client/transaction_client/noop_timestamp_provider.h>

#include <yt/yt/client/unittests/mock/client.h>
#include <yt/yt/client/unittests/mock/transaction.h>

#include <yt/yt/core/concurrency/action_queue.h>

namespace NYT::NQueryTracker {
namespace {

////////////////////////////////////////////////////////////////////////////////

class TTokenizerTest
    : public ::testing::Test
{
public:
    void SetUp() override
    {
        Tokenizer_ = CreateRegexTokenizer();
    }

    std::unordered_map<std::string, i64> GetDifference(
        const std::string& text,
        ETokenizationMode tokenizationMode,
        std::unordered_map<std::string, i64> expectedResult)
    {
        auto tokens = Tokenize(text, Tokenizer_, tokenizationMode);
        for (const auto& parsedToken : tokens) {
            if (expectedResult[parsedToken.Token] -= parsedToken.Occurrences == 0) {
                expectedResult.erase(parsedToken.Token);
            }
        }
        return expectedResult;
    }

private:
    IQueryTokenizerPtr Tokenizer_;
};

TEST_F(TTokenizerTest, StandardTokenization)
{
    EXPECT_TRUE(GetDifference(
        "some_token 123",
        ETokenizationMode::Standard,
        {
            {"some_token", 1},
            {"123", 1},
        }
    ).empty());

    EXPECT_TRUE(GetDifference(
        "one_symbol_tokens 1 a A * ! 12 123 1234",
        ETokenizationMode::Standard,
        {
            {"one_symbol_tokens", 1},
            {"12", 1},
            {"123", 1},
            {"1234", 1},
        }
    ).empty());

    EXPECT_TRUE(GetDifference(
        "simple_operations 123-123+123/123*123",
        ETokenizationMode::Standard,
        {
            {"simple_operations", 1},
            {"123", 5},
        }
    ).empty());

    EXPECT_TRUE(GetDifference(
        "corrtct splitting 111\t111\n111\\111/111::111'111'(111)[111]$111",
        ETokenizationMode::Standard,
        {
            {"corrtct", 1},
            {"splitting", 1},
            {"111", 10},
        }
    ).empty());

    EXPECT_TRUE(GetDifference(
        "stop_words select from where",
        ETokenizationMode::Standard,
        {
            {"stop_words", 1},
        }
    ).empty());

    EXPECT_TRUE(GetDifference(
        "special_aco_construction_for_selecting_acos 'aco:some_aco'",
        ETokenizationMode::Standard,
        {
            {"special_aco_construction_for_selecting_acos", 1},
            {"aco", 1},
            {"some_aco", 1},
        }
    ).empty());
}

TEST_F(TTokenizerTest, SearchTokenization)
{
    EXPECT_TRUE(GetDifference(
        "some_token 123",
        ETokenizationMode::ForSearch,
        {
            {"some_token", 1},
            {"123", 1},
        }
    ).empty());

    EXPECT_TRUE(GetDifference(
        "one_symbol_tokens 1 a A * ! 12 123 1234",
        ETokenizationMode::ForSearch,
        {
            {"one_symbol_tokens", 1},
            {"12", 1},
            {"123", 1},
            {"1234", 1},
        }
    ).empty());

    EXPECT_TRUE(GetDifference(
        "simple_operations 123-123+123/123*123",
        ETokenizationMode::ForSearch,
        {
            {"simple_operations", 1},
            {"123", 5},
        }
    ).empty());

    EXPECT_TRUE(GetDifference(
        "corrtct splitting 111\t111\n111\\111/111::111'111'(111)[111]$111",
        ETokenizationMode::ForSearch,
        {
            {"corrtct", 1},
            {"splitting", 1},
            {"111", 10},
        }
    ).empty());

    EXPECT_TRUE(GetDifference(
        "special_aco_construction_for_selecting_acos 'aco:some_aco'",
        ETokenizationMode::ForSearch,
        {
            {"special_aco_construction_for_selecting_acos", 1},
            {"aco:some_aco", 1},
        }
    ).empty());

    EXPECT_TRUE(GetDifference(
        "stop_words select from where",
        ETokenizationMode::ForSearch,
        {
            {"stop_words", 1},
        }
    ).empty());

    EXPECT_TRUE(GetDifference(
        "special_aco_construction_for_selecting_acos_with_strange_symbols 'aco:%^&*!_-+/\\\"`' ",
        ETokenizationMode::ForSearch,
        {
            {"special_aco_construction_for_selecting_acos_with_strange_symbols", 1},
            {"aco:%^&*!_-+/\\\"`", 1},
        }
    ).empty());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NQueryTracker
