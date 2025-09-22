#include "tokenizer.h"

#include <yt/yt/ytlib/api/native/client.h>
#include <yt/yt/ytlib/api/native/connection.h>

#include <yt/yt/ytlib/query_tracker_client/records/query.record.h>

#include <yt/yt/ytlib/yql_client/yql_service_proxy.h>

#include <yt/yt/core/rpc/roaming_channel.h>

#include <regex>

namespace NYT::NQueryTracker {

using namespace NQueryTrackerClient;
using namespace NApi;
using namespace NYPath;
using namespace NHiveClient;
using namespace NYTree;
using namespace NRpc;
using namespace NYqlClient;
using namespace NYqlClient::NProto;
using namespace NYson;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

static NLogging::TLogger Logger("Tokenizers");

const std::string DefaultYqlAgentStageName = "production";
const std::unordered_set<std::string> UnindexedTokens = {"select", "from", "where"};

// Regex for splitting text filters and queries started using query tracker into tokens.
// Divides text by substrings consisting of characters "().,;:[]{}$*'"`<>=-+\|/%".
std::regex DefaultTokenRegex(R"([\(\)\.\,\;\[\]\{\}\$\*\s\:\'\"\`\>\<\=\+\/\|\\\%\-]+)");
// Searches for occurrences of subsrings like "aco:some-aco" in query
std::regex DefaultAcoRegex(R"(\"aco:.+?\"|'aco:.+?'|`aco:.+?`)");

constexpr i64 TokenizerSymbolLimit = 4096;

////////////////////////////////////////////////////////////////////////////////

class TRegexTokenizer
    : public IQueryTokenizer
{
public:
    TRegexTokenizer() = default;

    std::vector<std::string> Tokenize(const std::string& query, ETokenizationMode tokenizationMode) override
    {
        std::vector<std::string> result;
        auto toLower = [](unsigned char c){
            return std::tolower(c);
        };
        auto tokenFilter = [](const std::string& token) {
            return (token.size() <= 1 || UnindexedTokens.contains(token));
        };

        std::optional<std::string> queryAfterReplace;

        // Looking for ACOs in search query text.
        if (tokenizationMode == ETokenizationMode::ForSearch) {
            std::sregex_token_iterator itBegin{query.begin(), query.end(), DefaultAcoRegex};
            std::sregex_token_iterator itEnd;

            for (; itBegin != itEnd; ++itBegin) {
                auto token = itBegin->str();
                result.push_back(token.substr(1, token.size() - 2));
            }

            queryAfterReplace = std::regex_replace(query, DefaultAcoRegex, " ");
        }

        std::sregex_token_iterator itBegin{
            queryAfterReplace ? queryAfterReplace->begin() : query.begin(),
            queryAfterReplace ? queryAfterReplace->end() : query.end(),
            DefaultTokenRegex,
            -1};
        std::sregex_token_iterator itEnd;

        for (; itBegin != itEnd; ++itBegin) {
            auto token = itBegin->str();
            std::transform(token.begin(), token.end(), token.begin(), toLower);
            if (!tokenFilter(token)) {
                result.push_back(token);
            }
        }

        return result;
    }
};

IQueryTokenizerPtr CreateRegexTokenizer()
{
    return New<TRegexTokenizer>();
}

////////////////////////////////////////////////////////////////////////////////

std::vector<TParsedToken> Tokenize(
    const std::string& query,
    IQueryTokenizerPtr tokenizer,
    ETokenizationMode tokenizationMode)
{
    std::unordered_map<std::string, i64> counter;
    std::optional<std::string> strippedQuery;

    if (query.size() > TokenizerSymbolLimit) {
        strippedQuery = std::string(query.begin(), std::next(query.begin(), TokenizerSymbolLimit));
    }

    auto tokens = tokenizer->Tokenize(strippedQuery ? strippedQuery.value() : query, tokenizationMode);

    for (const auto& token : tokens) {
        ++counter[token];
    }

    std::vector<TParsedToken> result;
    result.reserve(counter.size());
    for (const auto& [token, occurrences] : counter) {
        result.emplace_back(token, occurrences);
    }

    YT_LOG_DEBUG("Query parsed (TokenOccurences: %v)", MakeShrunkFormattableView(
        result,
        [] (
            TStringBuilderBase* builder,
            const TParsedToken& entry
        ) {
            builder->AppendFormat("%v: %v", entry.Token, entry.Occurrences);
        },
        20));

    return result;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryClient
