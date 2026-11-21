#include "shard.h"

#include <yt/yt/server/master/cell_master/serialize.h>

#include <yt/yt/core/misc/serialize.h>

#include <yt/yt/server/master/security_server/account.h>
#include <yt/yt/server/master/cypress_server/portal_exit_node.h>

#include <yt/yt/core/ypath/tokenizer.h>

#include <yt/yt/core/ytree/fluent.h>

#include <yt/yt/core/phoenix/type_def.h>

namespace NYT::NCypressServer {

using namespace NCellMaster;
using namespace NObjectClient;
using namespace NYPath;
using namespace NYson;
using namespace NYTree;

using NYPath::TTokenizer;
using NYPath::ETokenType;

////////////////////////////////////////////////////////////////////////////////

constinit const auto Logger = CypressServerLogger;

////////////////////////////////////////////////////////////////////////////////

bool TCypressShardAccountStatistics::IsZero() const
{
    return NodeCount == 0;
}

void TCypressShardAccountStatistics::RegisterMetadata(auto&& registrar)
{
    PHOENIX_REGISTER_FIELD(1, NodeCount);
}

PHOENIX_DEFINE_TYPE(TCypressShardAccountStatistics);

void Serialize(const TCypressShardAccountStatistics& statistics, IYsonConsumer* consumer)
{
    BuildYsonFluently(consumer)
        .BeginMap()
            .Item("node_count").Value(statistics.NodeCount)
        .EndMap();
}

TCypressShardAccountStatistics& operator+=(
    TCypressShardAccountStatistics& lhs,
    const TCypressShardAccountStatistics& rhs)
{
    lhs.NodeCount += rhs.NodeCount;
    return lhs;
}

TCypressShardAccountStatistics operator+(
    const TCypressShardAccountStatistics& lhs,
    const TCypressShardAccountStatistics& rhs)
{
    TCypressShardAccountStatistics result;
    result += lhs;
    result += rhs;
    return result;
}

////////////////////////////////////////////////////////////////////////////////

TCypressShardAccountStatistics TCypressShard::ComputeTotalAccountStatistics() const
{
    TCypressShardAccountStatistics result;
    for (const auto& [account, statistics] : AccountStatistics_) {
        result += statistics;
    }
    return result;
}

std::string TCypressShard::GetLowercaseObjectName() const
{
    return Format("Cypress shard %v", GetId());
}

std::string TCypressShard::GetCapitalizedObjectName() const
{
    return Format("Cypress shard %v", GetId());
}

void TCypressShard::RegisterMetadata(auto&& registrar)
{
    registrar.template BaseType<TObject>();

    PHOENIX_REGISTER_FIELD(1, AccountStatistics_);
    PHOENIX_REGISTER_FIELD(2, Root_);
    PHOENIX_REGISTER_FIELD(3, Name_);
}

PHOENIX_DEFINE_TYPE(TCypressShard);

NYPath::TYPath TCypressShard::MaybeRewritePath(const NYPath::TYPath& path, bool useBetterCheckWhenRewritingPath)
{
    auto rootNode = GetRoot();
    if (rootNode->GetType() != NCypressClient::EObjectType::PortalExit) {
        // Fast path.
        return path;
    }

    auto exitNode = rootNode->As<TPortalExitNode>();

    if (!useBetterCheckWhenRewritingPath) {
        auto pathPrefix = exitNode->GetPath();
        if (path.length() != pathPrefix.length() && path.StartsWith(pathPrefix)) {
            return NObjectClient::FromObjectId(exitNode->GetId()) + path.substr(pathPrefix.length());
        }

        return path;
    }

    TTokenizer prefixTokenizer(exitNode->GetPath());
    TTokenizer pathTokenizer(path);

    auto prefixTokenType = prefixTokenizer.Advance();
    auto pathTokenType = pathTokenizer.Advance();

    while (prefixTokenType != ETokenType::EndOfStream &&
        pathTokenType != ETokenType::EndOfStream)
    {
        if (prefixTokenizer.GetToken() != pathTokenizer.GetToken()) {
            return path;
        }

        prefixTokenizer.Advance();
        prefixTokenizer.Skip(ETokenType::Ampersand);
        prefixTokenType = prefixTokenizer.GetType();

        pathTokenizer.Advance();
        pathTokenizer.Skip(ETokenType::Ampersand);
        pathTokenType = pathTokenizer.GetType();
    }

    if (pathTokenType == ETokenType::EndOfStream && prefixTokenType == ETokenType::EndOfStream) {
        return FromObjectId(exitNode->GetId());
    }

    if (pathTokenType == ETokenType::EndOfStream) {
        return path;
    }

    YT_TLOG_ALERT_UNLESS(pathTokenType == ETokenType::Slash, "Unexpected token sequence encountered when attempting to rewrite path")
        .With("ExpectedTokenType", ETokenType::Slash)
        .With("ActualTokenType", pathTokenType)
        .With("Token", pathTokenizer.GetToken());

    return FromObjectId(exitNode->GetId()) + pathTokenizer.GetInput();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressServer

