#include "shard.h"

#include <yt/yt/server/master/cell_master/serialize.h>

#include <yt/yt/core/misc/serialize.h>

#include <yt/yt/server/master/security_server/account.h>
#include <yt/yt/server/master/cypress_server/portal_exit_node.h>

#include <yt/yt/core/ypath/tokenizer.h>

#include <yt/yt/core/ytree/fluent.h>

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

void TCypressShardAccountStatistics::Persist(const NCellMaster::TPersistenceContext& context)
{
    using NYT::Persist;
    Persist(context, NodeCount);
}

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

void TCypressShard::Save(NCellMaster::TSaveContext& context) const
{
    TObject::Save(context);

    using NYT::Save;
    Save(context, AccountStatistics_);
    Save(context, Root_);
    Save(context, Name_);
}

void TCypressShard::Load(NCellMaster::TLoadContext& context)
{
    TObject::Load(context);

    using NYT::Load;
    Load(context, AccountStatistics_);
    Load(context, Root_);
    Load(context, Name_);
}

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

    YT_LOG_ALERT_UNLESS(pathTokenType == ETokenType::Slash,
        "Unexpected token sequence encountered when attempting to rewrite path "
        "(ExpectedTokenType: %v, ActualTokenType: %v, Token: %v)",
        ETokenType::Slash,
        pathTokenType,
        pathTokenizer.GetToken());

    return FromObjectId(exitNode->GetId()) + pathTokenizer.GetInput();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressServer

