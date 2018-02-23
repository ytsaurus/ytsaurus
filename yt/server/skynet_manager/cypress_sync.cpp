#include "cypress_sync.h"

#include <yt/ytlib/api/client.h>
#include <yt/ytlib/api/transaction.h>

#include <yt/ytlib/ypath/rich.h>

#include <yt/ytlib/table_client/skynet_column_evaluator.h>

#include <yt/core/ytree/helpers.h>
#include <yt/core/ytree/fluent.h>

#include <yt/core/utilex/random.h>

#include "private.h"
#include "config.h"

namespace NYT {
namespace NSkynetManager {

using namespace NApi;
using namespace NYPath;
using namespace NYTree;
using namespace NYson;
using namespace NConcurrency;
using namespace NObjectClient;
using namespace NTransactionClient;
using namespace NCypressClient;

static const auto& Logger = SkynetManagerLogger;

////////////////////////////////////////////////////////////////////////////////

TCypressSync::TCypressSync(
    const TClusterConnectionConfigPtr& config,
    const IClientPtr& client,
    const TString& name,
    const TShareCachePtr& shareCache)
    : Config_(config)
    , Client_(client)
    , Name_(name)
    , ShareCache_(shareCache)
{ }

void TCypressSync::CreateCypressNodesIfNeeded()
{
    TCreateNodeOptions options;
    options.IgnoreExisting = true;
    options.Recursive = true;

    WaitFor(Client_->CreateNode(
        Config_->Root + "/managers/" + Name_,
        EObjectType::MapNode,
        options))
        .ThrowOnError();

    WaitFor(Client_->CreateNode(
        Config_->Root + "/torrents",
        EObjectType::MapNode,
        options))
        .ThrowOnError();
}

std::vector<std::pair<TShardName, i64>> TCypressSync::FindChangedShards()
{
    TListNodeOptions options;
    options.Attributes = {"revision"};

    auto shardsYson = WaitFor(Client_->ListNode(Config_->Root + "/torrents", options))
        .ValueOrThrow();
    auto shardsNode = ConvertToNode(shardsYson);

    std::vector<std::pair<TShardName, i64>> changedShards;
    for (const auto& shard : shardsNode->AsList()->GetChildren()) {
        auto shardName = shard->AsString()->GetValue();
        auto shardRevision = shard->Attributes().Get<i64>("revision");

        if (LastSeenRevision_[shardName] != shardRevision) {
            changedShards.emplace_back(shardName, shardRevision);
        }
    }

    LOG_INFO("Found changed shards (ChangedShards: %v)",
        ConvertToYsonString(changedShards, EYsonFormat::Text));
    return changedShards;
}

std::vector<TCypressShareState> TCypressSync::ListShard(const TShardName& shardName, i64 revision)
{
    TListNodeOptions options;
    options.Attributes = {"skynet_table_path", "skynet_table_revision"};

    auto torrentsYson = WaitFor(Client_->ListNode(Config_->Root + "/torrents/" + shardName, options))
        .ValueOrThrow();

    std::vector<TCypressShareState> torrents;
    for (const auto& torrent : ConvertToNode(torrentsYson)->AsList()->GetChildren()) {
        auto rbTorrentId = torrent->AsString()->GetValue();
        auto tablePath = torrent->Attributes().Get<TYPath>("skynet_table_path");
        auto tableRevision = torrent->Attributes().Get<i64>("skynet_table_revision");

        torrents.push_back(TCypressShareState{std::move(rbTorrentId), std::move(tablePath), tableRevision});
    }

    return torrents;
}

void TCypressSync::CommitLastSeenRevision(const TShardName& shardName, i64 revision)
{
    LastSeenRevision_[shardName] = revision;
}

TErrorOr<i64> TCypressSync::CheckTableAttributes(const NYPath::TRichYPath& path)
{
    TGetNodeOptions options;
    options.Attributes = {
        "type",
        "account",
        "row_count",
        "schema",
        "enable_skynet_sharing",
        "revision",
    };

    try {
        auto asyncGet = Client_->GetNode(path.Normalize().GetPath(), options);
        auto node = ConvertToNode(WaitFor(asyncGet).ValueOrThrow());
        const auto& attributes = node->Attributes();
 
        if (attributes.Get<NObjectClient::EObjectType>("type") != EObjectType::Table) {
            return TError("Cypress node is not a table");
        }

        if (attributes.Get<i64>("row_count") == 0) {
            return TError("Table is empty");
        }

        if (!attributes.Get<bool>("enable_skynet_sharing", false)) {
            return TError("\"enable_skynet_sharing\" attribute is not set");
        }

        auto schema = attributes.Get<NTableClient::TTableSchema>("schema");
        try {
            NTableClient::ValidateSkynetSchema(schema);
        } catch (const TErrorException& ex) {
            return ex.Error();
        }

        // TODO(prime): keep per-account usage statistics
        auto account = attributes.Get<TString>("account");

        return attributes.Get<i64>("revision");
    } catch (const TErrorException& ex) {
        if (ex.Error().GetCode() == NYTree::EErrorCode::ResolveError) {
            return ex.Error();
        }
        throw;
    }
}

void TCypressSync::AddShare(
    const TString& rbTorrentId,
    const TRichYPath& tablePath,
    i64 tableRevision)
{
    std::shared_ptr<IAttributeDictionary> attributes = CreateEphemeralAttributes();
    attributes->SetYson("skynet_table_path", ConvertToYsonString(tablePath));
    attributes->SetYson("skynet_table_revision", ConvertToYsonString(tableRevision));

    TCreateNodeOptions options;
    options.Force = true; // intentinally override node if exists
    options.Recursive = true;
    options.Attributes = attributes;

    WaitFor(Client_->CreateNode(GetPath(rbTorrentId), EObjectType::MapNode, options))
        .ThrowOnError();
}

void TCypressSync::RemoveShare(const TString& rbTorrentId)
{
    TRemoveNodeOptions options;
    options.Force = true;

    WaitFor(Client_->RemoveNode(GetPath(rbTorrentId)))
        .ThrowOnError();
}

TYPath TCypressSync::GetPath(const TString& rbTorrentId) const
{
    auto shard = GetShardName(rbTorrentId);
    return Format("%v/torrents/%v/%v", Config_->Root, shard, rbTorrentId);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NSkynetManager
} // namespace NYT
