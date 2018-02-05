#pragma once

#include "public.h"

#include "share_cache.h"

#include <yt/ytlib/api/public.h>

#include <yt/ytlib/ypath/rich.h>

#include <yt/core/ypath/public.h>

#include <yt/core/actions/future.h>

namespace NYT {
namespace NSkynetManager {

////////////////////////////////////////////////////////////////////////////////

struct TCypressShareState
{
    TString RbTorrentId;
    NYPath::TYPath TablePath;
    i64 TableRevision;
};

class TCypressSync
    : public TRefCounted
{
public:
    TCypressSync(
        const TClusterConnectionConfigPtr& config,
        const NApi::IClientPtr& client,
        const TString& name,
        const TShareCachePtr& shareCache);

    void CreateCypressNodesIfNeeded();

    std::vector<std::pair<TShardName, i64>> FindChangedShards();
    std::vector<TCypressShareState> ListShard(const TShardName& shardName, i64 revision);
    void CommitLastSeenRevision(const TShardName& shardName, i64 revision);

    TErrorOr<i64> CheckTableAttributes(const NYPath::TRichYPath& path);

    void AddShare(const TString& rbTorrentId, const NYPath::TRichYPath& tablePath, i64 tableRevision);
    void RemoveShare(const TString& rbTorrentId);

private:
    TClusterConnectionConfigPtr Config_;
    NApi::IClientPtr Client_;
    TString Name_;

    std::map<TShardName, i64> LastSeenRevision_;

    TShareCachePtr ShareCache_;

    NYPath::TYPath GetPath(const TString& rbTorrentId) const;
};

DEFINE_REFCOUNTED_TYPE(TCypressSync)

////////////////////////////////////////////////////////////////////////////////

} // namespace NSkynetManager
} // namespace NYT
