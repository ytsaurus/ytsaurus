#pragma once

#include "public.h"

#include "rb_torrent.h"

#include "config.h"

#include <yt/ytlib/ypath/rich.h>

#include <yt/core/actions/future.h>

#include <yt/core/yson/public.h>

#include <yt/core/ytree/public.h>

namespace NYT {
namespace NSkynetManager {

////////////////////////////////////////////////////////////////////////////////

typedef TString TShardName;

TStringBuf GetShardName(TStringBuf rbTorrentId);

TString FormatShareKey(const TShareKey& shareKey);

struct TFileOffset
{
    TString FilePath;
    ui64 StartRow;
    ui64 RowCount = 0;
    ui64 FileSize = 0;
};

////////////////////////////////////////////////////////////////////////////////

struct IShareHost
    : virtual public TRefCounted
{
public:
    virtual IInvokerPtr GetInvoker() = 0;

    virtual std::pair<TSkynetShareMeta, std::vector<TFileOffset>> ReadMeta(
        const TString& clusterName,
        const NYPath::TRichYPath& path) = 0;

    virtual void AddShareToCypress(
        const TString& clusterName,
        const TString& rbTorrentId,
        const NYPath::TRichYPath& tablePath,
        i64 tableRevision) = 0;
    virtual void RemoveShareFromCypress(
        const TString& clusterName,
        const TString& rbTorrentId) = 0;

    virtual void AddResourceToSkynet(
        const TString& rbTorrentId,
        const TString& rbTorrent) = 0;
    virtual void RemoveResourceFromSkynet(
        const TString& rbTorrentId) = 0;
};

DEFINE_REFCOUNTED_TYPE(IShareHost)

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EShareState,
    (Creating)
    (Active)
    (Removing)
);

class TShareInfo
    : public TIntrinsicRefCounted
{
public:
    TShareInfo(
        TString cluster,
        NYPath::TYPath tablePath,
        i64 tableRevision,
        TShareCache* cache,
        IShareHostPtr shareHost);

    TNullable<TString> GetRbTorrentId();

    const TShareKey& GetKey() const;
    const TString& GetCluster() const;
    const NYPath::TRichYPath& GetTablePath() const;
    i64 GetTableRevision() const;

    std::shared_ptr<TSkynetShareMeta> GetHashes();
    std::shared_ptr<std::vector<TFileOffset>> GetOffsets();

    void DoCreate(bool addToCypress);
    void DoRemove();

    bool IsActive();
    bool MarkRemoved();

private:
    const TString Cluster_;
    const NYPath::TRichYPath TablePath_;
    const i64 TableRevision_;
    const TShareKey Key_;

    TShareCache* Cache_ = nullptr;
    IShareHostPtr Host_;

    EShareState State_;

    TNullable<TString> RbTorrentId_;
    
    std::shared_ptr<TSkynetShareMeta> Hashes_;
    std::shared_ptr<std::vector<TFileOffset>> Offsets_;
};

DEFINE_REFCOUNTED_TYPE(TShareInfo)

////////////////////////////////////////////////////////////////////////////////

struct TDiscoveryInfo
{
    TString Cluster;
    NYPath::TRichYPath TablePath;
    i64 TableRevision;
    std::shared_ptr<std::vector<TFileOffset>> Offsets;
};

////////////////////////////////////////////////////////////////////////////////

class TTombstoneCache
{
public:
    TTombstoneCache(const TTombstoneCacheConfigPtr& config)
        : Ttl_(config->Ttl)
        , MaxSize_(config->MaxSize)
    { }

    void Put(const TShareKey& key, const TError& error);
    TError GetAndEvict(const TShareKey& key);

private:
    const TDuration Ttl_;
    const int MaxSize_;

    struct TEntry
    {
        TError Error;
        TInstant ExpirationDeadline;

        std::list<std::map<TShareKey, TEntry>::iterator>::iterator QueuePosition;
    };
    
    std::map<TShareKey, TEntry> Tombstones_;
    std::list<std::map<TShareKey, TEntry>::iterator> EvictionQueue_;
};

////////////////////////////////////////////////////////////////////////////////

class TShareCache
    : public TRefCounted
{
public:
    TShareCache(IShareHostPtr shareHost, const TTombstoneCacheConfigPtr& config);

    TError CheckTombstone(const TShareKey& key);

    TNullable<TString> TryShare(const TShareKey& key, bool addToCypress);

    void Unshare(const TShareKey& key);

    TNullable<TDiscoveryInfo> TryDiscover(TStringBuf rbTorrentId);

    std::vector<TShardName> ListShards();
    std::vector<TShareKey> ListActiveShares(const TShardName& shardName, const TString& cluster);

private:
    const IShareHostPtr ShareHost_;

    TSpinLock Lock_;
    THashMap<TShardName, THashMap<TString, TShareInfoPtr>> Shards_;
    THashMap<TShareKey, TShareInfoPtr> Shares_;

    TTombstoneCache Tombstones_;

    void PutByRbTorrentIdNoLock(const TString& rbTorrentId, const TShareInfoPtr& info);
    void UnlinkByRbTorrentIdNoLock(const TString& rbTorrentId, const TShareInfoPtr& info);

    friend class TShareInfo;
};

DEFINE_REFCOUNTED_TYPE(TShareCache)

////////////////////////////////////////////////////////////////////////////////

} // namespace NSkynetManager
} // namespace NYT
