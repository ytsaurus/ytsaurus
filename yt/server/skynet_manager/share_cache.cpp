#include "share_cache.h"

#include "skynet_api.h"
#include "cypress_sync.h"
#include "private.h"

#include <yt/core/ytree/virtual.h>
#include <yt/core/ytree/fluent.h>

namespace NYT {
namespace NSkynetManager {

using namespace NYTree;
using namespace NYson;
using namespace NConcurrency;

static const auto& Logger = SkynetManagerLogger;

DECLARE_REFCOUNTED_TYPE(IShareHost)

////////////////////////////////////////////////////////////////////////////////

TStringBuf GetShardName(const TStringBuf& rbTorrentId)
{
    constexpr int offset = sizeof("rbtorrent");
    return rbTorrentId.substr(offset, 2);
}

TString FormatShareKey(const TShareKey& shareKey)
{
    return Format("{%Qv, %Qv, %v}", std::get<0>(shareKey), std::get<1>(shareKey), std::get<2>(shareKey));
}

////////////////////////////////////////////////////////////////////////////////

TShareInfo::TShareInfo(
    TString cluster,
    TYPath tablePath,
    i64 tableRevision,
    TShareCache* cache,
    IShareHostPtr shareHost)
    : Cluster_(std::move(cluster))
    , TablePath_(tablePath)
    , TableRevision_(tableRevision)
    , Key_{Cluster_, std::move(tablePath), tableRevision}
    , Cache_(cache)
    , Host_(std::move(shareHost))
    , State_(EShareState::Creating)
{ }

TNullable<TString> TShareInfo::GetRbTorrentId()
{
    return State_ == EShareState::Active ? RbTorrentId_ : Null;
}

const TString& TShareInfo::GetCluster() const
{
    return Cluster_;
}

const NYPath::TRichYPath& TShareInfo::GetTablePath() const
{
    return TablePath_;
}

i64 TShareInfo::GetTableRevision() const
{
    return TableRevision_;
}

const TShareKey& TShareInfo::GetKey() const
{
    return Key_;
}

std::shared_ptr<TSkynetShareMeta> TShareInfo::GetHashes()
{
    return Hashes_;
}

std::shared_ptr<std::vector<TFileOffset>> TShareInfo::GetOffsets()
{
    return Offsets_;
}

bool TShareInfo::IsActive()
{
    return State_ == EShareState::Active;
}

bool TShareInfo::MarkRemoved()
{
    if (State_ == EShareState::Active) {
        State_ = EShareState::Removing;
        return true;
    }

    State_ = EShareState::Removing;
    return false;
}

void TShareInfo::DoCreate(bool addToCypress)
{
    LOG_INFO("Adding share to cache (Key: %v)", FormatShareKey(Key_));

    bool tailRemove = false;
    try {
        auto meta = Host_->ReadMeta(Cluster_, TablePath_);
        {
            auto guard = Guard(Cache_->Lock_);
            Hashes_ = std::make_shared<TSkynetShareMeta>(std::move(meta.first));
            Offsets_ = std::make_shared<std::vector<TFileOffset>>(std::move(meta.second));
        }

        auto resource = GenerateResource(*Hashes_);
        {
            auto guard = Guard(Cache_->Lock_);
            RbTorrentId_ = resource.RbTorrentId;
            Cache_->PutByRbTorrentIdNoLock(*RbTorrentId_, this);
        }

        if (addToCypress) {
            Host_->AddShareToCypress(Cluster_, *RbTorrentId_, TablePath_, TableRevision_);
        }
        Host_->AddResourceToSkynet(*RbTorrentId_, resource.BencodedTorrentMeta);

        auto guard = Guard(Cache_->Lock_);
        if (State_ == EShareState::Creating) {
            State_ = EShareState::Active;
        } else {
            tailRemove = true;
        }
    } catch (const std::exception& ex) {    
        auto guard = Guard(Cache_->Lock_);
        YCHECK(State_ != EShareState::Active);

        Cache_->Tombstones_.Put(Key_, TError(ex));

        Cache_->Shares_.erase(Key_);
        if (RbTorrentId_) {
            Cache_->UnlinkByRbTorrentIdNoLock(*RbTorrentId_, this);
        }
        return;
    }

    if (tailRemove) {
        DoRemove();
    } else {
        LOG_INFO("Finished adding share to cache (Key: %v, RbTorrentId: %v)", FormatShareKey(Key_), RbTorrentId_);
    }
}

void TShareInfo::DoRemove()
{
    LOG_INFO("Removing share from cache (Key: %v, RbTorrentId: %v)", FormatShareKey(Key_), RbTorrentId_);
    try {
        if (RbTorrentId_) {
            Host_->RemoveResourceFromSkynet(*RbTorrentId_);
            Host_->RemoveShareFromCypress(Cluster_, *RbTorrentId_);
        }
    } catch (const std::exception& ex) {
        LOG_ERROR(ex, "Error while removing share (Key: %v, RbTorrentId: %Qv)",
            FormatShareKey(Key_), RbTorrentId_);
    }

    {
        auto guard = Guard(Cache_->Lock_);
        Cache_->Shares_.erase(Key_);
        if (RbTorrentId_) {
            Cache_->UnlinkByRbTorrentIdNoLock(*RbTorrentId_, this);
        }
    }
    LOG_INFO("Finished removing share from cache (Key: %v, RbTorrentId: %v)", FormatShareKey(Key_), RbTorrentId_);
}

////////////////////////////////////////////////////////////////////////////////

void TTombstoneCache::Put(const TShareKey& key, const TError& error)
{
    auto it = Tombstones_.find(key);
    if (it != Tombstones_.end()) {
        EvictionQueue_.erase(it->second.QueuePosition);

        it->second.Error = error;
        it->second.ExpirationDeadline = TInstant::Now() + Ttl_;
    } else {
        it = Tombstones_.emplace(key, TEntry{error, TInstant::Now() + Ttl_, {}}).first;
    }

    EvictionQueue_.push_back(it);
    it->second.QueuePosition = --EvictionQueue_.end();
    
    if (EvictionQueue_.empty()) {
        return;
    }

    if (EvictionQueue_.front()->second.ExpirationDeadline < TInstant::Now() || Tombstones_.size() > MaxSize_) {
        Tombstones_.erase(EvictionQueue_.front());
        EvictionQueue_.pop_front();
    }
}

TError TTombstoneCache::GetAndEvict(const TShareKey& key)
{
    auto it = Tombstones_.find(key);
    if (it == Tombstones_.end()) {
        return {};
    } else {
        auto entry = std::move(it->second);

        Tombstones_.erase(it);
        EvictionQueue_.erase(entry.QueuePosition);

        if (entry.ExpirationDeadline < TInstant::Now()) {
            return {};
        } else {
            return entry.Error;
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

TShareCache::TShareCache(IShareHostPtr shareHost, const TTombstoneCacheConfigPtr& config)
    : ShareHost_(std::move(shareHost))
    , Tombstones_(config)
{ }

TError TShareCache::CheckTombstone(const TShareKey& key)
{
    auto guard = Guard(Lock_);
    return Tombstones_.GetAndEvict(key);
}

TNullable<TString> TShareCache::TryShare(const TShareKey& key, bool addToCypress)
{
    auto guard = Guard(Lock_);
    auto it = Shares_.find(key);
    if (it == Shares_.end()) {
        auto share = New<TShareInfo>(std::get<0>(key), std::get<1>(key), std::get<2>(key), this, ShareHost_);
        Shares_.emplace(key, share);
        guard.Release();

        BIND(&TShareInfo::DoCreate, share, addToCypress)
            .AsyncVia(ShareHost_->GetInvoker())
            .Run();
        return {};
    }

    return it->second->GetRbTorrentId();
}

void TShareCache::Unshare(const TShareKey& key)
{
    auto guard = Guard(Lock_);

    auto it = Shares_.find(key);
    if (it == Shares_.end()) {
        return;
    }

    auto share = it->second;
    bool spawn = share->MarkRemoved();
    guard.Release();

    if (spawn) {
        BIND(&TShareInfo::DoRemove, share)
            .AsyncVia(ShareHost_->GetInvoker())
            .Run();
    }
}

TNullable<TDiscoverInfo> TShareCache::TryDiscover(
    const TStringBuf& rbTorrentId)
{
    auto guard = Guard(Lock_);
    auto shardName = GetShardName(rbTorrentId);
    auto shardIt = Shards_.find(shardName);
    if (shardIt == Shards_.end()) {
        return Null;
    }
    auto it = shardIt->second.find(rbTorrentId);
    if (it == shardIt->second.end()) {
        return Null;
    }

    if (it->second->GetOffsets()) {
        TDiscoverInfo info;
        info.Cluster = it->second->GetCluster();
        info.TablePath = it->second->GetTablePath();
        info.Offsets = it->second->GetOffsets();
        info.TableRevision = it->second->GetTableRevision();
        return info;
    } else {
        return Null;
    }
}

std::vector<TShardName> TShareCache::ListShards()
{
    auto guard = Guard(Lock_);
    std::vector<TShardName> shards;
    for (const auto& shard : Shards_) {
        shards.push_back(shard.first);
    }
    return shards;
}

std::vector<TShareKey> TShareCache::ListActiveShares(const TShardName& shardName, const TString& cluster)
{
    auto guard = Guard(Lock_);
    std::vector<TShareKey> shares;
    for (const auto& share : Shards_[shardName]) {
        if (share.second->IsActive() && share.second->GetCluster() == cluster) {
            shares.emplace_back(share.second->GetKey());
        }
    }
    return shares;
}

void TShareCache::PutByRbTorrentIdNoLock(const TString& rbTorrentId, const TShareInfoPtr& info)
{
    auto& slot = Shards_[GetShardName(rbTorrentId)][rbTorrentId];

    if (slot) {
        LOG_WARNING("Evicting share with duplicate content (RbTorrentId: %v, Evicted: %v, New: %v)",
            rbTorrentId, FormatShareKey(slot->GetKey()), FormatShareKey(info->GetKey()));
        Shares_.erase(slot->GetKey());
    }

    slot = info;
}

void TShareCache::UnlinkByRbTorrentIdNoLock(const TString& rbTorrentId, const TShareInfoPtr& info)
{
    auto shardName = GetShardName(rbTorrentId);
    Shards_[shardName].erase(rbTorrentId);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NSkynetManager
} // namespace NYT
