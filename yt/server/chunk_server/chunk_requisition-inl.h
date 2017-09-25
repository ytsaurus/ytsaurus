#ifndef CHUNK_REQUISITION_INL_H_
#error "Direct inclusion of this file is not allowed, include chunk_requisition.h"
#endif

namespace NYT {
namespace NChunkServer {

////////////////////////////////////////////////////////////////////////////////

inline TReplicationPolicy::TReplicationPolicy()
    : ReplicationFactor_(0)
    , DataPartsOnly_(false)
{ }

inline TReplicationPolicy::TReplicationPolicy(int replicationFactor, bool dataPartsOnly)
    : ReplicationFactor_(replicationFactor)
    , DataPartsOnly_(dataPartsOnly)
{ }

inline void TReplicationPolicy::Clear()
{
    *this = TReplicationPolicy();
}

inline int TReplicationPolicy::GetReplicationFactor() const
{
    return ReplicationFactor_;
}

inline void TReplicationPolicy::SetReplicationFactor(int replicationFactor)
{
    ReplicationFactor_ = replicationFactor;
}

inline bool TReplicationPolicy::GetDataPartsOnly() const
{
    return DataPartsOnly_;
}

inline void TReplicationPolicy::SetDataPartsOnly(bool dataPartsOnly)
{
    DataPartsOnly_ = dataPartsOnly;
}

inline TReplicationPolicy::operator bool() const
{
    return GetReplicationFactor() != 0;
}

inline bool operator==(TReplicationPolicy lhs, TReplicationPolicy rhs)
{
    return lhs.GetReplicationFactor() == rhs.GetReplicationFactor() &&
        lhs.GetDataPartsOnly() == rhs.GetDataPartsOnly();
}

inline bool operator!=(TReplicationPolicy lhs, TReplicationPolicy rhs)
{
    return !(lhs == rhs);
}

////////////////////////////////////////////////////////////////////////////////

inline TChunkReplication::const_iterator TChunkReplication::begin() const
{
    return MediumReplicationPolicies.begin();
}

inline TChunkReplication::const_iterator TChunkReplication::end() const
{
    return MediumReplicationPolicies.end();
}

inline TChunkReplication::const_iterator TChunkReplication::cbegin() const
{
    return begin();
}

inline TChunkReplication::const_iterator TChunkReplication::cend() const
{
    return end();
}

inline TChunkReplication::iterator TChunkReplication::begin()
{
    return MediumReplicationPolicies.begin();
}

inline TChunkReplication::iterator TChunkReplication::end()
{
    return MediumReplicationPolicies.end();
}

inline const TReplicationPolicy& TChunkReplication::operator[](int mediumIndex) const
{
    return MediumReplicationPolicies[mediumIndex];
}

inline TReplicationPolicy& TChunkReplication::operator[](int mediumIndex)
{
    return MediumReplicationPolicies[mediumIndex];
}

inline bool TChunkReplication::GetVital() const
{
    return Vital_;
}

inline void TChunkReplication::SetVital(bool vital)
{
    Vital_ = vital;
}

inline bool operator==(const TChunkReplication& lhs, const TChunkReplication& rhs)
{
    if (&lhs == &rhs)
        return true;

    return (lhs.GetVital() == rhs.GetVital()) &&
        std::equal(lhs.begin(), lhs.end(), rhs.begin());
}

inline bool operator!=(const TChunkReplication& lhs, const TChunkReplication& rhs)
{
    return !(lhs == rhs);
}

////////////////////////////////////////////////////////////////////////////////

inline TRequisitionEntry::TRequisitionEntry(
    const NSecurityClient::TAccountId& accountId,
    int mediumIndex,
    TReplicationPolicy replicationPolicy,
    bool committed)
    : AccountId(accountId)
    , MediumIndex(mediumIndex)
    , ReplicationPolicy(replicationPolicy)
    , Committed(committed)
{ }

inline bool TRequisitionEntry::operator<(const TRequisitionEntry& rhs) const
{
    // TChunkRequisition merges entries by the "account, medium, committed" triplet.
    // TSecurityManager relies on that order. Don't change it lightly.
    if (AccountId != rhs.AccountId) {
        return AccountId < rhs.AccountId;
    }

    if (MediumIndex != rhs.MediumIndex) {
        return MediumIndex < rhs.MediumIndex;
    }

    if (Committed != rhs.Committed) {
        return Committed && !rhs.Committed; // Committed entries come first.
    }

    if (ReplicationPolicy.GetReplicationFactor() != rhs.ReplicationPolicy.GetReplicationFactor()) {
        return ReplicationPolicy.GetReplicationFactor() < rhs.ReplicationPolicy.GetReplicationFactor();
    }

    return !ReplicationPolicy.GetDataPartsOnly() && rhs.ReplicationPolicy.GetDataPartsOnly();
}

inline bool TRequisitionEntry::operator==(const TRequisitionEntry& rhs) const
{
    return
        AccountId == rhs.AccountId &&
        MediumIndex == rhs.MediumIndex &&
        ReplicationPolicy == rhs.ReplicationPolicy &&
        Committed == rhs.Committed;
}

inline size_t TRequisitionEntry::Hash() const
{
    return
        13 * NObjectClient::TDirectObjectIdHash()(AccountId) +
        17 * hash<int>()(MediumIndex) +
        19 * hash<int>()(ReplicationPolicy.GetReplicationFactor()) +
        23 * hash<bool>()(ReplicationPolicy.GetDataPartsOnly()) +
        29 * hash<bool>()(Committed);
}

////////////////////////////////////////////////////////////////////////////////

inline TChunkRequisition::TChunkRequisition(
    const NSecurityClient::TAccountId& accountId,
    int mediumIndex,
    TReplicationPolicy replicationPolicy,
    bool committed)
    : Entries_(1, TRequisitionEntry(accountId, mediumIndex, replicationPolicy, committed))
{ }

inline TChunkRequisition::const_iterator TChunkRequisition::begin() const
{
    return Entries_.begin();
}

inline TChunkRequisition::const_iterator TChunkRequisition::end() const
{
    return Entries_.end();
}

inline TChunkRequisition::const_iterator TChunkRequisition::cbegin() const
{
    return begin();
}

inline TChunkRequisition::const_iterator TChunkRequisition::cend() const
{
    return end();
}

inline size_t TChunkRequisition::EntryCount() const
{
    return Entries_.size();
}

inline bool TChunkRequisition::GetVital() const
{
    return Vital_;
}

inline void TChunkRequisition::SetVital(bool vital)
{
    Vital_ = vital;
}

inline bool TChunkRequisition::operator==(const TChunkRequisition& rhs) const
{
    if (this == &rhs) {
        return true;
    }

    return Entries_ == rhs.Entries_ && Vital_ == rhs.Vital_;
}

inline size_t TChunkRequisition::Hash() const
{
    size_t result = hash<bool>()(Vital_);

    Y_ASSERT(std::is_sorted(Entries_.begin(), Entries_.end()));

    for(const auto& entry : Entries_) {
        HashCombine(result, entry);
    }

    return result;
}

////////////////////////////////////////////////////////////////////////////////

inline const TChunkRequisition& TChunkRequisitionRegistry::GetRequisition(ui32 index) const
{
    auto it = Index_.find(index);
    YCHECK(it != Index_.end());
    return it->second.Requisition;
}

inline const TChunkReplication& TChunkRequisitionRegistry::GetReplication(ui32 index) const
{
    auto it = Index_.find(index);
    YCHECK(it != Index_.end());
    return it->second.Replication;
}

inline ui32 TChunkRequisitionRegistry::GetIndex(const TChunkRequisition& requisition)
{
    auto it = ReverseIndex_.find(requisition);
    if (it != ReverseIndex_.end()) {
        auto it2 = Index_.find(it->second);
        Y_ASSERT(it2 != Index_.end());
        return it->second;
    }

    return Insert(requisition);
}

inline void TChunkRequisitionRegistry::Ref(ui32 index)
{
    auto it = Index_.find(index);
    YCHECK(it != Index_.end());
    ++it->second.RefCount;
}

inline void TChunkRequisitionRegistry::Unref(ui32 index)
{
    auto it = Index_.find(index);
    YCHECK(it != Index_.end());
    YCHECK(it->second.RefCount != 0);
    --it->second.RefCount;

    if (it->second.RefCount == 0) {
        ReverseIndex_.erase(it->second.Requisition);
        Index_.erase(it);
    }
}

inline ui32 TChunkRequisitionRegistry::Insert(const TChunkRequisition& requisition)
{
    auto index = NextIndex();

    TIndexedItem item;
    item.Requisition = requisition;
    item.Replication = requisition.ToReplication();
    item.RefCount = 0; // This is ok, Ref()/Unref() will be called soon.
    YCHECK(Index_.emplace(index, item).second);
    YCHECK(ReverseIndex_.emplace(requisition, index).second);

    return index;
}

inline ui32 TChunkRequisitionRegistry::NextIndex()
{
    auto result = NextIndex_++;
    while (Index_.has(NextIndex_)) {
        ++NextIndex_;
    }
    return result;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkServer
} // namespace NYT
