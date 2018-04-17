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
    NSecurityServer::TAccount* account,
    int mediumIndex,
    TReplicationPolicy replicationPolicy,
    bool committed)
    : Account(account)
    , MediumIndex(mediumIndex)
    , ReplicationPolicy(replicationPolicy)
    , Committed(committed)
{ }

inline bool TRequisitionEntry::operator<(const TRequisitionEntry& rhs) const
{
    // TChunkRequisition merges entries by the "account, medium, committed" triplet.
    // TSecurityManager relies on that order. Don't change it lightly.
    Y_ASSERT((Account == rhs.Account) == (Account->GetId() == rhs.Account->GetId()));
    if (Account != rhs.Account) {
        return Account->GetId() < rhs.Account->GetId();
    }


    if (MediumIndex != rhs.MediumIndex) {
        return MediumIndex < rhs.MediumIndex;
    }

    if (Committed != rhs.Committed) {
        return Committed && !rhs.Committed; // committed entries come first
    }

    if (ReplicationPolicy.GetReplicationFactor() != rhs.ReplicationPolicy.GetReplicationFactor()) {
        return ReplicationPolicy.GetReplicationFactor() < rhs.ReplicationPolicy.GetReplicationFactor();
    }

    return !ReplicationPolicy.GetDataPartsOnly() && rhs.ReplicationPolicy.GetDataPartsOnly();
}

inline bool TRequisitionEntry::operator==(const TRequisitionEntry& rhs) const
{
    Y_ASSERT((Account == rhs.Account) == (Account->GetId() == rhs.Account->GetId()));

    return
        Account == rhs.Account &&
        MediumIndex == rhs.MediumIndex &&
        ReplicationPolicy == rhs.ReplicationPolicy &&
        Committed == rhs.Committed;
}

inline size_t TRequisitionEntry::GetHash() const
{
    auto result = NObjectClient::TDirectObjectIdHash()(Account->GetId());
    HashCombine(result, MediumIndex);
    HashCombine(result, ReplicationPolicy.GetReplicationFactor());
    HashCombine(result, ReplicationPolicy.GetDataPartsOnly());
    HashCombine(result, Committed);
    return result;
}

////////////////////////////////////////////////////////////////////////////////

inline TChunkRequisition::TChunkRequisition(
    NSecurityServer::TAccount* account,
    int mediumIndex,
    TReplicationPolicy replicationPolicy,
    bool committed)
    : Entries_(1, TRequisitionEntry(account, mediumIndex, replicationPolicy, committed))
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

inline size_t TChunkRequisition::GetEntryCount() const
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

inline size_t TChunkRequisition::GetHash() const
{
    size_t result = hash<bool>()(Vital_);

    Y_ASSERT(std::is_sorted(Entries_.begin(), Entries_.end()));

    for (const auto& entry : Entries_) {
        HashCombine(result, entry);
    }

    return result;
}

////////////////////////////////////////////////////////////////////////////////

inline const TChunkRequisition& TChunkRequisitionRegistry::GetRequisition(TChunkRequisitionIndex index) const
{
    auto it = IndexToItem_.find(index);
    YCHECK(it != IndexToItem_.end());
    return it->second.Requisition;
}

inline const TChunkReplication& TChunkRequisitionRegistry::GetReplication(TChunkRequisitionIndex index) const
{
    auto it = IndexToItem_.find(index);
    YCHECK(it != IndexToItem_.end());
    return it->second.Replication;
}

inline TChunkRequisitionIndex TChunkRequisitionRegistry::GenerateIndex()
{
    auto result = NextIndex_++;
    while (IndexToItem_.has(NextIndex_)) {
        ++NextIndex_;
    }
    return result;
}

////////////////////////////////////////////////////////////////////////////////

inline TChunkRequisitionIndex TEphemeralRequisitionRegistry::Insert(const TChunkRequisition& requisition)
{
    auto index = GenerateIndex();

    YCHECK(IndexToRequisition_.emplace(index, requisition).second);
    YCHECK(RequisitionToIndex_.emplace(requisition, index).second);

    return index;
}

inline TChunkRequisitionIndex TEphemeralRequisitionRegistry::GenerateIndex()
{
    auto result = NextIndex_++;
    while (IndexToRequisition_.has(NextIndex_)) {
        ++NextIndex_;
    }
    return result;
}

////////////////////////////////////////////////////////////////////////////////

template <typename T>
void FillChunkRequisitionDict(NProto::TReqUpdateChunkRequisition* request, const T& requisitionRegistry)
{
    YCHECK(request->chunk_requisition_dict_size() == 0);

    if (request->updates_size() == 0) {
        return;
    }

    std::vector<TChunkRequisitionIndex> indexes;
    for (const auto& update : request->updates()) {
        indexes.push_back(update.chunk_requisition_index());
    }
    std::sort(indexes.begin(), indexes.end());
    indexes.erase(std::unique(indexes.begin(), indexes.end()), indexes.end());

    for (auto index : indexes) {
        const auto& requisition = requisitionRegistry.GetRequisition(index);
        auto* protoDictItem = request->add_chunk_requisition_dict();
        protoDictItem->set_index(index);
        ToProto(protoDictItem->mutable_requisition(), requisition);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkServer
} // namespace NYT
