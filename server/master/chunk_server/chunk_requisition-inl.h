#ifndef CHUNK_REQUISITION_INL_H_
#error "Direct inclusion of this file is not allowed, include chunk_requisition.h"
// For the sake of sane code completion.
#include "chunk_requisition.h"
#endif

namespace NYT::NChunkServer {

////////////////////////////////////////////////////////////////////////////////

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

inline bool TChunkReplication::TEntry::operator==(const TEntry& rhs) const
{
    return MediumIndex_ == rhs.MediumIndex_ && Policy_ == rhs.Policy_;
}

inline bool TChunkReplication::TEntryComparator::operator()(const TEntry& lhs, const TEntry& rhs) const
{
    return lhs.MediumIndex_ < rhs.MediumIndex_;
}

////////////////////////////////////////////////////////////////////////////////

inline TChunkReplication::iterator TChunkReplication::begin()
{
    return Entries_.begin();
}

inline TChunkReplication::iterator TChunkReplication::end()
{
    return Entries_.end();
}

inline TChunkReplication::const_iterator TChunkReplication::begin() const
{
    return Entries_.begin();
}

inline TChunkReplication::const_iterator TChunkReplication::end() const
{

    return Entries_.end();
}

inline TChunkReplication::const_iterator TChunkReplication::cbegin() const
{
    return begin();
}

inline TChunkReplication::const_iterator TChunkReplication::cend() const
{
    return end();
}

inline bool TChunkReplication::Contains(int mediumIndex) const
{
    return Find(mediumIndex) != end();
}

inline bool TChunkReplication::Erase(int mediumIndex)
{
    auto it = Find(mediumIndex);
    if (it != Entries_.end()) {
        Entries_.erase(it);
        return true;
    } else {
        return false;
    }
}

inline void TChunkReplication::Set(int mediumIndex, TReplicationPolicy policy, bool eraseEmpty)
{
    if (policy || !eraseEmpty) {
        auto [it, inserted] = Insert(mediumIndex, policy);
        if (!inserted) {
            it->Policy() = policy;
        }
    } else {
        // NB: ignoring policy.DataPartsOnly as it makes no sense for 0 RF
        Erase(mediumIndex);
    }
}

inline void TChunkReplication::Aggregate(int mediumIndex, TReplicationPolicy policy)
{
    auto [it, inserted] = Insert(mediumIndex, policy);
    if (!inserted) {
        it->Policy() |= policy;
    }
}

inline TReplicationPolicy TChunkReplication::Get(int mediumIndex) const
{
    auto it = Find(mediumIndex);
    if (it != end()) {
        return it->Policy();
    } else {
        return EmptyReplicationPolicy;
    }
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
        std::equal(lhs.begin(), lhs.end(), rhs.begin(), rhs.end());
}

inline bool operator!=(const TChunkReplication& lhs, const TChunkReplication& rhs)
{
    return !(lhs == rhs);
}

template <class T>
/*static*/ inline auto TChunkReplication::Find(T& entries, int mediumIndex) -> decltype(entries.begin())
{
    TEntry entry(mediumIndex, TReplicationPolicy()); // the comparator ignores policy during lookup
    auto [rangeBegin, rangeEnd] = std::equal_range(entries.begin(), entries.end(), entry, TEntryComparator());
    if (rangeBegin == rangeEnd) {
        return entries.end();
    } else {
        YT_VERIFY(std::distance(rangeBegin, rangeEnd) == 1);
        return rangeBegin;
    }
}

inline TChunkReplication::const_iterator TChunkReplication::Find(int mediumIndex) const
{
    return Find(Entries_, mediumIndex);
}

inline TChunkReplication::iterator TChunkReplication::Find(int mediumIndex)
{
    return Find(Entries_, mediumIndex);
}

inline std::pair<TChunkReplication::iterator, bool> TChunkReplication::Insert(int mediumIndex, TReplicationPolicy policy)
{
    TEntry entry(mediumIndex, policy);
    auto it = std::lower_bound(
        Entries_.begin(),
        Entries_.end(),
        entry, // the comparator ignores policy during lookup
        TEntryComparator());
    if (it != Entries_.end() && !TEntryComparator()(entry, *it)) {
        return {it, false};
    }
    it = Entries_.insert(it, TEntry(mediumIndex, policy));
    return {it, true};
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
    YT_ASSERT((Account == rhs.Account) == (Account->GetId() == rhs.Account->GetId()));
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
    YT_ASSERT((Account == rhs.Account) == (Account->GetId() == rhs.Account->GetId()));

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
{
    YT_VERIFY(replicationPolicy);
}

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

    YT_ASSERT(std::is_sorted(Entries_.begin(), Entries_.end()));

    for (const auto& entry : Entries_) {
        HashCombine(result, entry);
    }

    return result;
}

////////////////////////////////////////////////////////////////////////////////

inline const TChunkRequisition& TChunkRequisitionRegistry::GetRequisition(TChunkRequisitionIndex index) const
{
    return GetOrCrash(IndexToItem_, index).Requisition;
}

inline const TChunkReplication& TChunkRequisitionRegistry::GetReplication(TChunkRequisitionIndex index) const
{
    return GetOrCrash(IndexToItem_, index).Replication;
}

inline TChunkRequisitionIndex TChunkRequisitionRegistry::GenerateIndex()
{
    auto result = NextIndex_++;
    while (IndexToItem_.contains(NextIndex_)) {
        ++NextIndex_;
    }
    return result;
}

////////////////////////////////////////////////////////////////////////////////

inline TChunkRequisitionIndex TEphemeralRequisitionRegistry::Insert(const TChunkRequisition& requisition)
{
    auto index = GenerateIndex();

    YT_VERIFY(IndexToRequisition_.emplace(index, requisition).second);
    YT_VERIFY(RequisitionToIndex_.emplace(requisition, index).second);

    return index;
}

inline TChunkRequisitionIndex TEphemeralRequisitionRegistry::GenerateIndex()
{
    auto result = NextIndex_++;
    while (IndexToRequisition_.contains(NextIndex_)) {
        ++NextIndex_;
    }
    return result;
}

////////////////////////////////////////////////////////////////////////////////

template <typename T>
void FillChunkRequisitionDict(NProto::TReqUpdateChunkRequisition* request, const T& requisitionRegistry)
{
    YT_VERIFY(request->chunk_requisition_dict_size() == 0);

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

} // namespace NYT::NChunkServer
