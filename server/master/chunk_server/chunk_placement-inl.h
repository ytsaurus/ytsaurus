#ifndef CHUNK_PLACEMENT_INL_H_
#error "Direct inclusion of this file is not allowed, include chunk_placement.h"
#endif

namespace NYT::NChunkServer {

///////////////////////////////////////////////////////////////////////////////

inline bool TPlacementDomain::operator==(const TPlacementDomain& rhs) const
{
    if (this == &rhs) {
        return true;
    }

    YT_ASSERT(!DataCenter || !rhs.DataCenter || (DataCenter->GetId() == rhs.DataCenter->GetId()) == (DataCenter == rhs.DataCenter));
    YT_ASSERT((Medium->GetId() == rhs.Medium->GetId()) == (Medium == rhs.Medium));

    return DataCenter == rhs.DataCenter && Medium == rhs.Medium;
}

inline size_t TPlacementDomain::GetHash() const
{
    auto hasher = NObjectClient::TDirectObjectIdHash();
    auto result = hasher(DataCenter ? DataCenter->GetId() : NNodeTrackerServer::TDataCenterId());
    HashCombine(result, hasher(Medium->GetId()));
    return result;
}

////////////////////////////////////////////////////////////////////////////////

template <class T, class TCompare>
struct TReusableMergeIterator<T, TCompare>::TRangeCompare
{
    bool operator()(const TRange& lhs, const TRange& rhs) const
    {
        // Note the argument reversal: in C++, the front of a heap holds the
        // highest value, and we need the lowest.
        return TCompare()(*rhs.Begin, *lhs.Begin);
    }
};

template <class T, class TCompare>
template <class U>
void TReusableMergeIterator<T, TCompare>::AddRange(U&& range)
{
    auto begin = std::begin(range);
    auto end = std::end(range);

    if (begin == end) {
        return;
    }

    Ranges_.emplace_back(begin, end);
    std::push_heap(Ranges_.begin(), Ranges_.end(), TRangeCompare());
}

template <class T, class TCompare>
void TReusableMergeIterator<T, TCompare>::Reset()
{
    Ranges_.clear();
}

template <class T, class TCompare>
decltype(*std::declval<T>()) TReusableMergeIterator<T, TCompare>::operator*()
{
    return *Ranges_.front().Begin;
}

template <class T, class TCompare>
decltype(&*std::declval<T>()) TReusableMergeIterator<T, TCompare>::operator->()
{
    return &operator*();
}

template <class T, class TCompare>
bool TReusableMergeIterator<T, TCompare>::IsValid()
{
    return !Ranges_.empty();
}

template <class T, class TCompare>
TReusableMergeIterator<T, TCompare>& TReusableMergeIterator<T, TCompare>::operator++()
{
    std::pop_heap(Ranges_.begin(), Ranges_.end(), TRangeCompare());
    auto& range = Ranges_.back();
    ++range.Begin;
    if (range.Begin == range.End) {
        Ranges_.pop_back();
    } else {
        std::push_heap(Ranges_.begin(), Ranges_.end(), TRangeCompare());
    }

    return *this;
}

////////////////////////////////////////////////////////////////////////////////

template <class T>
void TChunkPlacement::ForEachDataCenter(const TDataCenterSet* dataCenters, T callback)
{
    const auto& allDataCenters = Bootstrap_->GetNodeTracker()->DataCenters();

    if (dataCenters) {
        if (dataCenters->count(nullptr) != 0) {
            callback(nullptr);
        }

        // NB: SmallSet doesn't support iteration.
        for (const auto& [dataCenterId, dataCenter] : allDataCenters) {
            if (dataCenters->count(dataCenter) != 0) {
                callback(dataCenter);
            }
        }
    } else {
        callback(nullptr);

        for (const auto& [dataCenterId, dataCenter] : allDataCenters) {
            callback(dataCenter);
        }
    }
}

} // namespace NYT::NChunkServer
