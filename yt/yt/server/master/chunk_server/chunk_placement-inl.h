#ifndef CHUNK_PLACEMENT_INL_H_
#error "Direct inclusion of this file is not allowed, include chunk_placement.h"
#endif

namespace NYT::NChunkServer {

///////////////////////////////////////////////////////////////////////////////

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

} // namespace NYT::NChunkServer
