#ifndef SKIP_LIST_INL_H_
#error "Direct inclusion of this file is not allowed, include skip_list.h"
#endif
#undef SKIP_LIST_INL_H_

#include "chunked_memory_pool.h"

#include <type_traits>

#include <util/random/random.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

template <class TKey, class TComparer>
TSkipList<TKey, TComparer>::TIterator::TIterator()
    : Owner_(nullptr)
    , Current_(nullptr)
{ }

template <class TKey, class TComparer>
TSkipList<TKey, TComparer>::TIterator::TIterator(
    const TSkipList* owner,
    const TNode* current)
    : Owner_(owner)
    , Current_(current)
{ }

template <class TKey, class TComparer>
TSkipList<TKey, TComparer>::TIterator::TIterator(const TIterator& other)
    : Owner_(other.Owner_)
    , Current_(other.Current_)
{ }

template <class TKey, class TComparer>
const TKey& TSkipList<TKey, TComparer>::TIterator::GetCurrent() const
{
    YASSERT(IsValid());
    return Current_->GetKey();
}

template <class TKey, class TComparer>
bool TSkipList<TKey, TComparer>::TIterator::IsValid() const
{
    return Current_;
}

template <class TKey, class TComparer>
void TSkipList<TKey, TComparer>::TIterator::MoveNext()
{
    if (Current_) {
        Current_ = Current_->GetNext(0);
    }
}

template <class TKey, class TComparer>
typename TSkipList<TKey, TComparer>::TIterator& TSkipList<TKey, TComparer>::TIterator::operator=(const TIterator& other)
{
    Owner_ = other.Owner_;
    Current_ = other.Current_;
    return *this;
}

////////////////////////////////////////////////////////////////////////////////

template <class TKey, class TComparer>
TSkipList<TKey, TComparer>::TNode::TNode(const TKey& key, int height)
    : Key_(key)
{
    ::memset(const_cast<intptr_t*>(Next_), 0, sizeof (TAtomic)* height);
}

template <class TKey, class TComparer>
const TKey& TSkipList<TKey, TComparer>::TNode::GetKey() const
{
    return Key_;
}

template <class TKey, class TComparer>
typename TSkipList<TKey, TComparer>::TNode* TSkipList<TKey, TComparer>::TNode::GetNext(int height) const
{
    return reinterpret_cast<TNode*>(AtomicGet(Next_[height]));
}

template <class TKey, class TComparer>
void TSkipList<TKey, TComparer>::TNode::SetNext(int height, TNode* next)
{
    AtomicSet(Next_[height], reinterpret_cast<intptr_t>(next));
}

template <class TKey, class TComparer>
void TSkipList<TKey, TComparer>::TNode::InsertAfter(int height, TNode** prevs)
{
    for (int index = 0; index < height; ++index) {
        SetNext(index, prevs[index]->GetNext(index));
        prevs[index]->SetNext(index, this);
    }
}

////////////////////////////////////////////////////////////////////////////////

template <class TKey, class TComparer>
TSkipList<TKey, TComparer>::TSkipList(
    TChunkedMemoryPool* pool,
    const TComparer& comparer)
    : Pool_(pool)
    , Comparer_(comparer)
    , Head_(AllocateHeadNode())
    , Size_(0)
    , Height_(1)
{ }

template <class TKey, class TComparer>
TSkipList<TKey, TComparer>::~TSkipList()
{
    if (!std::is_trivially_destructible<TKey>::value) {
        auto* current = Head_;
        while (current) {
            auto* next = current->GetNext(0);
            current->~TNode();
            current = next;
        }
    }
}

template <class TKey, class TComparer>
int TSkipList<TKey, TComparer>::GetSize() const
{
    return Size_;
}

template <class TKey, class TComparer>
template <class TPivot, class TNewKeyProvider, class TExistingKeyConsumer>
void TSkipList<TKey, TComparer>::Insert(
    const TPivot& pivot,
    const TNewKeyProvider& newKeyProvider,
    const TExistingKeyConsumer& existingKeyConsumer)
{
    TNode* prevs[MaxHeight];

    auto* lowerBound = DoFindGreaterThanOrEqualTo(pivot, prevs);
    if (lowerBound && Comparer_(lowerBound->GetKey(), pivot) == 0) {
        existingKeyConsumer(lowerBound->GetKey());
        return;
    }

    int currentHeight = AtomicGet(Height_);
    int randomHeight = GenerateHeight();

    // Upgrade current height if needed.
    if (randomHeight > currentHeight) {
        for (int index = currentHeight; index < randomHeight; ++index) {
            prevs[index] = Head_;
        }
        AtomicSet(Height_, randomHeight);
    }

    // Insert a new node.
    auto* node = AllocateNode(newKeyProvider(), randomHeight);
    node->InsertAfter(randomHeight, prevs);
    AtomicIncrement(Size_);
}

template <class TKey, class TComparer>
bool TSkipList<TKey, TComparer>::Insert(const TKey& key)
{
    bool result = true;
    Insert(
        key,
        [&] () { return key; },
        [&] (const TKey& /*key*/) { result = false; });
    return result;
}

template <class TKey, class TComparer>
template <class TPivot>
typename TSkipList<TKey, TComparer>::TIterator TSkipList<TKey, TComparer>::FindGreaterThanOrEqualTo(const TPivot& pivot) const
{
    auto* lowerBound = DoFindGreaterThanOrEqualTo(pivot, nullptr);
    return lowerBound
        ? TIterator(this, lowerBound)
        : TIterator();
}

template <class TKey, class TComparer>
template <class TPivot>
typename TSkipList<TKey, TComparer>::TIterator TSkipList<TKey, TComparer>::FindEqualTo(const TPivot& pivot) const
{
    auto* lowerBound = DoFindGreaterThanOrEqualTo(pivot, nullptr);
    return lowerBound && Comparer_(lowerBound->GetKey(), pivot) == 0
        ? TIterator(this, lowerBound)
        : TIterator();
}

template <class TKey, class TComparer>
int TSkipList<TKey, TComparer>::GenerateHeight()
{
    int height = 1;
    while (height < MaxHeight && (RandomNumber<unsigned int>() % InverseProbability) == 0) {
        ++height;
    }
    return height;
}

template <class TKey, class TComparer>
typename TSkipList<TKey, TComparer>::TNode* TSkipList<TKey, TComparer>::AllocateNode(const TKey& key, int height)
{
    // -1 since Next_ is of size 1
    size_t size = sizeof (TNode) + sizeof (TAtomic) * (height - 1);
    auto* buffer = Pool_->AllocateAligned(size);
    new (buffer)TNode(key, height);
    return reinterpret_cast<TNode*>(buffer);
}

template <class TKey, class TComparer>
typename TSkipList<TKey, TComparer>::TNode* TSkipList<TKey, TComparer>::AllocateHeadNode()
{
    return AllocateNode(TKey(), MaxHeight);
}

template <class TKey, class TComparer>
template <class TPivot>
typename TSkipList<TKey, TComparer>::TNode* TSkipList<TKey, TComparer>::DoFindGreaterThanOrEqualTo(const TPivot& pivot, TNode** prevs) const
{
    auto* current = Head_;
    int height = AtomicGet(Height_) - 1;
    while (true) {
        auto* next = current->GetNext(height);
        if (next && Comparer_(next->GetKey(), pivot) < 0) {
            current = next;
        } else {
            if (prevs) {
                prevs[height] = current;
            }
            if (height > 0) {
                --height;
            } else {
                return next == Head_ ? nullptr : next;
            }
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
