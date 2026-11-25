#ifndef COW_TREE_ITERATOR_INL_H
    #error "Direct inclusion of this file is not allowed, include cow_tree_iterator.h"
    // For the sake of sane code completion.
    #include "cow_tree_iterator.h"
#endif

#include <library/cpp/yt/assert/assert.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

template <CCowTreeConfig TConfig>
TCowTreeIteratorBase<TConfig>::TCowTreeIteratorBase(const TCowTreeIteratorBase& other)
{
    CopyState(other);
}

template <CCowTreeConfig TConfig>
TCowTreeIteratorBase<TConfig>::TCowTreeIteratorBase(TCowTreeIteratorBase&& other) noexcept
{
    MoveState(other);
}

template <CCowTreeConfig TConfig>
TCowTreeIteratorBase<TConfig>& TCowTreeIteratorBase<TConfig>::operator=(const TCowTreeIteratorBase& other)
{
    if (this != &other) {
        ResetState();
        CopyState(other);
    }
    return *this;
}

template <CCowTreeConfig TConfig>
TCowTreeIteratorBase<TConfig>& TCowTreeIteratorBase<TConfig>::operator=(TCowTreeIteratorBase&& other) noexcept
{
    if (this != &other) {
        ResetState();
        MoveState(other);
    }
    return *this;
}

template <CCowTreeConfig TConfig>
TCowTreeIteratorBase<TConfig>::~TCowTreeIteratorBase()
{
    ResetState();
}

template <CCowTreeConfig TConfig>
TCowTreeIteratorBase<TConfig>::TCowTreeIteratorBase(TTree* tree)
    : Tree_(tree)
{ }

template <CCowTreeConfig TConfig>
TCowTreeIteratorBase<TConfig>::TCowTreeIteratorBase(TTree* tree, TLeaf* leaf, int pos, NCowTreeInternals::TInner<TConfig>* parent, int parentPos)
    : Tree_(tree)
    , Leaf_(leaf)
    , Pos_(pos)
    , Parent_(parent)
    , ParentPos_(parentPos)
    , Tracking_(tree->IsMaster())
{
    if (Tracking_) {
        LinkToLeaf();
    }
}

////////////////////////////////////////////////////////////////////////////////

template <CCowTreeConfig TConfig>
TCowTreeIteratorBase<TConfig>* TCowTreeIteratorBase<TConfig>::LeafListHead() const
{
    return static_cast<TCowTreeIteratorBase*>(Leaf_->TrackedIterators);
}

template <CCowTreeConfig TConfig>
void TCowTreeIteratorBase<TConfig>::SetLeafListHead(TCowTreeIteratorBase* head) const
{
    Leaf_->TrackedIterators = head;
}

template <CCowTreeConfig TConfig>
void TCowTreeIteratorBase<TConfig>::LinkToLeaf()
{
    PrevTracked_ = nullptr;
    NextTracked_ = LeafListHead();
    if (NextTracked_) {
        NextTracked_->PrevTracked_ = this;
    }
    SetLeafListHead(this);
}

template <CCowTreeConfig TConfig>
void TCowTreeIteratorBase<TConfig>::UnlinkFromLeaf()
{
    if (PrevTracked_) {
        PrevTracked_->NextTracked_ = NextTracked_;
    } else {
        SetLeafListHead(NextTracked_);
    }
    if (NextTracked_) {
        NextTracked_->PrevTracked_ = PrevTracked_;
    }
}

////////////////////////////////////////////////////////////////////////////////

template <CCowTreeConfig TConfig>
bool TCowTreeIteratorBase<TConfig>::operator==(const TCowTreeIteratorBase& other) const
{
    return Leaf_ == other.Leaf_ && (Leaf_ == nullptr || Pos_ == other.Pos_);
}

template <CCowTreeConfig TConfig>
void TCowTreeIteratorBase<TConfig>::Increment()
{
    YT_ASSERT(Leaf_ != nullptr);
    if (COW_TREE_LIKELY(Pos_ + 1 < Leaf_->Size)) {
        ++Pos_;
        return;
    }
    IncrementSlow();
}

template <CCowTreeConfig TConfig>
void TCowTreeIteratorBase<TConfig>::Decrement()
{
    if (COW_TREE_LIKELY(Leaf_ != nullptr && Pos_ > 0)) {
        --Pos_;
        return;
    }
    DecrementSlow();
}

////////////////////////////////////////////////////////////////////////////////

template <CCowTreeConfig TConfig>
void TCowTreeIteratorBase<TConfig>::RepairParentSlot()
{
    ParentPos_ = TCowTree<TConfig>::RepairLeafSlot(Parent_, Leaf_, ParentPos_);
}

template <CCowTreeConfig TConfig>
void TCowTreeIteratorBase<TConfig>::StepToSibling(int parentPos)
{
    auto* next = static_cast<TLeaf*>(Parent_->Children[parentPos]);
    if (Tracking_) {
        UnlinkFromLeaf();
        Leaf_ = next;
        LinkToLeaf();
    } else {
        Leaf_ = next;
    }
    ParentPos_ = parentPos;
}

template <CCowTreeConfig TConfig>
void TCowTreeIteratorBase<TConfig>::IncrementSlow()
{
    if (Parent_) {
        RepairParentSlot();
        if (ParentPos_ + 1 < Parent_->Size) {
            StepToSibling(ParentPos_ + 1);
            Pos_ = 0;
            return;
        }
        // A cross-parent transition: reseek by the leaf's own last key.
        *this = Tree_->SeekFirstGreater(Leaf_->Keys[Leaf_->Size - 1].Get());
        return;
    }
    *this = TCowTreeIteratorBase(Tree_);
}

template <CCowTreeConfig TConfig>
void TCowTreeIteratorBase<TConfig>::DecrementSlow()
{
    if (COW_TREE_UNLIKELY(Leaf_ == nullptr)) {
        *this = Tree_->SeekLast();
        return;
    }
    YT_ASSERT(Pos_ == 0);
    if (Parent_) {
        RepairParentSlot();
        if (ParentPos_ > 0) {
            StepToSibling(ParentPos_ - 1);
            Pos_ = Leaf_->Size - 1;
            return;
        }
        *this = Tree_->SeekLastLess(Leaf_->Keys[0].Get());
        return;
    }
    *this = TCowTreeIteratorBase(Tree_);
}

////////////////////////////////////////////////////////////////////////////////

template <CCowTreeConfig TConfig>
void TCowTreeIteratorBase<TConfig>::ResetState()
{
    if (Tracking_ && Leaf_) {
        UnlinkFromLeaf();
    }
    Leaf_ = nullptr;
    Parent_ = nullptr;
    Tracking_ = false;
}

template <CCowTreeConfig TConfig>
void TCowTreeIteratorBase<TConfig>::CopyState(const TCowTreeIteratorBase& other)
{
    Tree_ = other.Tree_;
    Leaf_ = other.Leaf_;
    Pos_ = other.Pos_;
    Parent_ = other.Parent_;
    ParentPos_ = other.ParentPos_;
    Tracking_ = other.Tracking_;
    if (Tracking_ && Leaf_) {
        LinkToLeaf();
    }
}

template <CCowTreeConfig TConfig>
void TCowTreeIteratorBase<TConfig>::MoveState(TCowTreeIteratorBase& other)
{
    Tree_ = other.Tree_;
    Leaf_ = other.Leaf_;
    Pos_ = other.Pos_;
    Parent_ = other.Parent_;
    ParentPos_ = other.ParentPos_;
    Tracking_ = other.Tracking_;
    if (Tracking_ && Leaf_) {
        // Take over the source's place in its leaf's tracked list.
        PrevTracked_ = other.PrevTracked_;
        NextTracked_ = other.NextTracked_;
        if (PrevTracked_) {
            PrevTracked_->NextTracked_ = this;
        } else {
            SetLeafListHead(this);
        }
        if (NextTracked_) {
            NextTracked_->PrevTracked_ = this;
        }
    }
    other.Leaf_ = nullptr;
    other.Parent_ = nullptr;
    other.Tracking_ = false;
}

////////////////////////////////////////////////////////////////////////////////

template <CCowTreeConfig TConfig, bool IsConst>
template <class TFirst, class TSecond>
TCowTreeIterator<TConfig, IsConst>::TReference::operator std::pair<TFirst, TSecond>() const
{
    return {first, second};
}

template <CCowTreeConfig TConfig, bool IsConst>
auto TCowTreeIterator<TConfig, IsConst>::TArrowProxy::operator->() -> TReference*
{
    return &Ref;
}

template <CCowTreeConfig TConfig, bool IsConst>
template <bool OtherConst>
TCowTreeIterator<TConfig, IsConst>::TCowTreeIterator(const TCowTreeIterator<TConfig, OtherConst>& other)
    requires(IsConst && !OtherConst)
    : TBase(other)
{ }

template <CCowTreeConfig TConfig, bool IsConst>
TCowTreeIterator<TConfig, IsConst>::TCowTreeIterator(TBase&& base)
    : TBase(std::move(base))
{ }

template <CCowTreeConfig TConfig, bool IsConst>
TCowTreeIterator<TConfig, IsConst>::TCowTreeIterator(typename TBase::TTree* tree)
    : TBase(tree)
{ }

template <CCowTreeConfig TConfig, bool IsConst>
template <class TResult>
TResult& TCowTreeIterator<TConfig, IsConst>::MutableValue() const
    requires(HasValue && !IsConst)
{
    // Only the master hands out mutable iterators.
    return static_cast<TCowTree<TConfig>*>(this->Tree_)->MutableValueFor(const_cast<TCowTreeIterator&>(*this));
}

template <CCowTreeConfig TConfig, bool IsConst>
auto TCowTreeIterator<TConfig, IsConst>::operator*() const -> reference
{
    YT_ASSERT(this->Leaf_ != nullptr);
    if constexpr (!HasValue) {
        return this->Leaf_->Keys[this->Pos_].Get();
    } else if constexpr (IsConst) {
        return TReference{this->Leaf_->Keys[this->Pos_].Get(), this->Leaf_->Values[this->Pos_].Get()};
    } else {
        // The value access may copy the leaf; take the key after it settles.
        auto& value = MutableValue();
        return TReference{this->Leaf_->Keys[this->Pos_].Get(), value};
    }
}

template <CCowTreeConfig TConfig, bool IsConst>
auto TCowTreeIterator<TConfig, IsConst>::operator->() const -> pointer
{
    if constexpr (HasValue) {
        return TArrowProxy{operator*()};
    } else {
        return &operator*();
    }
}

template <CCowTreeConfig TConfig, bool IsConst>
TCowTreeIterator<TConfig, IsConst>& TCowTreeIterator<TConfig, IsConst>::operator++()
{
    this->Increment();
    return *this;
}

template <CCowTreeConfig TConfig, bool IsConst>
TCowTreeIterator<TConfig, IsConst> TCowTreeIterator<TConfig, IsConst>::operator++(int)
{
    TCowTreeIterator tmp = *this;
    this->Increment();
    return tmp;
}

template <CCowTreeConfig TConfig, bool IsConst>
TCowTreeIterator<TConfig, IsConst>& TCowTreeIterator<TConfig, IsConst>::operator--()
{
    this->Decrement();
    return *this;
}

template <CCowTreeConfig TConfig, bool IsConst>
TCowTreeIterator<TConfig, IsConst> TCowTreeIterator<TConfig, IsConst>::operator--(int)
{
    TCowTreeIterator tmp = *this;
    this->Decrement();
    return tmp;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
