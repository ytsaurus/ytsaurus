#ifndef RCU_TREE_INL_H_
#error "Direct inclusion of this file is not allowed, include rcu_tree.h"
#endif
#undef RCU_TREE_INL_H_

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

template <class TKey, class TComparer>
TRcuTree<TKey, TComparer>::TRcuTree(
    TChunkedMemoryPool* pool,
    const TComparer* comparer)
    : Pool_(pool)
    , Comparer_(comparer)
    , Size_(0)
    , Root_(nullptr)
    , Timestamp_(0)
    , GCSize_(0)
    , GCHead_(nullptr)
    , GCTail_(nullptr)
    , FreeHead_(nullptr)
{ }

template <class TKey, class TComparer>
int TRcuTree<TKey, TComparer>::Size() const
{
    return Size_;
}

template <class TKey, class TComparer>
template <class TPivot, class TNewKeyProvider, class TExistingKeyAcceptor>
void TRcuTree<TKey, TComparer>::Insert(
    TPivot pivot,
    TNewKeyProvider newKeyProvider,
    TExistingKeyAcceptor existingKeyAcceptor)
{
    ++Timestamp_;
    MaybeGCCollect();

    TNode* newNode;
    auto* current = Root_;
    if (current) {
        while (true) {
            int result = (*Comparer_)(pivot, current->Key);
            if (result == 0) {
                existingKeyAcceptor(current->Key);
                return;
            }
            if (result < 0) {
                auto* left = current->Left;
                if (!left) {
                    newNode = AllocateNode();
                    newNode->Left = newNode->Right = nullptr;
                    newNode->Parent = current;
                    newNode->Flags.Red = false;
                    newNode->Key = newKeyProvider();
                    current->Left = newNode;
                    break;
                }
                current = left;
            } else {
                auto* right = current->Right;
                if (!right) {
                    newNode = AllocateNode();
                    newNode->Left = newNode->Right = nullptr;
                    newNode->Parent = current;
                    newNode->Flags.Red = false;
                    newNode->Key = newKeyProvider();
                    current->Right = newNode; 
                    break;
                }
                current = right;
            }
        }
    } else {
        newNode = AllocateNode();
        newNode->Left = newNode->Right = newNode->Parent = nullptr;
        newNode->Flags.Red = false;
        newNode->Key = newKeyProvider();
        Root_ = newNode;
    }

    ++Size_;
    Rebalance(newNode);
}

template <class TKey, class TComparer>
bool TRcuTree<TKey, TComparer>::Insert(TKey key)
{
    bool result = true;
    Insert(
        key,
        [key] () { return key; },
        [key, &result] (TKey /*key*/) { result = false; });
    return result;
}

template <class TKey, class TComparer>
void TRcuTree<TKey, TComparer>::Rebalance(TNode* x)
{
    x->Flags.Red = true;

    while (true) {
        if (x == Root_)
            break;

        auto* p = x->Parent;
        if (!p->Flags.Red)
            break;

        auto* q = p->Parent;
        YASSERT(q);

        if (p == q->Left) {
            auto* y = q->Right;
            if (y && y->Flags.Red) {
                p->Flags.Red = false;
                y->Flags.Red = false;
                q->Flags.Red = true;
                x = q;
            } else {
                if (x == p->Left) {
                    /*
                            q                     p'
                           / \                   / \
                          p   d       =>        x   q'
                         / \                       / \
                        x   c                     c   d
                    */
                    auto* c = p->Right;
                    auto* d = q->Right;

                    auto* p_ = AllocateNode();
                    auto* q_ = AllocateNode();

                    p_->Left = x;                    x->Parent = p_;
                    p_->Right = q_;                  q_->Parent = p_;
                    p_->Key = p->Key;

                    q_->Left = c;                    if (c) c->Parent = q_;
                    q_->Right = d;                   if (d) d->Parent = q_;
                    q_->Key = q->Key;

                    p_->Flags.Red = false;
                    q_->Flags.Red = true;
                    x->Flags.Red = true;

                    ReplaceChild(q, p_);
                    FreeNode(p);
                    FreeNode(q);
                    break;
                } else {
                    /*
                            q                     q
                           / \                   / \
                          p   d       =>        x'  d
                         / \                   / \
                        a   x                 p'  c
                           / \               / \
                          b   c             a   b
                    */
                    auto* a = p->Left;
                    auto* b = x->Left;
                    auto* c = x->Right;

                    auto* p_ = AllocateNode();
                    auto* x_ = AllocateNode();

                    p_->Left = a;                    if (a) a->Parent = p_;
                    p_->Right = b;                   if (b) b->Parent = p_;
                    p_->Key = p->Key;

                    x_->Left = p_;                   p_->Parent = x_;
                    x_->Right = c;                   if (c) c->Parent = x_;
                    x_->Key = x->Key;

                    q->Left = x_;                    x_->Parent = q;

                    x_->Flags.Red = false;
                    p_->Flags.Red = true;

                    FreeNode(x);
                    FreeNode(p);
                    break;
                }
            }
        } else {
            auto* y = q->Left;
            if (y && y->Flags.Red) {
                p->Flags.Red = false;
                y->Flags.Red = false;
                q->Flags.Red = true;
                x = q;
            } else {
                if (x == p->Right) {
                    /*
                         q                     p'
                        / \                   / \
                       c   p       =>        q'  x
                          / \               / \    
                         d   x             c   d
                    */
                    auto* c = q->Left;
                    auto* d = p->Left;

                    auto* p_ = AllocateNode();
                    auto* q_ = AllocateNode();

                    p_->Left = q_;                   q_->Parent = p_;
                    p_->Right = x;                   x->Parent = p_;
                    p_->Key = p->Key;

                    q_->Left = c;                    if (c) c->Parent = q_;
                    q_->Right = d;                   if (d) d->Parent = q_;
                    q_->Key = q->Key;

                    p_->Flags.Red = false;
                    q_->Flags.Red = true;
                    x->Flags.Red = true;

                    ReplaceChild(q, p_);
                    FreeNode(p);
                    FreeNode(q);
                    break;
                } else {
                    /*
                            q                     q
                           / \                   / \
                          a   p       =>        a   x'
                             / \                   / \
                            x   d                 b   p'
                           / \                       / \
                          b   c                     c   d
                    */
                    auto* b = x->Left;
                    auto* c = x->Right;
                    auto* d = p->Right;

                    auto* p_ = AllocateNode();
                    auto* x_ = AllocateNode();

                    p_->Left = c;                    if (c) c->Parent = p_;
                    p_->Right = d;                   if (d) d->Parent = p_;
                    p_->Key = p->Key;

                    x_->Left = b;                    if (b) b->Parent = x_;
                    x_->Right = p_;                  p_->Parent = x_;
                    x_->Key = x->Key;

                    q->Right = x_;                   x_->Parent = q;

                    x_->Flags.Red = false;
                    p_->Flags.Red = true;

                    FreeNode(x);
                    FreeNode(p);
                    break;
                }
            }
        }
    }

    Root_->Flags.Red = false;
}

template <class TKey, class TComparer>
void TRcuTree<TKey, TComparer>::ReplaceChild(TNode* x, TNode* y)
{
    y->Parent = x->Parent;
    if (x == Root_) {
        Root_ = y;
    } else {
        if (x == x->Parent->Left) {
            x->Parent->Left = y;
        } else {
            x->Parent->Right = y;
        }
    }
}

template <class TKey, class TComparer>
typename TRcuTree<TKey, TComparer>::TReader* TRcuTree<TKey, TComparer>::CreateReader()
{
    auto reader = New<TReader>(this);
    Readers_.push_back(reader);
    return reader.Get();
}

template <class TKey, class TComparer>
TRcuTreeTimestamp TRcuTree<TKey, TComparer>::ComputeEarliestReaderTimestamp()
{
    auto result = std::numeric_limits<TRcuTreeTimestamp>::max();
    for (const auto& reader : Readers_) {
        result = std::min(result, reader->Timestamp_.load());
    }
    return result;
}

template <class TKey, class TComparer>
typename TRcuTree<TKey, TComparer>::TNode* TRcuTree<TKey, TComparer>::AllocateNode()
{
    if (FreeHead_) {
        auto* node = FreeHead_;
        FreeHead_ = node->FreeNext;
        --GCSize_;
        return node;
    } else {
        return Pool_->Allocate<TNode>();
    }
}

template <class TKey, class TComparer>
void TRcuTree<TKey, TComparer>::FreeNode(TNode* node)
{
    ++GCSize_;
    node->GCTimestamp = Timestamp_.load(std::memory_order_relaxed);
    node->GCNext = nullptr;
    if (GCTail_) {
        YASSERT(GCHead_); 
        GCTail_->GCNext = node;
    } else {
        YASSERT(!GCHead_); 
        GCHead_ = GCTail_= node;        
    }
    GCTail_ = node;
}

template <class TKey, class TComparer>
void TRcuTree<TKey, TComparer>::MaybeGCCollect()
{
    if (GCSize_ < MaxGCSize)
        return;

    auto readerTimestamp = ComputeEarliestReaderTimestamp();
    auto* node = GCHead_;
    while (node && node->GCTimestamp < readerTimestamp) {
        auto* nextNode = node->GCNext;
        node->FreeNext = FreeHead_;
        FreeHead_ = node;
        GCSize_--;
        node = nextNode;
    }

    GCHead_ = node;
    if (!GCHead_) {
        GCTail_ = nullptr;
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
