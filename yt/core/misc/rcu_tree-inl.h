#ifndef RCU_TREE_INL_H_
#error "Direct inclusion of this file is not allowed, include rcu_tree.h"
#endif
#undef RCU_TREE_INL_H_

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

template <class TKey, class TComparer>
struct TRcuTree<TKey, TComparer>::TNode
{
    union
    {
        struct
        {
            bool Red;
        } Flags;
        TRcuTreeTimestamp GCTimestamp;
    };
    union
    {
        TNode* Parent;
        TNode* Next;
    };
    TKey Key;
    TNode* Left;
    TNode* Right;
};

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
    , FirstActiveScanner_(nullptr)
    , FirstInactiveScanner_(nullptr)
    , InactiveScannerCount_(0)
    , GCNodeCount_(0)
    , FirstGCNode_(nullptr)
    , LastGCNode_(nullptr)
    , FirstFreeNode_(nullptr)
{
    VERIFY_THREAD_AFFINITY(WriterThread);
}

template <class TKey, class TComparer>
TRcuTree<TKey, TComparer>::~TRcuTree()
{
    VERIFY_THREAD_AFFINITY(WriterThread);

    DestroyScannerList(FirstActiveScanner_);
    DestroyScannerList(FirstInactiveScanner_);
}

template <class TKey, class TComparer>
int TRcuTree<TKey, TComparer>::Size() const
{
    VERIFY_THREAD_AFFINITY(WriterThread);

    return Size_;
}

template <class TKey, class TComparer>
template <class TPivot, class TNewKeyProvider, class TExistingKeyConsumer>
void TRcuTree<TKey, TComparer>::Insert(
    TPivot pivot,
    TNewKeyProvider newKeyProvider,
    TExistingKeyConsumer existingKeyConsumer)
{
    VERIFY_THREAD_AFFINITY(WriterThread);

    ++Timestamp_;
    MaybeGCCollect();

    TNode* newNode;
    auto* current = Root_;
    if (current) {
        while (true) {
            int result = (*Comparer_)(pivot, current->Key);
            if (result == 0) {
                existingKeyConsumer(current->Key);
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
    VERIFY_THREAD_AFFINITY(WriterThread);

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
typename TRcuTree<TKey, TComparer>::TScanner* TRcuTree<TKey, TComparer>::AllocateScanner()
{
    VERIFY_THREAD_AFFINITY(WriterThread);

    TScanner* scanner;
    if (FirstInactiveScanner_) {
        // Take from pool.
        scanner = FirstInactiveScanner_;
        FirstInactiveScanner_ = FirstInactiveScanner_->NextScanner_;
        --InactiveScannerCount_;
    } else {
        // Create a new one.
        scanner = new TScanner(this);
    }
    
    // Push to active list.
    if (FirstActiveScanner_) {
        FirstActiveScanner_->PrevScanner_ = scanner;
    }
    scanner->NextScanner_ = FirstActiveScanner_;
    scanner->PrevScanner_ = nullptr;
    FirstActiveScanner_ = scanner;
    
    return scanner;
}

template <class TKey, class TComparer>
void TRcuTree<TKey, TComparer>::FreeScanner(TScanner* scanner)
{
    VERIFY_THREAD_AFFINITY(WriterThread);

    // Remove from active list.
    if (scanner->NextScanner_) {
        scanner->NextScanner_->PrevScanner_ = scanner->PrevScanner_;
    }
    if (scanner->PrevScanner_) {
        scanner->PrevScanner_->NextScanner_ = scanner->NextScanner_;
    }
    if (scanner == FirstActiveScanner_) {
        FirstActiveScanner_ = scanner->NextScanner_;
    }

    if (InactiveScannerCount_ < MaxPooledScanners) {
        // Tidy up.
        scanner->EndScan();

        // Push to inactive list.
        scanner->NextScanner_ = FirstInactiveScanner_;
        scanner->PrevScanner_ = nullptr;
        FirstInactiveScanner_ = scanner;
        ++InactiveScannerCount_;
    } else {
        // Destroy.
        delete scanner;
    }
}

template <class TKey, class TComparer>
TRcuTreeTimestamp TRcuTree<TKey, TComparer>::ComputeEarliestReaderTimestamp()
{
    auto result = std::numeric_limits<TRcuTreeTimestamp>::max();
    auto* current = FirstActiveScanner_;
    while (current) {
        result = std::min(result, current->Timestamp_.load());
        current = current->NextScanner_;
    }
    return result;
}

template <class TKey, class TComparer>
typename TRcuTree<TKey, TComparer>::TNode* TRcuTree<TKey, TComparer>::AllocateNode()
{
    if (FirstFreeNode_) {
        auto* node = FirstFreeNode_;
        FirstFreeNode_ = node->Next;
        --GCNodeCount_;
        return node;
    } else {
        return Pool_->Allocate<TNode>();
    }
}

template <class TKey, class TComparer>
void TRcuTree<TKey, TComparer>::FreeNode(TNode* node)
{
    ++GCNodeCount_;
    node->GCTimestamp = Timestamp_.load(std::memory_order_relaxed);
    node->Next = nullptr;
    if (LastGCNode_) {
        YASSERT(FirstGCNode_); 
        LastGCNode_->Next = node;
    } else {
        YASSERT(!FirstGCNode_); 
        FirstGCNode_ = LastGCNode_= node;        
    }
    LastGCNode_ = node;
}

template <class TKey, class TComparer>
void TRcuTree<TKey, TComparer>::MaybeGCCollect()
{
    if (GCNodeCount_ < MaxGCSize)
        return;

    auto readerTimestamp = ComputeEarliestReaderTimestamp();
    auto* node = FirstGCNode_;
    while (node && node->GCTimestamp < readerTimestamp) {
        auto* nextNode = node->Next;
        node->Next = FirstFreeNode_;
        FirstFreeNode_ = node;
        GCNodeCount_--;
        node = nextNode;
    }

    FirstGCNode_ = node;
    if (!FirstGCNode_) {
        LastGCNode_ = nullptr;
    }
}

template <class TKey, class TComparer>
void TRcuTree<TKey, TComparer>::DestroyScannerList(TScanner* first)
{
    auto* current = first;
    while (current) {
        auto* next = current->NextScanner_;
        delete current;
        current = next;
    }
}

////////////////////////////////////////////////////////////////////////////////

template <class TKey, class TComparer>
TRcuTreeScanner<TKey, TComparer>::TRcuTreeScanner(TTree* tree)
    : Tree_(tree)
    , NextScanner_(nullptr)
    , PrevScanner_(nullptr)
    , Comparer_(Tree_->Comparer_)
    , Timestamp_(TTree::InactiveReaderTimestamp)
    , StackTop_(Stack_.data() - 1)
    , StackBottom_(Stack_.data())
{ }

template <class TKey, class TComparer>
template <class TPivot>
bool TRcuTreeScanner<TKey, TComparer>::Find(TPivot pivot, TKey* key /*= nullptr*/)
{
    Acquire();
    auto* current = Tree_->Root_;
    while (current) {
        int result = (*Comparer_)(pivot, current->Key);
        if (result == 0) {
            if (key) {
                *key = current->Key;
            }
            Release();
            return true;
        }
        if (result < 0) {
            current = current->Left;
        }
        else {
            current = current->Right;
        }
    }
    Release();
    return false;
}

template <class TKey, class TComparer>
template <class TPivot>
void TRcuTreeScanner<TKey, TComparer>::BeginScan(TPivot pivot)
{
    Acquire();
    Reset();

    auto* current = Tree_->Root_;
    if (!current) {
        return;
    }

    Push(RightChildToToken(current));

    while (true) {
        int result = (*Comparer_)(pivot, current->Key);
        if (result == 0) {
            return;
        }
        if (result < 0) {
            auto* left = current->Left;
            if (!left) {
                return;
            }
            Push(LeftChildToToken(left));
            current = left;
        }
        else {
            auto* right = current->Right;
            if (!right) {
                Advance();
                return;
            }
            Push(RightChildToToken(right));
            current = right;
        }
    }
}

template <class TKey, class TComparer>
bool TRcuTreeScanner<TKey, TComparer>::IsValid() const
{
    return StackTop_ >= StackBottom_;
}

template <class TKey, class TComparer>
TKey TRcuTreeScanner<TKey, TComparer>::GetCurrent() const
{
    YASSERT(IsValid());
    return TokenToChild(Peek())->Key;
}

template <class TKey, class TComparer>
void TRcuTreeScanner<TKey, TComparer>::Advance()
{
    YASSERT(IsValid());
    auto* current = TokenToChild(Peek());
    auto* right = current->Right;
    if (right) {
        Push(RightChildToToken(right));
        auto* left = right->Left;
        while (left) {
            Push(LeftChildToToken(left));
            left = left->Left;
        }
    }
    else {
        while (true) {
            if (!IsValid()) {
                return;
            }
            if (IsLeftChild(Peek())) {
                Pop();
                return;
            }
            Pop();
        }
    }
}

template <class TKey, class TComparer>
void TRcuTreeScanner<TKey, TComparer>::EndScan()
{
    Release();
    Reset();
}

template <class TKey, class TComparer>
void TRcuTreeScanner<TKey, TComparer>::Acquire()
{
    auto timestamp = Tree_->Timestamp_.load();
    Timestamp_.store(timestamp);
}

template <class TKey, class TComparer>
void TRcuTreeScanner<TKey, TComparer>::Release()
{
    Timestamp_.store(TTree::InactiveReaderTimestamp);
}

template <class TKey, class TComparer>
void TRcuTreeScanner<TKey, TComparer>::Reset()
{
    StackTop_ = StackBottom_ - 1;
}

template <class TKey, class TComparer>
typename TRcuTreeScanner<TKey, TComparer>::TNode* TRcuTreeScanner<TKey, TComparer>::TokenToChild(intptr_t token)
{
    return reinterpret_cast<TNode*>(token & ~1);
}

template <class TKey, class TComparer>
intptr_t TRcuTreeScanner<TKey, TComparer>::LeftChildToToken(TNode* node)
{
    auto token = reinterpret_cast<intptr_t>(node);
    YASSERT((token & 1) == 0);
    return token | 1;
}

template <class TKey, class TComparer>
intptr_t TRcuTreeScanner<TKey, TComparer>::RightChildToToken(TNode* node)
{
    auto token = reinterpret_cast<intptr_t>(node);
    YASSERT((token & 1) == 0);
    return token;
}

template <class TKey, class TComparer>
bool TRcuTreeScanner<TKey, TComparer>::IsLeftChild(intptr_t token)
{
    return (token & 1) != 0;
}

template <class TKey, class TComparer>
void TRcuTreeScanner<TKey, TComparer>::Push(intptr_t value)
{
    *++StackTop_ = value;
}

template <class TKey, class TComparer>
intptr_t TRcuTreeScanner<TKey, TComparer>::Peek() const
{
    return *StackTop_;
}

template <class TKey, class TComparer>
void TRcuTreeScanner<TKey, TComparer>::Pop()
{
    --StackTop_;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
