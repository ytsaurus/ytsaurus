#pragma once

#include "common.h"
#include "chunked_memory_pool.h"
#include "enum.h"

#include <atomic>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

typedef i64 TRcuTreeTimestamp;

template <class TKey, class TComparer>
class TRcuTree
{
public:
    TRcuTree(
        TChunkedMemoryPool* pool,
        const TComparer& comparer);

    int Size() const;

    bool Insert(TKey newKey, TKey* existingKey = nullptr);

    class TReader;
    TReader* CreateReader();

private:
    struct TNode
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
            TNode* GCNext;
            TNode* FreeNext;
        };
        TKey Key;
        TNode* Left;
        TNode* Right;
    };

    TChunkedMemoryPool* Pool_;
    TComparer Comparer_;
    
    int Size_;
    TNode* Root_;
    std::atomic<TRcuTreeTimestamp> Timestamp_;

    std::vector<TIntrusivePtr<TReader>> Readers_;

    int GCSize_;       // number of nodes in GC queue
    TNode* GCHead_;    // earliest GC node
    TNode* GCTail_;    // latest GC node
    TNode* FreeHead_;  // first free node


    static const int MaxGCSize = 1024;
    static const TRcuTreeTimestamp InactiveReaderTimestamp = 0x7fffffffffffffff; // std::numeric_limits<TRcuTreeTimestamp>::max();

    TRcuTreeTimestamp GenerateWriterTimestamp();
    TRcuTreeTimestamp ComputeEarliestReaderTimestamp();

    TNode* AllocateNode();
    void FreeNode(TNode* node);
    void MaybeGCCollect();

    void Rebalance(TNode* x);
    void ReplaceChild(TNode* x, TNode* y);

};

template <class TKey, class TComparer>
class TRcuTree<TKey, TComparer>::TReader
    : public TIntrinsicRefCounted
{
public:
    template <class TPivot>
    bool Find(TPivot pivot, TKey* key = nullptr)
    {
        Acquire();
        auto* current = Tree_->Root_;
        while (current) {
            int result = Comparer_(pivot, current->Key);
            if (result == 0) {
                if (key) {
                    *key = current->Key;
                }
                Release();
                return true;
            }
            if (result < 0) {
                current = current->Left;
            } else {
                current = current->Right;
            }
        }
        Release();
        return false;
    }


    template <class TPivot>
    void BeginScan(TPivot pivot)
    {
        Acquire();
        Reset();

        auto* current = Tree_->Root_;
        if (!current) {
            return;
        }

        Push(RightChildToToken(current));

        while (true) {
            int result = Comparer_(pivot, current->Key);
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
            } else {
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

    bool IsValid() const
    {
        return StackTop_ >= StackBottom_;
    }

    TKey GetCurrentKey() const
    {
        YASSERT(IsValid());
        return TokenToChild(Peek())->Key;
    }

    void Advance()
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
        } else {
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

    void EndScan()
    {
        Release();
        Reset();
    }


private:
    template <class TType, class A1>
    friend TIntrusivePtr<TType> NYT::New(A1&&);

    friend class TRcuTree;

    TRcuTree* Tree_;

    TComparer Comparer_; // to avoid indirection on Tree_
    std::atomic<TRcuTreeTimestamp> Timestamp_;

    std::vector<intptr_t> Stack_; // TNode* + (1 if this is a left child)
    intptr_t* StackTop_;
    intptr_t* StackBottom_;


    explicit TReader(TRcuTree* tree)
        : Tree_(std::move(tree))
        , Comparer_(Tree_->Comparer_)
        , Timestamp_(InactiveReaderTimestamp)
        , Stack_(64) // should be enough for every balanced tree
        , StackTop_(Stack_.data() - 1)
        , StackBottom_(Stack_.data())
    { }


    void Acquire()
    {
        auto timestamp = Tree_->Timestamp_.load();
        Timestamp_.store(timestamp);
    }

    void Release()
    {
        Timestamp_.store(InactiveReaderTimestamp);
    }

    void Reset()
    {
        StackTop_ = StackBottom_ - 1;
    }


    static TNode* TokenToChild(intptr_t token)
    {
        return reinterpret_cast<TNode*>(token & ~1);
    }

    static intptr_t LeftChildToToken(TNode* node)
    {
        return reinterpret_cast<intptr_t>(node) | 1;
    }

    static intptr_t RightChildToToken(TNode* node)
    {
        return reinterpret_cast<intptr_t>(node);
    }

    static bool IsLeftChild(intptr_t token)
    {
        return token & 1;
    }


    void Push(intptr_t value)
    {
        *++StackTop_ = value;
    }

    intptr_t Peek() const
    {
        return *StackTop_;
    }

    void Pop()
    {
        --StackTop_;
    }

};

template <class TKey, class TComparer>
TRcuTree<TKey, TComparer>::TRcuTree(
    TChunkedMemoryPool* pool,
    const TComparer& comparer)
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
bool TRcuTree<TKey, TComparer>::Insert(TKey newKey, TKey* existingKey)
{
    ++Timestamp_;
    MaybeGCCollect();

    TNode* newNode;
    auto* current = Root_;
    if (current) {
        while (true) {
            int result = Comparer_(newKey, current->Key);
            if (result == 0) {
                if (existingKey) {
                    *existingKey = current->Key;
                }
                return false;
            }
            if (result < 0) {
                auto* left = current->Left;
                if (!left) {
                    newNode = AllocateNode();
                    newNode->Left = newNode->Right = nullptr;
                    newNode->Parent = current;
                    newNode->Flags.Red = false;
                    newNode->Key = newKey;
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
                    newNode->Key = newKey;
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
        newNode->Key = newKey;
        Root_ = newNode;
    }

    ++Size_;
    Rebalance(newNode);
    return true;
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
                    //        q                     p'
                    //       / \                   / \
                    //      p   d       =>        x   q'
                    //     / \                       / \
                    //    x   c                     c   d

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
                    //        q                     q
                    //       / \                   / \
                    //      p   d       =>        x'  d
                    //     / \                   / \
                    //    a   x                 p'  c
                    //       / \               / \
                    //      b   c             a   b

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
                    //     q                     p'
                    //    / \                   / \
                    //   c   p       =>        q'  x
                    //      / \               / \     
                    //     d   x             c   d

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
                    //        q                     q
                    //       / \                   / \
                    //      a   p       =>        a   x'
                    //         / \                   / \
                    //        x   d                 b   p'
                    //       / \                       / \
                    //      b   c                     c   d

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

#define RCU_TREE_INL_H_
#include "rcu_tree-inl.h"
#undef RCU_TREE_INL_H_
