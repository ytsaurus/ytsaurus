#pragma once

#include "common.h"
#include "chunked_memory_pool.h"

#include <atomic>
#include <array>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

// TODO(babenko): extract
typedef i64 TRcuTreeTimestamp;

template <class TKey, class TComparer>
class TRcuTree;

////////////////////////////////////////////////////////////////////////////////

// TODO(babenko): move impl to inl

template <class TKey, class TComparer>
class TRcuTree
{
public:
    TRcuTree(
        TChunkedMemoryPool* pool,
        const TComparer* comparer);

    int Size() const;

    template <class TPivot, class TNewKeyProvider, class TExistingKeyAcceptor>
    void Insert(
        TPivot pivot,
        TNewKeyProvider newKeyProvider,
        TExistingKeyAcceptor existingKeyAcceptor);

    bool Insert(TKey key);

    class TReader;
    TReader* CreateReader();

private:
    struct TNode;

    TChunkedMemoryPool* Pool_;
    const TComparer* Comparer_;
    
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
        TNode* GCNext;
        TNode* FreeNext;
    };
    TKey Key;
    TNode* Left;
    TNode* Right;
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

    const TComparer* Comparer_; // to avoid indirection on Tree_
    std::atomic<TRcuTreeTimestamp> Timestamp_;

    // TNode* + (1 if this is a left child)
    // 128 should be enough for every balanced tree
    std::array<intptr_t, 128> Stack_;
    intptr_t* StackTop_;
    intptr_t* StackBottom_;


    explicit TReader(TRcuTree* tree)
        : Tree_(tree)
        , Comparer_(Tree_->Comparer_)
        , Timestamp_(InactiveReaderTimestamp)
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
        auto token = reinterpret_cast<intptr_t>(node);
        YASSERT((token & 1) == 0);
        return token | 1;
    }

    static intptr_t RightChildToToken(TNode* node)
    {
        auto token = reinterpret_cast<intptr_t>(node);
        YASSERT((token & 1) == 0);
        return token;
    }

    static bool IsLeftChild(intptr_t token)
    {
        return (token & 1) != 0;
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

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

#define RCU_TREE_INL_H_
#include "rcu_tree-inl.h"
#undef RCU_TREE_INL_H_
