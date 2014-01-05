#pragma once

#include "common.h"
#include "chunked_memory_pool.h"

#include <atomic>
#include <array>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

typedef i64 TRcuTreeTimestamp;

template <class TKey, class TComparer>
class TRcuTree;

template <class TKey, class TComparer>
class TRcuTreeScanner;

template <class TKey, class TComparer>
class TRcuTreeScannerPtr;

////////////////////////////////////////////////////////////////////////////////

//! An insert-only red-black tree featuring Read-Copy-Update technique.
/*!
 *  All public methods (including ctor and dtor) must be called from a single (writer) thread.
 *  All public methods of scanner instances (returned by #AllocateScanner)
 *  can be called from arbitrary (reader) threads.
 */
template <class TKey, class TComparer>
class TRcuTree
{
public:
    typedef TRcuTreeScanner<TKey, TComparer> TScanner;

    TRcuTree(
        TChunkedMemoryPool* pool,
        const TComparer* comparer);
    ~TRcuTree();

    int Size() const;

    template <class TPivot, class TNewKeyProvider, class TExistingKeyConsumer>
    void Insert(
        TPivot pivot,
        TNewKeyProvider newKeyProvider,
        TExistingKeyConsumer existingKeyConsumer);

    bool Insert(TKey key);

    TScanner* AllocateScanner();
    void FreeScanner(TScanner* scanner);

private:
    friend class TRcuTreeScanner<TKey, TComparer>;
    struct TNode;

    TChunkedMemoryPool* Pool_;
    const TComparer* Comparer_;
    
    int Size_;
    TNode* Root_;
    std::atomic<TRcuTreeTimestamp> Timestamp_;

    TScanner* FirstActiveScanner_;    // the head of a linked list of active (allocated) scanners
    TScanner* FirstInactiveScanner_;  // the head of a linked list of inactive (freed) scanners

    static const int MaxPooledScanners = 256;
    int InactiveScannerCount_;        // number of inactive (freed) scanners

    int GCNodeCount_;       // number of nodes in GC queue
    TNode* FirstGCNode_;    // earliest GC node
    TNode* LastGCNode_;     // latest GC node
    TNode* FirstFreeNode_;  // first free node


    static const int MaxGCSize = 1024;
    static const TRcuTreeTimestamp InactiveReaderTimestamp = 0x7fffffffffffffff; // std::numeric_limits<TRcuTreeTimestamp>::max();

    TRcuTreeTimestamp GenerateWriterTimestamp();
    TRcuTreeTimestamp ComputeEarliestReaderTimestamp();

    TNode* AllocateNode();
    void FreeNode(TNode* node);
    void MaybeGCCollect();

    void Rebalance(TNode* x);
    void ReplaceChild(TNode* x, TNode* y);

    void DestroyScannerList(TScanner* first);

};

template <class TKey, class TComparer>
class TRcuTreeScanner
{
private:
    typedef TRcuTree<TKey, TComparer> TTree;
    friend class TRcuTree<TKey, TComparer>;

    typedef TRcuTreeScanner<TKey, TComparer> TScanner;

    typedef typename TTree::TNode TNode;

public:
    template <class TPivot>
    bool Find(TPivot pivot, TKey* key = nullptr);


    template <class TPivot>
    void BeginScan(TPivot pivot);

    bool IsValid() const;

    TKey GetCurrent() const;

    void Advance();

    void EndScan();

private:
    TTree* Tree_;

    TScanner* NextScanner_; // next scanner in the corresponding linked list
    TScanner* PrevScanner_; // next scanner in the corresponding linked list (only used for active ones)

    const TComparer* Comparer_; // to avoid indirection on Tree_
    std::atomic<TRcuTreeTimestamp> Timestamp_;

    // TNode* + (1 if this is a left child)
    // 128 should be enough for every balanced tree
    std::array<intptr_t, 128> Stack_;
    intptr_t* StackTop_;
    intptr_t* StackBottom_;


    explicit TRcuTreeScanner(TTree* tree);

    void Acquire();
    void Release();
    void Reset();

    static TNode* TokenToChild(intptr_t token);
    static intptr_t LeftChildToToken(TNode* node);
    static intptr_t RightChildToToken(TNode* node);
    static bool IsLeftChild(intptr_t token);

    void Push(intptr_t value);
    intptr_t Peek() const;
    void Pop();

};

////////////////////////////////////////////////////////////////////////////////

template <class TKey, class TComparer>
class TRcuTreeScannerPtr
{
public:
    explicit TRcuTreeScannerPtr(TRcuTree<TKey, TComparer>* tree);
    ~TRcuTreeScannerPtr();

    TRcuTreeScanner<TKey, TComparer>* operator -> ();

private:
    TRcuTree<TKey, TComparer>* Tree_;
    TRcuTreeScanner<TKey, TComparer>* Scanner_;

};

template <class TKey, class TComparer>
TRcuTreeScannerPtr<TKey, TComparer>::TRcuTreeScannerPtr(TRcuTree<TKey, TComparer>* tree)
    : Tree_(tree)
    , Scanner_(Tree_->AllocateScanner())
{ }

template <class TKey, class TComparer>
TRcuTreeScannerPtr<TKey, TComparer>::~TRcuTreeScannerPtr()
{
    Tree_->FreeScanner(Scanner_);
}

template <class TKey, class TComparer>
TRcuTreeScanner<TKey, TComparer>* TRcuTreeScannerPtr<TKey, TComparer>::operator->()
{
    return Scanner_;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

#define RCU_TREE_INL_H_
#include "rcu_tree-inl.h"
#undef RCU_TREE_INL_H_
