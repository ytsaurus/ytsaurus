#pragma once

#include "cow_tree_detail/cow_tree_config.h"
#include "cow_tree_detail/cow_tree_core.h"
#include "cow_tree_detail/cow_tree_iterator.h"
#include "cow_tree_detail/cow_tree_node.h"
#include "cow_tree_detail/cow_tree_search.h"
#include "cow_tree_detail/cow_tree_value.h"

#include <util/system/compiler.h>
#include <util/system/types.h>

#include <concepts>
#include <functional>
#include <initializer_list>
#include <iterator>
#include <stdexcept>
#include <type_traits>
#include <utility>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

//! Read-only interface shared by #TCowTree and #TCowTreeSnapshot.
//!
//! Keys and mapped values must be nothrow move-constructible. An exception from a node
//! allocation or deletion hook or a stored slot constructor aborts the process with a diagnostic.
//!
//! A master has a stable address and requires external synchronization, including
//! for lookups. A snapshot is an immutable, O(1) copy that can outlive its master.
//! The default configuration permits independent snapshot readers on other threads.
//! With #TCowTreeStConfig all views belong to one thread.
//!
//! Iterators remain valid until their owning view dies. A master iterator follows
//! its element through modifications and advances to the successor on erasure.
//! Erasing by key uses the search path to advance live iterators without further key comparisons.
//! References into the master expire on mutation. Taking a snapshot also invalidates
//! writable references. Requesting writable access to a mapped value counts as a
//! mutation. A map iterator yields a proxy with references to the key and value.
//!
//! Lookup and update take O(log n) for fixed node capacity, plus the cost of repairing
//! live master iterators in affected leaves. #ForEach() and #BuildFromSorted() are
//! linear. A full iterator scan costs O(n + n / MaxNodeSize * log n).
template <CCowTreeConfig TConfig>
class TCowTreeBase
{
public:
    using TKey = typename TConfig::TKey;
    using TValue = typename TConfig::TValue;
    using TLess = typename TConfig::TLess;

    using TNode = NCowTreeInternals::TNode;
    using TLeaf = NCowTreeInternals::TLeaf<TConfig>;
    using TInner = NCowTreeInternals::TInner<TConfig>;
    using TKeyHolder = NCowTreeInternals::TValueHolder<TKey>;
    using TSearch = NCowTreeInternals::TCowTreeSearch<TConfig>;
    using TPath = NCowTreeInternals::TCowTreePath<TConfig>;

    template <class TAnyNode>
    using TPathItem = NCowTreeInternals::TPathItem<TAnyNode>;

    struct TNodeOps
    {
        static void Ref(TNode* node);
        static void Unref(TNode* node);
    };

    using TNodeRef = NCowTreeInternals::TRef<TNode, TNodeOps>;

    using TCore = NCowTreeInternals::TCowTreeCore<TConfig>;

    struct TCoreOps
    {
        static void Ref(TCore* core);
        static void Unref(TCore* core);
    };

    using TCoreRef = NCowTreeInternals::TRef<TCore, TCoreOps>;

    friend class TCowTreeIteratorBase<TConfig>;
    friend class TCowTreeIterator<TConfig, false>;
    friend class TCowTreeIterator<TConfig, true>;

    //! The untyped iterator engine; the public API deals in the two flavours below.
    using TIterator = TCowTreeIteratorBase<TConfig>;

    using iterator = TCowTreeIterator<TConfig, false>;
    using const_iterator = TCowTreeIterator<TConfig, true>;
    using reverse_iterator = std::reverse_iterator<iterator>;
    using const_reverse_iterator = std::reverse_iterator<const_iterator>;
    using value_type = typename const_iterator::value_type;
    using key_type = TKey;
    using mapped_type = TValue;
    using size_type = size_t;

    using TSnapshot = TCowTreeSnapshot<TConfig>;

    //! Distinguishes a map (mapped values inline in the leaves) from a set.
    static constexpr bool HasValue = !std::is_void_v<TValue>;
    static constexpr int MaxNodeSize = TConfig::MaxNodeSize;
    static constexpr int LinearSearchLimit = TConfig::LinearSearchLimit;
    static constexpr ECowTreeSearch ReadSearch = CowTreeReadSearch<TKey, TLess>();
    static constexpr ECowTreeSearch WriteSearch = CowTreeWriteSearch<TKey, TLess>();
    static_assert(MaxNodeSize >= 4 && MaxNodeSize % 2 == 0);

    //! MaxSize is a hard contract, not a hint: it bounds the height of the
    //! fixed descent stack, and the root split verifies the budget in release.
    static constexpr int MaxHeight = ComputeCowTreeMaxHeight(MaxNodeSize, TConfig::MaxSize);
    static_assert(NCowTreeInternals::TCowTreePath<TConfig>::MaxHeight == MaxHeight);

    bool IsMaster() const;

    //! The master's modification counter, which grows with every change; a snapshot
    //! reports the value it was taken at, so comparing the two tells whether the
    //! snapshot still matches the master.
    ui64 GetVersion() const;

    //! The const lookups: the const_iterators they return read the tree and
    //! revalidate their own bookkeeping, but never change the elements (a
    //! master's const_iterator still registers itself, like any of its iterators).
    const_iterator begin() const;
    const_iterator end() const;
    const_iterator cbegin() const;
    const_iterator cend() const;

    //! Convenience only: every reverse step copies an iterator (a registration
    //! for the master's ones) — hot backward scans should use -- directly.
    const_reverse_iterator rbegin() const;
    const_reverse_iterator rend() const;

    size_t size() const;
    bool empty() const;

    const_iterator find(const TKey& key) const;

    //! The first element >= |key| / > |key|, end() if there is none.
    const_iterator lower_bound(const TKey& key) const;
    const_iterator upper_bound(const TKey& key) const;

    //! A lookup that builds no iterator (and takes no node reference).
    bool contains(const TKey& key) const;
    size_t count(const TKey& key) const;

    // std-style heterogeneous lookup, enabled when the comparator declares
    // is_transparent; the mutations stay keyed by TKey.

    template <class TAnyKey>
    const_iterator find(const TAnyKey& key) const
        requires CCowTreeTransparentLess<TLess>;

    template <class TAnyKey>
    const_iterator lower_bound(const TAnyKey& key) const
        requires CCowTreeTransparentLess<TLess>;

    template <class TAnyKey>
    const_iterator upper_bound(const TAnyKey& key) const
        requires CCowTreeTransparentLess<TLess>;

    template <class TAnyKey>
    bool contains(const TAnyKey& key) const
        requires CCowTreeTransparentLess<TLess>;

    template <class TAnyKey>
    size_t count(const TAnyKey& key) const
        requires CCowTreeTransparentLess<TLess>;

    // The dependent TResult below keeps void-returning declarations out of set
    // instantiations; it always equals TValue.

    //! Returns a reference to the value of |key|. Throws std::out_of_range if absent.
    template <class TResult = TValue>
    const TResult& at(const TKey& key) const
        requires HasValue;

    //! Visits every element in order with no per-step iterator bookkeeping — the
    //! fastest way to read the whole tree: visitor(key) for a set, visitor(key, value)
    //! for a map. The tree must not be mutated from the visitor.
    template <class TVisitor>
    void ForEach(TVisitor&& visitor) const;

    //! ForEach over the half-open key range [from, to): the visitor sees every
    //! element with from <= key < to, in order, at O(answer + log n).
    template <class TVisitor>
    void ForEachInRange(const TKey& from, const TKey& to, TVisitor&& visitor) const;

    TLess key_comp() const;

    //! For a set the value comparator is the key one; a map compares by first.
    auto value_comp() const;

    //! Element-wise equality; requires == on the keys (and mapped values).
    //! Compares masters and snapshots with each other freely; trees sharing
    //! their root (a snapshot and its unmutated master, two snapshots of the
    //! same version) compare equal in O(1).
    bool operator==(const TCowTreeBase& other) const;

protected:
    struct TSnapshotTag
    { };

    TCoreRef Core_;
    mutable NCowTreeInternals::TViewLink ViewLink_;
    TKeyHolder MaxKeyHolder_;
    TNodeRef Root_;
    size_t Size_ = 0;
    int Height_ = 0;
    [[no_unique_address]] TLess Less_;

    explicit TCowTreeBase(const TLess& less);
    TCowTreeBase(const TCowTreeBase&) = delete;
    TCowTreeBase& operator=(const TCowTreeBase&) = delete;

    //! Shares |source|'s state as a read-only snapshot view at |snapshotVersion|.
    TCowTreeBase(TSnapshotTag, const TCowTreeBase& source, ui64 snapshotVersion);

    ~TCowTreeBase();

    TIterator MakeIterator(TLeaf* leaf, int pos, TInner* parent, int parentPos);
    TIterator SeekFirst();
    TIterator SeekLast();

    template <class TAnyKey>
    TIterator SeekFirstGreaterOrEqual(const TAnyKey& key);
    template <class TAnyKey>
    TIterator SeekFirstGreater(const TAnyKey& key);

    //! The last element < |key|, end() if there is none. The predecessor is either to
    //! the left within the reached leaf, or it is the deepest on-path separator < key
    //! (an exact subtree max, so a second descent by it lands right on the element).
    template <class TAnyKey>
    TIterator SeekLastLess(const TAnyKey& key);

    template <class TAnyKey>
    TIterator DoFind(const TAnyKey& key) const;
    template <class TAnyKey>
    bool DoContains(const TAnyKey& key) const;

    //! The first stored key >= |key| by a plain read descent, nullptr if none.
    const TKey* FindKeyGreaterOrEqual(const TKey& key) const;

    template <class TResult = TValue>
    const TResult* TryReadValue(const TKey& key) const
        requires HasValue;

    template <class TResult = TValue>
    const TResult& ReadValue(const TKey& key) const
        requires HasValue;

    template <class TVisitor>
    static void ForEachNode(TNode* node, int moreLevels, TVisitor& visitor);

    template <class TVisitor>
    void ForEachNodeInRange(TNode* node, int moreLevels, const TKey& from, const TKey& to, TVisitor& visitor) const;

    //! The debug-build half of the "a master's iterator must not outlive its
    //! tree" contract: at master destruction every leaf's tracked list must be
    //! empty.
    static void AssertNoTrackedIterators(TNode* node, int moreLevels);

    bool KeysEqual(const TKey& lhs, const TKey& rhs) const;
};

////////////////////////////////////////////////////////////////////////////////

//! The master: the single mutable owner of the tree's state. See TCowTreeBase
//! above for the complete contract; every read there is inherited, the const
//! flavours next to the mutable overloads below.
template <CCowTreeConfig TConfig>
class TCowTree
    : public TCowTreeBase<TConfig>
{
public:
    using TBase = TCowTreeBase<TConfig>;

    using TKey = typename TConfig::TKey;
    using TValue = typename TConfig::TValue;
    using TLess = typename TConfig::TLess;

    using TNodeOps = typename TBase::TNodeOps;
    using TCoreOps = typename TBase::TCoreOps;
    using TNode = typename TBase::TNode;
    using TLeaf = typename TBase::TLeaf;
    using TInner = typename TBase::TInner;
    using TKeyHolder = typename TBase::TKeyHolder;
    using TSearch = typename TBase::TSearch;
    using TPath = typename TBase::TPath;
    using TNodeRef = typename TBase::TNodeRef;
    using TCore = typename TBase::TCore;
    using TCoreRef = typename TBase::TCoreRef;
    using TIterator = typename TBase::TIterator;

    template <class TAnyNode>
    using TPathItem = NCowTreeInternals::TPathItem<TAnyNode>;

    using iterator = typename TBase::iterator;
    using const_iterator = typename TBase::const_iterator;
    using reverse_iterator = typename TBase::reverse_iterator;
    using const_reverse_iterator = typename TBase::const_reverse_iterator;
    using value_type = typename TBase::value_type;
    using key_type = typename TBase::key_type;
    using mapped_type = typename TBase::mapped_type;
    using size_type = typename TBase::size_type;
    using TSnapshot = TCowTreeSnapshot<TConfig>;

    static constexpr bool HasValue = !std::is_void_v<TValue>;
    static constexpr int MaxNodeSize = TConfig::MaxNodeSize;
    static constexpr int MaxHeight = TBase::MaxHeight;

    TCowTree();

    //! A stateful comparator — a function pointer, a functor carrying data — comes
    //! in here, like std::set(const Compare&); the snapshots inherit it.
    explicit TCowTree(const TLess& less);

    TCowTree(std::initializer_list<value_type> elements, const TLess& less = TLess());

    //! The master is unique: copying or moving it is forbidden — hand out
    //! snapshots (or wrap the master in a smart pointer). A move would also
    //! silently break live iterators holding the old tree pointer.
    TCowTree(const TCowTree&) = delete;
    TCowTree(TCowTree&&) = delete;
    TCowTree& operator=(const TCowTree&) = delete;
    TCowTree& operator=(TCowTree&&) = delete;

    ~TCowTree() = default;

    //! Returns a read-only view of the current state in O(1). The snapshot shares the
    //! nodes, may be copied, read and destroyed independently of the master; further
    //! master mutations copy shared nodes on write and never disturb the snapshot.
    TSnapshot Snapshot() const;

    // The const lookup flavours, inherited next to the mutable ones below.
    using TBase::at;
    using TBase::begin;
    using TBase::contains;
    using TBase::count;
    using TBase::end;
    using TBase::find;
    using TBase::IsMaster;
    using TBase::lower_bound;
    using TBase::rbegin;
    using TBase::rend;
    using TBase::upper_bound;

    //! Mutable map iterators provide writable access to mapped values.
    iterator begin();
    iterator end();
    reverse_iterator rbegin();
    reverse_iterator rend();

    iterator find(const TKey& key);
    iterator lower_bound(const TKey& key);
    iterator upper_bound(const TKey& key);

    template <class TAnyKey>
    iterator find(const TAnyKey& key)
        requires CCowTreeTransparentLess<TLess>;

    template <class TAnyKey>
    iterator lower_bound(const TAnyKey& key)
        requires CCowTreeTransparentLess<TLess>;

    template <class TAnyKey>
    iterator upper_bound(const TAnyKey& key)
        requires CCowTreeTransparentLess<TLess>;

    // The dependent TResult below keeps void-returning declarations out of set
    // instantiations; it always equals TValue.

    //! Returns a writable reference to the value of |key|. Throws std::out_of_range if absent.
    template <class TResult = TValue>
    TResult& at(const TKey& key)
        requires HasValue;

    //! The std::map contract: inserts a missing |key| with a default-constructed
    //! value and returns a plain mutable reference. The access counts as a
    //! modification (copies the leaf shared with a snapshot).
    template <class TResult = TValue>
    TResult& operator[](const TKey& key)
        requires HasValue;

    //! Mutable access to an existing value; the explicitly named form of
    //! non-const at().
    template <class TResult = TValue>
    TResult& MutableAt(const TKey& key)
        requires HasValue;

    //! Replaces the value of an existing |key| under copy-on-write.
    template <std::convertible_to<TValue> TValueRef>
    void AssignValue(const TKey& key, TValueRef&& value)
        requires HasValue;

    //! Replaces the value of |key|, inserting the key if it is missing.
    template <std::convertible_to<TValue> TValueRef>
    void AssignOrInsert(const TKey& key, TValueRef&& value)
        requires HasValue;

    //! Mutable access to the value of |key|, default-constructing it if missing.
    template <class TResult = TValue>
    TResult& MutableOrInsert(const TKey& key)
        requires HasValue;

    //! The std shapes over AssignOrInsert and friends: same single descent,
    //! plus the returned iterator (which AssignOrInsert does not even register).
    template <std::convertible_to<TValue> TValueRef>
    std::pair<iterator, bool> insert_or_assign(const TKey& key, TValueRef&& value)
        requires HasValue;

    //! Does nothing when |key| is present; otherwise inserts it with a value
    //! constructed from |args|.
    template <class... TArgs>
    std::pair<iterator, bool> try_emplace(const TKey& key, TArgs&&... args)
        requires HasValue;

    //! Returns the registered iterator and whether insertion took place.
    std::pair<iterator, bool> insert(const TKey& key)
        requires(!HasValue);

    template <std::convertible_to<TValue> TValueRef>
    std::pair<iterator, bool> insert(const TKey& key, TValueRef&& value)
        requires HasValue;

    std::pair<iterator, bool> insert(const value_type& keyValue)
        requires HasValue;

    std::pair<iterator, bool> insert(value_type&& keyValue)
        requires HasValue;

    //! The fast inserts: no iterator is built, the return says whether the key
    //! was actually inserted.
    bool Insert(const TKey& key)
        requires(!HasValue);

    template <std::convertible_to<TValue> TValueRef>
    bool Insert(const TKey& key, TValueRef&& value)
        requires HasValue;

    //! Constructs the key from |args| and inserts it. The tree stores keys by
    //! value in its leaves, so this is exactly insert(TKey(args...)) — sugar,
    //! not a saved copy.
    template <class... TArgs>
    std::pair<iterator, bool> emplace(TArgs&&... args)
        requires(!HasValue);

    //! Constructs the key and the value and inserts them.
    template <class TKeyArg, class TValueArg>
    std::pair<iterator, bool> emplace(TKeyArg&& key, TValueArg&& value)
        requires HasValue;

    //! Inserts |key| near the |position| hint and leaves |position| on the
    //! inserted (or the preexisting equal) element. With a good hint — the key
    //! falls strictly inside |position|'s leaf — the insert is one in-leaf shift
    //! with no root descent, the mirror of EraseAdvance; a boundary or wrong
    //! hint falls back to a regular insertion.
    void InsertHint(TIterator& position, const TKey& key)
        requires(!HasValue);

    template <std::convertible_to<TValue> TValueRef>
    void InsertHint(TIterator& position, const TKey& key, TValueRef&& value)
        requires HasValue;

    //! The std-shaped hint inserts: the returned iterator denotes the element.
    iterator insert(TIterator hint, const TKey& key)
        requires(!HasValue);

    iterator insert(TIterator hint, const value_type& keyValue)
        requires HasValue;

    iterator insert(TIterator hint, value_type&& keyValue)
        requires HasValue;

    //! The std emplace_hint — insert(hint, TKey(args...)), sugar like emplace.
    template <class... TArgs>
    iterator emplace_hint(TIterator hint, TArgs&&... args)
        requires(!HasValue);

    template <class TKeyArg, class TValueArg>
    iterator emplace_hint(TIterator hint, TKeyArg&& key, TValueArg&& value)
        requires HasValue;

    //! The number of erased elements, std style: 1 or 0.
    size_t erase(const TKey& key);

    //! Erases the element the iterator denotes and returns the successor, as in
    //! std. Amortized O(1): in the frequent case — the leaf is not shared with a
    //! snapshot, does not underflow and keeps its last element — the erase is one
    //! in-leaf shift and the returned iterator is placed with no root descent
    //! either. The std shape passes iterators around by value, and each hand-over
    //! registers and unregisters one in its leaf: prefer the two by-reference
    //! flavours below in code of your own — they are about twice as fast.
    iterator erase(TIterator position);

    //! Erases the element |position| denotes and leaves |position| on the
    //! successor (end() after the last element) — the sweep-and-erase flavour:
    //!
    //!     while (it != tree.end()) {
    //!         if (Drop(*it)) { tree.EraseAdvance(it); } else { ++it; }
    //!     }
    void EraseAdvance(TIterator& position);

    //! Erases the element |position| denotes and leaves |position| equal to
    //! end() — the flavour for a caller that is done with the iterator, and the
    //! cheapest of the three: the iterator leaves the tree's bookkeeping before
    //! the erase, so nothing repairs it afterwards.
    void EraseAt(TIterator& position);

    //! Takes the element |position| denotes out of the tree: the key is copied
    //! (a map's value is moved) and the element is erased, with |position| left
    //! equal to end() — the extracting twin of EraseAt.
    TKey ExtractAt(TIterator& position)
        requires(!HasValue);

    value_type ExtractAt(TIterator& position)
        requires HasValue;

    //! The extracting twin of EraseAdvance: |position| is left on the successor.
    TKey ExtractAdvance(TIterator& position)
        requires(!HasValue);

    value_type ExtractAdvance(TIterator& position)
        requires HasValue;

    void clear();

    //! Inserts every element of the range / list; duplicates are skipped.
    template <std::input_iterator TInputIt>
    void insert(TInputIt first, TInputIt last);

    void insert(std::initializer_list<value_type> elements);

    //! Bulk-loads a strictly increasing (by the tree's comparator) range into
    //! an EMPTY master in O(n): the tree is built bottom-up with fully packed
    //! nodes — the densest layout at one slot per element (the last two nodes
    //! of every level split their remainder, so nothing ends up underfull).
    //! The order is verified as the elements stream in. For a map the elements
    //! are pair-likes — another tree's iterators work, so rebuilding a master
    //! from a snapshot is exactly BuildFromSorted(snapshot.begin(), .end()).
    //! Mutations afterwards are fully supported (the first insert into a packed
    //! leaf splits it, as with any full leaf).
    template <std::input_iterator TInputIt>
    void BuildFromSorted(TInputIt first, TInputIt last);

    void SelfCheck();

private:
    friend class TCowTreeIteratorBase<TConfig>;
    friend class TCowTreeIterator<TConfig, false>;

    using TBase::Core_;
    using TBase::Height_;
    using TBase::Less_;
    using TBase::MaxKeyHolder_;
    using TBase::Root_;
    using TBase::Size_;
    using TBase::ViewLink_;

    using typename TBase::TSnapshotTag;

    using TBase::DoContains;
    using TBase::DoFind;
    using TBase::FindKeyGreaterOrEqual;
    using TBase::KeysEqual;
    using TBase::MakeIterator;
    using TBase::ReadValue;
    using TBase::SeekFirst;
    using TBase::SeekFirstGreater;
    using TBase::SeekFirstGreaterOrEqual;
    using TBase::SeekLast;
    using TBase::SeekLastLess;
    using TBase::TryReadValue;

    // In-place repairs of the master's live iterators. Each leaf carries the
    // intrusive list of the iterators currently parked on it; every mutation fixes
    // exactly the iterators of the leaves it touches, so an iterator is always
    // valid and mutations elsewhere cost the iterators nothing.

    static TIterator* LeafIterators(TLeaf* leaf);

    //! In-leaf element shifts: iterators follow their element.
    static void FixLeafInsert(TLeaf* leaf, int insertPos);
    static void FixLeafErase(TLeaf* leaf, int erasePos);

    //! Repoints iterators from |from| to |to|, accounting for the insertion at |insertPos| or erasure at |skipPos|.
    static void MoveLeafIterators(TLeaf* from, TLeaf* to, int insertPos = -1, int skipPos = -1);

    TIterator* DetachIteratorsAt(TLeaf* leaf, int pos);
    void AdvanceLastLeafIterators(const TPath& path);

    //! Pulls the tracked iterators of a leaf sibling group into a detached chain,
    //! encoding each iterator's position within the group's concatenated element
    //! stream in Pos_; positions >= |shiftFrom| get |shiftBy| (the pending insert
    //! gap or erased element). DistributeGroupIterators reseats the chain over the
    //! group's post-operation layout by the same stream arithmetic.
    static TIterator* CollectGroupIterators(TNode* const* group, int groupSize, int shiftFrom, int shiftBy);
    static void DistributeGroupIterators(TIterator* chain, TNode* const* group, int groupSize);

    //! Repairs the parent hints after an operation that changed which inner node
    //! owns which leaf: walks |parent|'s (leaf) children and repoints their
    //! iterators' hints. SetLeafParent is the single-leaf flavour (root changes).
    static void ReparentLeafChildren(TInner* parent);
    static void SetLeafParent(TLeaf* leaf, TInner* parent, int parentPos);

    //! Turns every iterator of the subtree's leaves into end() — the mass-eviction
    //! repair of clear().
    static void EvictLeafIterators(TNode* node, int moreLevels);

    //! The writable value slot behind a mutable map iterator's dereference: the
    //! in-place slot when the leaf is already exclusive, a full copy-on-write
    //! access otherwise (which repoints the registered iterator to the copy).
    template <class TResult = TValue>
    TResult& MutableValueFor(TIterator& position)
        requires HasValue;

    //! The value slot of |key| for writing, nullptr if the key is missing. Copies the
    //! leaf if it is shared; a successful call counts as a modification. The slot is
    //! writable until the next Snapshot() or structural change of the tree.
    NCowTreeInternals::TValueHolder<TValue>* TryMutableValueHolder(const TKey& key)
        requires HasValue;

    NCowTreeInternals::TValueHolder<TValue>& MutableValueHolder(const TKey& key)
        requires HasValue;

    //! The descent for a key expected present (an erase, a value access): a
    //! three-way comparator may end it at a separator hit.
    TPath FindPath(const TKey& key);
    //! The descent for a key expected absent (an insert): plain two-way halving.
    TPath FindInsertPath(const TKey& key);
    TPath FindRightmostPath();

    template <class TTuple>
    bool InsertFirst(const TKey& key, TTuple&& valueTuple);

    TKeyHolder* ResolveMaxSlot(TPath& path);

    //! Runs |action| on the sibling group of a complex multi-node operation around
    //! the parent's current child: two or three adjacent children plus the parent
    //! separators between them. Advances the parent position to where the cascading
    //! pair lands, one slot further for an insert (a new child appears) than for an
    //! erase.
    template <int PosShift, class TAction>
    static decltype(auto) DispatchSiblingGroup(TPathItem<TInner>& parent, TAction&& action);

    // Copy-on-write machinery. A node is shared (visible through some snapshot) iff its
    // version is not above the newest live snapshot's version. Exclusivity is closed
    // upwards: installing a node stamps its parent with the same version, and an in-place
    // write requires the node to be exclusive already — so a shared node never has an
    // exclusive descendant, and the shared part of any root-to-leaf path is a suffix.
    // Hence a single leaf version check gives the no-snapshot fast path for free.

    //! A private copy of |source| with re-referenced children.
    TInner* CopyInnerNode(TInner* source, ui64 version);

    static void AppendLeafRange(TLeaf* copy, TLeaf* source, int from, int to);

    //! A private copy of |source| with the optional |skipPos| omitted.
    TLeaf* CopyLeafNode(TLeaf* source, ui64 version, int skipPos = -1);

    //! Installs |replacement| into the (exclusive) parent slot currently holding the
    //! path node at |level|; the displaced node lives on in snapshots.
    void ReplaceInParent(TPath& path, int level, TNode* oldNode, TNode* replacement, ui64 version);

    //! Replaces every shared member of a sibling group with a private copy; the group
    //! slots live in an already exclusive parent. Path members are exclusive by now.
    template <class TAnyNode>
    void NormalizeGroup(TNode** groupChildren, int count, ui64 version, ui64 maxSnapshotVersion);

    //! Copies the shared part of the path — always its leaf-side suffix — so that all
    //! inner nodes on the path are exclusive before any of them is written. Walks from
    //! the root so that each copy is installed into an already exclusive parent.
    void EnsureExclusivePath(TPath& path, ui64 version, ui64 maxSnapshotVersion);

    // The write descent, split so that every writing operation runs exactly one
    // of it and shares only what it genuinely has in common.

    //! Locates |key|'s path in a non-empty tree. One whole-tree append check
    //! replaces the per-node ones: a key above the tree max cannot be a
    //! duplicate and descends the rightmost path with no comparisons at all.
    TPath FindWritePath(const TKey& key, bool& newGlobalMax);

    //! Whether a located |path| landed exactly on |key|.
    bool PathHitsKey(const TPath& path) const;

    //! An iterator on the element a located |path| denotes.
    TIterator MakePathIterator(const TPath& path);

    //! The value slot of the element a located |path| denotes, made writable:
    //! copies the leaf (and the shared part of the path above it) if it is
    //! shared with a snapshot, and repoints |path| at the copy.
    NCowTreeInternals::TValueHolder<TValue>* MutableValueAtPath(TPath& path)
        requires HasValue;

    //! Inserts at |path| and sets |position| to the new element if |CollectPosition| is true.
    template <bool CollectPosition, class TTuple>
    Y_FORCE_INLINE void InsertAtPath(TPath& path, const TKey& key, TTuple&& valueTuple, bool newGlobalMax, TIterator* position);

    template <bool CollectPosition = false, class TTuple>
    bool DoInsert(const TKey& key, TTuple&& valueTuple, TIterator* position = nullptr);

    //! The insert half of MutableOrInsert, kept out of line so that its common
    //! (key already present) path stays a short function.
    template <class TResult = TValue>
    Y_NO_INLINE TResult& InsertDefaultAtPath(TPath& path, const TKey& key, bool newGlobalMax)
        requires HasValue;

    //! The shared body of AssignOrInsert and insert_or_assign: one descent that
    //! either overwrites the value of an existing |key| or inserts it.
    template <bool CollectPosition, class TValueRef>
    bool DoAssignOrInsert(const TKey& key, TValueRef&& value, TIterator* position)
        requires HasValue;

    template <class TTuple>
    void DoInsertHint(TIterator& position, const TKey& key, TTuple&& valueTuple);

    bool EraseLast();

    //! The shared body of the iterator erases: erases the element at |pos| of
    //! |leaf| in place when the leaf allows it, using its ancestor path for a cascade. |parentPos| is a hint that may have drifted.
    void EraseLeafSlot(TLeaf* leaf, int pos, TInner* parent, int parentPos);

    //! The underfull half of the iterator erases: runs the sibling group operation
    //! over the parent's children — reachable through the iterator hint alone —
    //! and absorbs a shrink cascade with a simple erase in the parent. Callable
    //! only when the parent can take that erase (see the call site).
    void EraseLeafGroup(int pos, TInner* parent, int leafSlot, int separatorSlot);

    //! Reseats iterators taken off an erased leaf max onto the successor: the
    //! first element of the parent's next child, which the hint reaches with no
    //! descent of its own.
    static void ReseatStrandToNextLeaf(TIterator* strand, TInner* parent, int slot);

    //! The leaf's current slot in |parent|, refound if the |hint| has drifted.
    static int RepairLeafSlot(TInner* parent, TLeaf* leaf, int hint);

    //! The relaxed half of the leaf-level underfill policy of erase, consulted
    //! when the leaf is at or under the two-thirds bar: between half and two
    //! thirds of a node a simple erase is still fine as long as the sibling group
    //! holds two thirds per node ON AVERAGE (for a three-node group: the sum
    //! exceeds 2 * MaxNodeSize). The group's collective
    //! weight upholds the memory contract of at most 1.5 slots per record on
    //! average, and the repack is deferred until the neighborhood actually thins
    //! out; inner nodes keep the strict per-node bar — they are the ones that
    //! bound the search depth.
    bool LeafEraseKeepsFill(TLeaf* leaf, TInner* parent, int parentPos) const;

    //! Erasing a leaf's max leaves its one extra copy (the MaxSlot separator) stale;
    //! rewrite it with the new leaf max — unless the erase's own separator range
    //! covered the slot: then the machinery or the pending pair-erase repairs it.
    void RepairMaxSeparator(TPath& path, TLeaf* lastLeaf, TInner* separatorsNode, int separatorsBegin, int separatorsEnd, ui64 version);

    //! Verifies the subtree and returns its max (the last key of its rightmost leaf).
    const TKeyHolder* SelfCheckNode(TNode* node, size_t moreLevels, size_t& calculatedSize, size_t& leafCount, size_t& separatorCount);
};

////////////////////////////////////////////////////////////////////////////////

//! A read-only view of a master's state at the moment of its creation, produced
//! by TCowTree::Snapshot() in O(1). None of the mutating API exists on this type;
//! see the contract atop TCowTreeBase. Snapshots are copyable in O(1), may
//! outlive the master, and with the thread-safe config may be read concurrently.
template <CCowTreeConfig TConfig>
class TCowTreeSnapshot
    : public TCowTreeBase<TConfig>
{
public:
    using TBase = TCowTreeBase<TConfig>;

    //! Copying a snapshot yields another view of the same version in O(1).
    TCowTreeSnapshot(const TCowTreeSnapshot& other);

    TCowTreeSnapshot(TCowTreeSnapshot&&) = delete;
    TCowTreeSnapshot& operator=(const TCowTreeSnapshot&) = delete;
    TCowTreeSnapshot& operator=(TCowTreeSnapshot&&) = delete;

    ~TCowTreeSnapshot() = default;

private:
    friend class TCowTree<TConfig>;

    using typename TBase::TSnapshotTag;

    //! Shares |source|'s state as a read-only view at version |snapshotVersion|.
    TCowTreeSnapshot(TSnapshotTag, const TBase& source, ui64 snapshotVersion);
};

////////////////////////////////////////////////////////////////////////////////

//! What a configuration is. Any type satisfying CCowTreeConfig works; the
//! facades below assemble one from the TCowTreeMtConfig/TCowTreeStConfig presets
//! (thread-safe and single-threaded respectively), and a hand-written config
//! spells out, in full:
//!
//!     struct TMyConfig
//!     {
//!         // The stored types: TValue = void makes a set, anything else a map.
//!         using TKey = TString;
//!         using TValue = void;
//!         // Strict weak order; declare is_transparent for heterogeneous
//!         // lookup, ReadSearch/WriteSearch/PrefersLinearSearch to tune the
//!         // in-node search (see cow_tree_detail/cow_tree_config.h).
//!         using TLess = std::less<TString>;
//!         // TAtomicRefCounter + a real lock let snapshots cross threads;
//!         // TPlainRefCounter + TNullMutex make the whole family
//!         // single-threaded and atomics-free.
//!         using TRefCounter = NCowTreeInternals::TAtomicRefCounter;
//!         using TMutex = TAdaptiveLock;
//!         // Elements per node (even, >= 4; 64 is the tuned default), the
//!         // linear tail of the hybrid in-node search, and the capacity bound
//!         // sizing the fixed descent stack — a hard contract verified at root
//!         // splits, not a hint.
//!         static constexpr int MaxNodeSize = 64;
//!         static constexpr int LinearSearchLimit = CowTreeDefaultLinearSearchLimit(MaxNodeSize);
//!         static constexpr ui64 MaxSize = ui64(1) << 32;
//!         // The node factory — the allocation hook.
//!         template <class TNode, class... TArgs>
//!         static TNode* CreateNode(TArgs&&... args)
//!         {
//!             return new TNode(std::forward<TArgs>(args)...);
//!         }
//!         template <class TNode>
//!         static void DeleteNode(TNode* node)
//!         {
//!             delete node;
//!         }
//!     };
//!
//! The TCowTreeConfig helper composes the stored types with a behavioural
//! preset when only the types differ.

//! The std::set-flavoured facade. TConfig carries the behavioural knobs — thread
//! safety and node size; the default lets snapshots cross threads, TCowTreeStConfig
//! gives a fully atomics-free single-threaded tree (snapshots included).
template <class TKey, class TLess = std::less<TKey>, class TConfig = TCowTreeMtConfig<>>
using TCowSet = TCowTree<TCowTreeConfig<TConfig, TKey, TLess>>;

template <class TKey, class TLess = std::less<TKey>, class TConfig = TCowTreeMtConfig<>>
using TCowSetSnapshot = TCowTreeSnapshot<TCowTreeConfig<TConfig, TKey, TLess>>;

//! The std::map-flavoured facade; values live inline in the leaves: use a refcounted
//! pointer as TValue for shared or bulky payloads.
template <class TKey, class TValue, class TLess = std::less<TKey>, class TConfig = TCowTreeMtConfig<>>
using TCowMap = TCowTree<TCowTreeConfig<TConfig, TKey, TLess, TValue>>;

template <class TKey, class TValue, class TLess = std::less<TKey>, class TConfig = TCowTreeMtConfig<>>
using TCowMapSnapshot = TCowTreeSnapshot<TCowTreeConfig<TConfig, TKey, TLess, TValue>>;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow

#define COW_TREE_INL_H
#include "cow_tree-inl.h"
#undef COW_TREE_INL_H
