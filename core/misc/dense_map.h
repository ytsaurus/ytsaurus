//===- llvm/ADT/DenseMap.h - Dense probed hash table ------------*- C++ -*-===//
//
//                     The LLVM Compiler Infrastructure
//
// This file is distributed under the University of Illinois Open Source
// License. See LICENSE.TXT for details.
//
//===----------------------------------------------------------------------===//
//
// This file defines the DenseMap class.
//
//===----------------------------------------------------------------------===//

#pragma once

#include "align_of.h"
#include <algorithm>
#include <cassert>
#include <cstddef>
#include <cstring>
#include <iterator>
#include <new>
#include <type_traits>
#include <utility>

#include <util/system/defaults.h>

#define YT_LLVM_UNLIKELY Y_UNLIKELY
#define YT_LLVM_LIKELY Y_LIKELY

/// Return true if the argument is a power of two > 0 (64 bit edition.)
constexpr inline bool isPowerOf2_64(uint64_t Value) {
    return Value && !(Value & (Value - int64_t(1L)));
}

/// Return the ceil log base 2 of the specified value, 32 if the value is zero.
/// (32 bit edition).
/// Ex. Log2_32_Ceil(32) == 5, Log2_32_Ceil(1) == 0, Log2_32_Ceil(6) == 3
inline unsigned Log2_32_Ceil(uint32_t Value) {
    return Value == 1 ? 0 : 32 - __builtin_clz(Value - 1);
}

/// Returns the next power of two (in 64-bits) that is strictly greater than A.
/// Returns zero on overflow.
inline uint64_t NextPowerOf2(uint64_t A) {
    A |= (A >> 1);
    A |= (A >> 2);
    A |= (A >> 4);
    A |= (A >> 8);
    A |= (A >> 16);
    A |= (A >> 32);
    return A + 1;
}

#ifndef __has_feature
# define __has_feature(x) 0
#endif

namespace NYT {

/// \brief If T is a pointer to X, return a pointer to const X. If it is not,
/// return const T.
template<typename T, typename Enable = void>
struct add_const_past_pointer {
  using type = const T;
};

template<typename T>
struct add_const_past_pointer<
  T, typename std::enable_if<std::is_pointer<T>::value>::type> {
  using type = const typename std::remove_pointer<T>::type *;
};

template<typename T, typename Enable = void>
struct const_pointer_or_const_ref {
  using type = const T &;
};
template<typename T>
struct const_pointer_or_const_ref<
  T, typename std::enable_if<std::is_pointer<T>::value>::type> {
  using type = typename add_const_past_pointer<T>::type;
};

template <typename T> class PointerLikeTypeTraits {
  // getAsVoidPointer
  // getFromVoidPointer
  // getNumLowBitsAvailable
};

namespace detail {
/// A tiny meta function to compute the log2 of a compile time constant.
template <size_t N>
struct ConstantLog2
    : std::integral_constant<size_t, ConstantLog2<N / 2>::value + 1> {};
template <> struct ConstantLog2<1> : std::integral_constant<size_t, 0> {};
}

// Provide PointerLikeTypeTraits for non-cvr pointers.
template <typename T> class PointerLikeTypeTraits<T *> {
public:
  static inline void *getAsVoidPointer(T *P) { return P; }
  static inline T *getFromVoidPointer(void *P) { return static_cast<T *>(P); }

  enum { NumLowBitsAvailable = detail::ConstantLog2<alignof(T)>::value };
};

template <> class PointerLikeTypeTraits<void *> {
public:
  static inline void *getAsVoidPointer(void *P) { return P; }
  static inline void *getFromVoidPointer(void *P) { return P; }

  /// Note, we assume here that void* is related to raw malloc'ed memory and
  /// that malloc returns objects at least 4-byte aligned. However, this may be
  /// wrong, or pointers may be from something other than malloc. In this case,
  /// you should specify a real typed pointer or avoid this template.
  ///
  /// All clients should use assertions to do a run-time check to ensure that
  /// this is actually true.
  enum { NumLowBitsAvailable = 2 };
};

// Provide PointerLikeTypeTraits for const things.
template <typename T> class PointerLikeTypeTraits<const T> {
  typedef PointerLikeTypeTraits<T> NonConst;

public:
  static inline const void *getAsVoidPointer(const T P) {
    return NonConst::getAsVoidPointer(P);
  }
  static inline const T getFromVoidPointer(const void *P) {
    return NonConst::getFromVoidPointer(const_cast<void *>(P));
  }
  enum { NumLowBitsAvailable = NonConst::NumLowBitsAvailable };
};

// Provide PointerLikeTypeTraits for const pointers.
template <typename T> class PointerLikeTypeTraits<const T *> {
  typedef PointerLikeTypeTraits<T *> NonConst;

public:
  static inline const void *getAsVoidPointer(const T *P) {
    return NonConst::getAsVoidPointer(const_cast<T *>(P));
  }
  static inline const T *getFromVoidPointer(const void *P) {
    return NonConst::getFromVoidPointer(const_cast<void *>(P));
  }
  enum { NumLowBitsAvailable = NonConst::NumLowBitsAvailable };
};

// Provide PointerLikeTypeTraits for uintptr_t.
template <> class PointerLikeTypeTraits<uintptr_t> {
public:
  static inline void *getAsVoidPointer(uintptr_t P) {
    return reinterpret_cast<void *>(P);
  }
  static inline uintptr_t getFromVoidPointer(void *P) {
    return reinterpret_cast<uintptr_t>(P);
  }
  // No bits are available!
  enum { NumLowBitsAvailable = 0 };
};

/// isPodLike - This is a type trait that is used to determine whether a given
/// type can be copied around with memcpy instead of running ctors etc.
template <typename T>
struct isPodLike {
    // std::is_trivially_copyable is available in libc++ with clang, libstdc++
    // that comes with GCC 5.
#if (__has_feature(is_trivially_copyable) && defined(_LIBCPP_VERSION)) ||      \
    (defined(__GNUC__) && __GNUC__ >= 5)
    // If the compiler supports the is_trivially_copyable trait use it, as it
    // matches the definition of isPodLike closely.
    static const bool value = std::is_trivially_copyable<T>::value;
#elif __has_feature(is_trivially_copyable)
    // Use the internal name if the compiler supports is_trivially_copyable but we
    // don't know if the standard library does. This is the case for clang in
    // conjunction with libstdc++ from GCC 4.x.
    static const bool value = __is_trivially_copyable(T);
#else
    // If we don't know anything else, we can (at least) assume that all non-class
    // types are PODs.
    static const bool value = !std::is_class<T>::value;
#endif
};

// std::pair's are pod-like if their elements are.
template<typename T, typename U>
struct isPodLike<std::pair<T, U>> {
    static const bool value = isPodLike<T>::value && isPodLike<U>::value;
};
} // namespace NYT

namespace NYT {

template<typename T>
struct TDenseMapInfo {
  //static inline T getEmptyKey();
  //static inline T getTombstoneKey();
  //static unsigned getHashValue(const T &Val);
  //static bool isEqual(const T &LHS, const T &RHS);
};

// Provide TDenseMapInfo for all pointers.
template<typename T>
struct TDenseMapInfo<T*> {
  static inline T* getEmptyKey() {
    uintptr_t Val = static_cast<uintptr_t>(-1);
    Val <<= NYT::PointerLikeTypeTraits<T*>::NumLowBitsAvailable;
    return reinterpret_cast<T*>(Val);
  }

  static inline T* getTombstoneKey() {
    uintptr_t Val = static_cast<uintptr_t>(-2);
    Val <<= NYT::PointerLikeTypeTraits<T*>::NumLowBitsAvailable;
    return reinterpret_cast<T*>(Val);
  }

  static unsigned getHashValue(const T *PtrVal) {
    return (unsigned((uintptr_t)PtrVal) >> 4) ^
           (unsigned((uintptr_t)PtrVal) >> 9);
  }

  static bool isEqual(const T *LHS, const T *RHS) { return LHS == RHS; }
};

// Provide TDenseMapInfo for chars.
template<> struct TDenseMapInfo<char> {
  static inline char getEmptyKey() { return ~0; }
  static inline char getTombstoneKey() { return ~0 - 1; }
  static unsigned getHashValue(const char& Val) { return Val * 37U; }

  static bool isEqual(const char &LHS, const char &RHS) {
    return LHS == RHS;
  }
};

// Provide TDenseMapInfo for unsigned shorts.
template <> struct TDenseMapInfo<unsigned short> {
  static inline unsigned short getEmptyKey() { return 0xFFFF; }
  static inline unsigned short getTombstoneKey() { return 0xFFFF - 1; }
  static unsigned getHashValue(const unsigned short &Val) { return Val * 37U; }

  static bool isEqual(const unsigned short &LHS, const unsigned short &RHS) {
    return LHS == RHS;
  }
};

// Provide TDenseMapInfo for unsigned ints.
template<> struct TDenseMapInfo<unsigned> {
  static inline unsigned getEmptyKey() { return ~0U; }
  static inline unsigned getTombstoneKey() { return ~0U - 1; }
  static unsigned getHashValue(const unsigned& Val) { return Val * 37U; }

  static bool isEqual(const unsigned& LHS, const unsigned& RHS) {
    return LHS == RHS;
  }
};

// Provide TDenseMapInfo for unsigned longs.
template<> struct TDenseMapInfo<unsigned long> {
  static inline unsigned long getEmptyKey() { return ~0UL; }
  static inline unsigned long getTombstoneKey() { return ~0UL - 1L; }

  static unsigned getHashValue(const unsigned long& Val) {
    return (unsigned)(Val * 37UL);
  }

  static bool isEqual(const unsigned long& LHS, const unsigned long& RHS) {
    return LHS == RHS;
  }
};

// Provide TDenseMapInfo for unsigned long longs.
template<> struct TDenseMapInfo<unsigned long long> {
  static inline unsigned long long getEmptyKey() { return ~0ULL; }
  static inline unsigned long long getTombstoneKey() { return ~0ULL - 1ULL; }

  static unsigned getHashValue(const unsigned long long& Val) {
    return (unsigned)(Val * 37ULL);
  }

  static bool isEqual(const unsigned long long& LHS,
                      const unsigned long long& RHS) {
    return LHS == RHS;
  }
};

// Provide TDenseMapInfo for shorts.
template <> struct TDenseMapInfo<short> {
  static inline short getEmptyKey() { return 0x7FFF; }
  static inline short getTombstoneKey() { return -0x7FFF - 1; }
  static unsigned getHashValue(const short &Val) { return Val * 37U; }
  static bool isEqual(const short &LHS, const short &RHS) { return LHS == RHS; }
};

// Provide TDenseMapInfo for ints.
template<> struct TDenseMapInfo<int> {
  static inline int getEmptyKey() { return 0x7fffffff; }
  static inline int getTombstoneKey() { return -0x7fffffff - 1; }
  static unsigned getHashValue(const int& Val) { return (unsigned)(Val * 37U); }

  static bool isEqual(const int& LHS, const int& RHS) {
    return LHS == RHS;
  }
};

// Provide TDenseMapInfo for longs.
template<> struct TDenseMapInfo<long> {
  static inline long getEmptyKey() {
    return (1UL << (sizeof(long) * 8 - 1)) - 1UL;
  }

  static inline long getTombstoneKey() { return getEmptyKey() - 1L; }

  static unsigned getHashValue(const long& Val) {
    return (unsigned)(Val * 37UL);
  }

  static bool isEqual(const long& LHS, const long& RHS) {
    return LHS == RHS;
  }
};

// Provide TDenseMapInfo for long longs.
template<> struct TDenseMapInfo<long long> {
  static inline long long getEmptyKey() { return 0x7fffffffffffffffLL; }
  static inline long long getTombstoneKey() { return -0x7fffffffffffffffLL-1; }

  static unsigned getHashValue(const long long& Val) {
    return (unsigned)(Val * 37ULL);
  }

  static bool isEqual(const long long& LHS,
                      const long long& RHS) {
    return LHS == RHS;
  }
};

// Provide TDenseMapInfo for all pairs whose members have info.
template<typename T, typename U>
struct TDenseMapInfo<std::pair<T, U>> {
  using Pair = std::pair<T, U>;
  using FirstInfo = TDenseMapInfo<T>;
  using SecondInfo = TDenseMapInfo<U>;

  static inline Pair getEmptyKey() {
    return std::make_pair(FirstInfo::getEmptyKey(),
                          SecondInfo::getEmptyKey());
  }

  static inline Pair getTombstoneKey() {
    return std::make_pair(FirstInfo::getTombstoneKey(),
                          SecondInfo::getTombstoneKey());
  }

  static unsigned getHashValue(const Pair& PairVal) {
    uint64_t key = (uint64_t)FirstInfo::getHashValue(PairVal.first) << 32
          | (uint64_t)SecondInfo::getHashValue(PairVal.second);
    key += ~(key << 32);
    key ^= (key >> 22);
    key += ~(key << 13);
    key ^= (key >> 8);
    key += (key << 3);
    key ^= (key >> 15);
    key += ~(key << 27);
    key ^= (key >> 31);
    return (unsigned)key;
  }

  static bool isEqual(const Pair &LHS, const Pair &RHS) {
    return FirstInfo::isEqual(LHS.first, RHS.first) &&
           SecondInfo::isEqual(LHS.second, RHS.second);
  }
};

} // namespace NYT

namespace NYT {

namespace NDetail {

// We extend a pair to allow users to override the bucket type with their own
// implementation without requiring two members.
template <typename KeyT, typename ValueT>
struct DenseMapPair : public std::pair<KeyT, ValueT> {
  KeyT &getFirst() { return std::pair<KeyT, ValueT>::first; }
  const KeyT &getFirst() const { return std::pair<KeyT, ValueT>::first; }
  ValueT &getSecond() { return std::pair<KeyT, ValueT>::second; }
  const ValueT &getSecond() const { return std::pair<KeyT, ValueT>::second; }
};

} // namespace NDetail

template <
    typename KeyT, typename ValueT, typename KeyInfoT = TDenseMapInfo<KeyT>,
    typename Bucket = NDetail::DenseMapPair<KeyT, ValueT>, bool IsConst = false>
class DenseMapIterator;

class DebugEpochBase {
  uint64_t Epoch;

public:
  DebugEpochBase() : Epoch(0) {}

  /// \brief Calling incrementEpoch invalidates all handles pointing into the
  /// calling instance.
  void incrementEpoch() { ++Epoch; }

  /// \brief The destructor calls incrementEpoch to make use-after-free bugs
  /// more likely to crash deterministically.
  ~DebugEpochBase() { incrementEpoch(); }

  /// \brief A base class for iterator classes ("handles") that wish to poll for
  /// iterator invalidating modifications in the underlying data structure.
  /// When LLVM is built without asserts, this class is empty and does nothing.
  ///
  /// HandleBase does not track the parent data structure by itself.  It expects
  /// the routines modifying the data structure to call incrementEpoch when they
  /// make an iterator-invalidating modification.
  ///
  class HandleBase {
      const uint64_t *EpochAddress;
      uint64_t EpochAtCreation;

  public:
      HandleBase() : EpochAddress(nullptr), EpochAtCreation(UINT64_MAX) {}

      explicit HandleBase(const DebugEpochBase *Parent)
          : EpochAddress(&Parent->Epoch), EpochAtCreation(Parent->Epoch) {}

      /// \brief Returns true if the DebugEpochBase this Handle is linked to has
      /// not called incrementEpoch on itself since the creation of this
      /// HandleBase instance.
      bool isHandleInSync() const { return *EpochAddress == EpochAtCreation; }

      /// \brief Returns a pointer to the epoch word stored in the data structure
      /// this handle points into.  Can be used to check if two iterators point
      /// into the same data structure.
      const void *getEpochAddress() const { return EpochAddress; }
  };
};

template <typename DerivedT, typename KeyT, typename ValueT, typename KeyInfoT,
          typename BucketT>
class DenseMapBase : public DebugEpochBase {
  template <typename T>
  using const_arg_type_t = typename NYT::const_pointer_or_const_ref<T>::type;

public:
  using size_type = unsigned;
  using key_type = KeyT;
  using mapped_type = ValueT;
  using value_type = BucketT;

  using iterator = DenseMapIterator<KeyT, ValueT, KeyInfoT, BucketT>;
  using const_iterator =
      DenseMapIterator<KeyT, ValueT, KeyInfoT, BucketT, true>;

  inline iterator begin() {
    // When the map is empty, avoid the overhead of AdvancePastEmptyBuckets().
    return empty() ? end() : iterator(getBuckets(), getBucketsEnd(), *this);
  }
  inline iterator end() {
    return iterator(getBucketsEnd(), getBucketsEnd(), *this, true);
  }
  inline const_iterator begin() const {
    return empty() ? end()
                   : const_iterator(getBuckets(), getBucketsEnd(), *this);
  }
  inline const_iterator end() const {
    return const_iterator(getBucketsEnd(), getBucketsEnd(), *this, true);
  }

  bool empty() const {
    return getNumEntries() == 0;
  }
  unsigned size() const { return getNumEntries(); }

  /// Grow the densemap so that it can contain at least \p NumEntries items
  /// before resizing again.
  void reserve(size_type NumEntries) {
    auto NumBuckets = getMinBucketToReserveForEntries(NumEntries);
    incrementEpoch();
    if (NumBuckets > getNumBuckets())
      grow(NumBuckets);
  }

  void clear() {
    incrementEpoch();
    if (getNumEntries() == 0 && getNumTombstones() == 0) return;

    // If the capacity of the array is huge, and the # elements used is small,
    // shrink the array.
    if (getNumEntries() * 4 < getNumBuckets() && getNumBuckets() > 64) {
      shrink_and_clear();
      return;
    }

    const KeyT EmptyKey = getEmptyKey(), TombstoneKey = getTombstoneKey();
    unsigned NumEntries = getNumEntries();
    for (BucketT *P = getBuckets(), *E = getBucketsEnd(); P != E; ++P) {
      if (!KeyInfoT::isEqual(P->getFirst(), EmptyKey)) {
        if (!KeyInfoT::isEqual(P->getFirst(), TombstoneKey)) {
          P->getSecond().~ValueT();
          --NumEntries;
        }
        P->getFirst() = EmptyKey;
      }
    }
    assert(NumEntries == 0 && "Node count imbalance!");
    setNumEntries(0);
    setNumTombstones(0);
  }

  /// Return 1 if the specified key is in the map, 0 otherwise.
  size_type count(const_arg_type_t<KeyT> Val) const {
    const BucketT *TheBucket;
    return LookupBucketFor(Val, TheBucket) ? 1 : 0;
  }

  iterator find(const_arg_type_t<KeyT> Val) {
    BucketT *TheBucket;
    if (LookupBucketFor(Val, TheBucket))
      return iterator(TheBucket, getBucketsEnd(), *this, true);
    return end();
  }
  const_iterator find(const_arg_type_t<KeyT> Val) const {
    const BucketT *TheBucket;
    if (LookupBucketFor(Val, TheBucket))
      return const_iterator(TheBucket, getBucketsEnd(), *this, true);
    return end();
  }

  /// Alternate version of find() which allows a different, and possibly
  /// less expensive, key type.
  /// The TDenseMapInfo is responsible for supplying methods
  /// getHashValue(LookupKeyT) and isEqual(LookupKeyT, KeyT) for each key
  /// type used.
  template<class LookupKeyT>
  iterator find_as(const LookupKeyT &Val) {
    BucketT *TheBucket;
    if (LookupBucketFor(Val, TheBucket))
      return iterator(TheBucket, getBucketsEnd(), *this, true);
    return end();
  }
  template<class LookupKeyT>
  const_iterator find_as(const LookupKeyT &Val) const {
    const BucketT *TheBucket;
    if (LookupBucketFor(Val, TheBucket))
      return const_iterator(TheBucket, getBucketsEnd(), *this, true);
    return end();
  }

  /// lookup - Return the entry for the specified key, or a default
  /// constructed value if no such entry exists.
  ValueT lookup(const_arg_type_t<KeyT> Val) const {
    const BucketT *TheBucket;
    if (LookupBucketFor(Val, TheBucket))
      return TheBucket->getSecond();
    return ValueT();
  }

  // Inserts key,value pair into the map if the key isn't already in the map.
  // If the key is already in the map, it returns false and doesn't update the
  // value.
  std::pair<iterator, bool> insert(const std::pair<KeyT, ValueT> &KV) {
    return try_emplace(KV.first, KV.second);
  }

  // Inserts key,value pair into the map if the key isn't already in the map.
  // If the key is already in the map, it returns false and doesn't update the
  // value.
  std::pair<iterator, bool> insert(std::pair<KeyT, ValueT> &&KV) {
    return try_emplace(std::move(KV.first), std::move(KV.second));
  }

  // Inserts key,value pair into the map if the key isn't already in the map.
  // The value is constructed in-place if the key is not in the map, otherwise
  // it is not moved.
  template <typename... Ts>
  std::pair<iterator, bool> try_emplace(KeyT &&Key, Ts &&... Args) {
    BucketT *TheBucket;
    if (LookupBucketFor(Key, TheBucket))
      return std::make_pair(iterator(TheBucket, getBucketsEnd(), *this, true),
                            false); // Already in map.

    // Otherwise, insert the new element.
    TheBucket =
        InsertIntoBucket(TheBucket, std::move(Key), std::forward<Ts>(Args)...);
    return std::make_pair(iterator(TheBucket, getBucketsEnd(), *this, true),
                          true);
  }

  // Inserts key,value pair into the map if the key isn't already in the map.
  // The value is constructed in-place if the key is not in the map, otherwise
  // it is not moved.
  template <typename... Ts>
  std::pair<iterator, bool> try_emplace(const KeyT &Key, Ts &&... Args) {
    BucketT *TheBucket;
    if (LookupBucketFor(Key, TheBucket))
      return std::make_pair(iterator(TheBucket, getBucketsEnd(), *this, true),
                            false); // Already in map.

    // Otherwise, insert the new element.
    TheBucket = InsertIntoBucket(TheBucket, Key, std::forward<Ts>(Args)...);
    return std::make_pair(iterator(TheBucket, getBucketsEnd(), *this, true),
                          true);
  }

  /// Alternate version of insert() which allows a different, and possibly
  /// less expensive, key type.
  /// The TDenseMapInfo is responsible for supplying methods
  /// getHashValue(LookupKeyT) and isEqual(LookupKeyT, KeyT) for each key
  /// type used.
  template <typename LookupKeyT>
  std::pair<iterator, bool> insert_as(std::pair<KeyT, ValueT> &&KV,
                                      const LookupKeyT &Val) {
    BucketT *TheBucket;
    if (LookupBucketFor(Val, TheBucket))
      return std::make_pair(iterator(TheBucket, getBucketsEnd(), *this, true),
                            false); // Already in map.

    // Otherwise, insert the new element.
    TheBucket = InsertIntoBucketWithLookup(TheBucket, std::move(KV.first),
                                           std::move(KV.second), Val);
    return std::make_pair(iterator(TheBucket, getBucketsEnd(), *this, true),
                          true);
  }

  /// insert - Range insertion of pairs.
  template<typename InputIt>
  void insert(InputIt I, InputIt E) {
    for (; I != E; ++I)
      insert(*I);
  }

  bool erase(const KeyT &Val) {
    BucketT *TheBucket;
    if (!LookupBucketFor(Val, TheBucket))
      return false; // not in map.

    TheBucket->getSecond().~ValueT();
    TheBucket->getFirst() = getTombstoneKey();
    decrementNumEntries();
    incrementNumTombstones();
    return true;
  }
  void erase(iterator I) {
    BucketT *TheBucket = &*I;
    TheBucket->getSecond().~ValueT();
    TheBucket->getFirst() = getTombstoneKey();
    decrementNumEntries();
    incrementNumTombstones();
  }

  value_type& FindAndConstruct(const KeyT &Key) {
    BucketT *TheBucket;
    if (LookupBucketFor(Key, TheBucket))
      return *TheBucket;

    return *InsertIntoBucket(TheBucket, Key);
  }

  ValueT &operator[](const KeyT &Key) {
    return FindAndConstruct(Key).second;
  }

  value_type& FindAndConstruct(KeyT &&Key) {
    BucketT *TheBucket;
    if (LookupBucketFor(Key, TheBucket))
      return *TheBucket;

    return *InsertIntoBucket(TheBucket, std::move(Key));
  }

  ValueT &operator[](KeyT &&Key) {
    return FindAndConstruct(std::move(Key)).second;
  }

  /// isPointerIntoBucketsArray - Return true if the specified pointer points
  /// somewhere into the DenseMap's array of buckets (i.e. either to a key or
  /// value in the DenseMap).
  bool isPointerIntoBucketsArray(const void *Ptr) const {
    return Ptr >= getBuckets() && Ptr < getBucketsEnd();
  }

  /// getPointerIntoBucketsArray() - Return an opaque pointer into the buckets
  /// array.  In conjunction with the previous method, this can be used to
  /// determine whether an insertion caused the DenseMap to reallocate.
  const void *getPointerIntoBucketsArray() const { return getBuckets(); }

protected:
  DenseMapBase() = default;

  void destroyAll() {
    if (getNumBuckets() == 0) // Nothing to do.
      return;

    const KeyT EmptyKey = getEmptyKey(), TombstoneKey = getTombstoneKey();
    for (BucketT *P = getBuckets(), *E = getBucketsEnd(); P != E; ++P) {
      if (!KeyInfoT::isEqual(P->getFirst(), EmptyKey) &&
          !KeyInfoT::isEqual(P->getFirst(), TombstoneKey))
        P->getSecond().~ValueT();
      P->getFirst().~KeyT();
    }
  }

  void initEmpty() {
    setNumEntries(0);
    setNumTombstones(0);

    assert((getNumBuckets() & (getNumBuckets()-1)) == 0 &&
           "# initial buckets must be a power of two!");
    const KeyT EmptyKey = getEmptyKey();
    for (BucketT *B = getBuckets(), *E = getBucketsEnd(); B != E; ++B)
      ::new (&B->getFirst()) KeyT(EmptyKey);
  }

  /// Returns the number of buckets to allocate to ensure that the DenseMap can
  /// accommodate \p NumEntries without need to grow().
  unsigned getMinBucketToReserveForEntries(unsigned NumEntries) {
    // Ensure that "NumEntries * 4 < NumBuckets * 3"
    if (NumEntries == 0)
      return 0;
    // +1 is required because of the strict equality.
    // For example if NumEntries is 48, we need to return 401.
    return NextPowerOf2(NumEntries * 4 / 3 + 1);
  }

  void moveFromOldBuckets(BucketT *OldBucketsBegin, BucketT *OldBucketsEnd) {
    initEmpty();

    // Insert all the old elements.
    const KeyT EmptyKey = getEmptyKey();
    const KeyT TombstoneKey = getTombstoneKey();
    for (BucketT *B = OldBucketsBegin, *E = OldBucketsEnd; B != E; ++B) {
      if (!KeyInfoT::isEqual(B->getFirst(), EmptyKey) &&
          !KeyInfoT::isEqual(B->getFirst(), TombstoneKey)) {
        // Insert the key/value into the new table.
        BucketT *DestBucket;
        bool FoundVal = LookupBucketFor(B->getFirst(), DestBucket);
        (void)FoundVal; // silence warning.
        assert(!FoundVal && "Key already in new map?");
        DestBucket->getFirst() = std::move(B->getFirst());
        ::new (&DestBucket->getSecond()) ValueT(std::move(B->getSecond()));
        incrementNumEntries();

        // Free the value.
        B->getSecond().~ValueT();
      }
      B->getFirst().~KeyT();
    }
  }

  template <typename OtherBaseT>
  void copyFrom(
      const DenseMapBase<OtherBaseT, KeyT, ValueT, KeyInfoT, BucketT> &other) {
    assert(&other != this);
    assert(getNumBuckets() == other.getNumBuckets());

    setNumEntries(other.getNumEntries());
    setNumTombstones(other.getNumTombstones());

    if (NYT::isPodLike<KeyT>::value && NYT::isPodLike<ValueT>::value)
      memcpy(getBuckets(), other.getBuckets(),
             getNumBuckets() * sizeof(BucketT));
    else
      for (size_t i = 0; i < getNumBuckets(); ++i) {
        ::new (&getBuckets()[i].getFirst())
            KeyT(other.getBuckets()[i].getFirst());
        if (!KeyInfoT::isEqual(getBuckets()[i].getFirst(), getEmptyKey()) &&
            !KeyInfoT::isEqual(getBuckets()[i].getFirst(), getTombstoneKey()))
          ::new (&getBuckets()[i].getSecond())
              ValueT(other.getBuckets()[i].getSecond());
      }
  }

  static unsigned getHashValue(const KeyT &Val) {
    return KeyInfoT::getHashValue(Val);
  }

  template<typename LookupKeyT>
  static unsigned getHashValue(const LookupKeyT &Val) {
    return KeyInfoT::getHashValue(Val);
  }

  static const KeyT getEmptyKey() {
    static_assert(std::is_base_of<DenseMapBase, DerivedT>::value,
                  "Must pass the derived type to this template!");
    return KeyInfoT::getEmptyKey();
  }

  static const KeyT getTombstoneKey() {
    return KeyInfoT::getTombstoneKey();
  }

private:
  unsigned getNumEntries() const {
    return static_cast<const DerivedT *>(this)->getNumEntries();
  }

  void setNumEntries(unsigned Num) {
    static_cast<DerivedT *>(this)->setNumEntries(Num);
  }

  void incrementNumEntries() {
    setNumEntries(getNumEntries() + 1);
  }

  void decrementNumEntries() {
    setNumEntries(getNumEntries() - 1);
  }

  unsigned getNumTombstones() const {
    return static_cast<const DerivedT *>(this)->getNumTombstones();
  }

  void setNumTombstones(unsigned Num) {
    static_cast<DerivedT *>(this)->setNumTombstones(Num);
  }

  void incrementNumTombstones() {
    setNumTombstones(getNumTombstones() + 1);
  }

  void decrementNumTombstones() {
    setNumTombstones(getNumTombstones() - 1);
  }

  const BucketT *getBuckets() const {
    return static_cast<const DerivedT *>(this)->getBuckets();
  }

  BucketT *getBuckets() {
    return static_cast<DerivedT *>(this)->getBuckets();
  }

  unsigned getNumBuckets() const {
    return static_cast<const DerivedT *>(this)->getNumBuckets();
  }

  BucketT *getBucketsEnd() {
    return getBuckets() + getNumBuckets();
  }

  const BucketT *getBucketsEnd() const {
    return getBuckets() + getNumBuckets();
  }

  void grow(unsigned AtLeast) {
    static_cast<DerivedT *>(this)->grow(AtLeast);
  }

  void shrink_and_clear() {
    static_cast<DerivedT *>(this)->shrink_and_clear();
  }

  template <typename KeyArg, typename... ValueArgs>
  BucketT *InsertIntoBucket(BucketT *TheBucket, KeyArg &&Key,
                            ValueArgs &&... Values) {
    TheBucket = InsertIntoBucketImpl(Key, Key, TheBucket);

    TheBucket->getFirst() = std::forward<KeyArg>(Key);
    ::new (&TheBucket->getSecond()) ValueT(std::forward<ValueArgs>(Values)...);
    return TheBucket;
  }

  template <typename LookupKeyT>
  BucketT *InsertIntoBucketWithLookup(BucketT *TheBucket, KeyT &&Key,
                                      ValueT &&Value, LookupKeyT &Lookup) {
    TheBucket = InsertIntoBucketImpl(Key, Lookup, TheBucket);

    TheBucket->getFirst() = std::move(Key);
    ::new (&TheBucket->getSecond()) ValueT(std::move(Value));
    return TheBucket;
  }

  template <typename LookupKeyT>
  BucketT *InsertIntoBucketImpl(const KeyT &Key, const LookupKeyT &Lookup,
                                BucketT *TheBucket) {
    incrementEpoch();

    // If the load of the hash table is more than 3/4, or if fewer than 1/8 of
    // the buckets are empty (meaning that many are filled with tombstones),
    // grow the table.
    //
    // The later case is tricky.  For example, if we had one empty bucket with
    // tons of tombstones, failing lookups (e.g. for insertion) would have to
    // probe almost the entire table until it found the empty bucket.  If the
    // table completely filled with tombstones, no lookup would ever succeed,
    // causing infinite loops in lookup.
    unsigned NewNumEntries = getNumEntries() + 1;
    unsigned NumBuckets = getNumBuckets();
    if (YT_LLVM_UNLIKELY(NewNumEntries * 4 >= NumBuckets * 3)) {
      this->grow(NumBuckets * 2);
      LookupBucketFor(Lookup, TheBucket);
      NumBuckets = getNumBuckets();
    } else if (YT_LLVM_UNLIKELY(NumBuckets-(NewNumEntries+getNumTombstones()) <=
                             NumBuckets/8)) {
      this->grow(NumBuckets);
      LookupBucketFor(Lookup, TheBucket);
    }
    assert(TheBucket);

    // Only update the state after we've grown our bucket space appropriately
    // so that when growing buckets we have self-consistent entry count.
    incrementNumEntries();

    // If we are writing over a tombstone, remember this.
    const KeyT EmptyKey = getEmptyKey();
    if (!KeyInfoT::isEqual(TheBucket->getFirst(), EmptyKey))
      decrementNumTombstones();

    return TheBucket;
  }

  /// LookupBucketFor - Lookup the appropriate bucket for Val, returning it in
  /// FoundBucket.  If the bucket contains the key and a value, this returns
  /// true, otherwise it returns a bucket with an empty marker or tombstone and
  /// returns false.
  template<typename LookupKeyT>
  bool LookupBucketFor(const LookupKeyT &Val,
                       const BucketT *&FoundBucket) const {
    const BucketT *BucketsPtr = getBuckets();
    const unsigned NumBuckets = getNumBuckets();

    if (NumBuckets == 0) {
      FoundBucket = nullptr;
      return false;
    }

    // FoundTombstone - Keep track of whether we find a tombstone while probing.
    const BucketT *FoundTombstone = nullptr;
    const KeyT EmptyKey = getEmptyKey();
    const KeyT TombstoneKey = getTombstoneKey();
    assert(!KeyInfoT::isEqual(Val, EmptyKey) &&
           !KeyInfoT::isEqual(Val, TombstoneKey) &&
           "Empty/Tombstone value shouldn't be inserted into map!");

    unsigned BucketNo = getHashValue(Val) & (NumBuckets-1);
    unsigned ProbeAmt = 1;
    while (true) {
      const BucketT *ThisBucket = BucketsPtr + BucketNo;
      // Found Val's bucket?  If so, return it.
      if (YT_LLVM_LIKELY(KeyInfoT::isEqual(Val, ThisBucket->getFirst()))) {
        FoundBucket = ThisBucket;
        return true;
      }

      // If we found an empty bucket, the key doesn't exist in the set.
      // Insert it and return the default value.
      if (YT_LLVM_LIKELY(KeyInfoT::isEqual(ThisBucket->getFirst(), EmptyKey))) {
        // If we've already seen a tombstone while probing, fill it in instead
        // of the empty bucket we eventually probed to.
        FoundBucket = FoundTombstone ? FoundTombstone : ThisBucket;
        return false;
      }

      // If this is a tombstone, remember it.  If Val ends up not in the map, we
      // prefer to return it than something that would require more probing.
      if (KeyInfoT::isEqual(ThisBucket->getFirst(), TombstoneKey) &&
          !FoundTombstone)
        FoundTombstone = ThisBucket;  // Remember the first tombstone found.

      // Otherwise, it's a hash collision or a tombstone, continue quadratic
      // probing.
      BucketNo += ProbeAmt++;
      BucketNo &= (NumBuckets-1);
    }
  }

  template <typename LookupKeyT>
  bool LookupBucketFor(const LookupKeyT &Val, BucketT *&FoundBucket) {
    const BucketT *ConstFoundBucket;
    bool Result = const_cast<const DenseMapBase *>(this)
      ->LookupBucketFor(Val, ConstFoundBucket);
    FoundBucket = const_cast<BucketT *>(ConstFoundBucket);
    return Result;
  }

public:
  /// Return the approximate size (in bytes) of the actual map.
  /// This is just the raw memory used by DenseMap.
  /// If entries are pointers to objects, the size of the referenced objects
  /// are not included.
  size_t getMemorySize() const {
    return getNumBuckets() * sizeof(BucketT);
  }
};

template <typename KeyT, typename ValueT,
          typename KeyInfoT = TDenseMapInfo<KeyT>,
          typename BucketT = NDetail::DenseMapPair<KeyT, ValueT>>
class DenseMap : public DenseMapBase<DenseMap<KeyT, ValueT, KeyInfoT, BucketT>,
                                     KeyT, ValueT, KeyInfoT, BucketT> {
  friend class DenseMapBase<DenseMap, KeyT, ValueT, KeyInfoT, BucketT>;

  // Lift some types from the dependent base class into this class for
  // simplicity of referring to them.
  using BaseT = DenseMapBase<DenseMap, KeyT, ValueT, KeyInfoT, BucketT>;

  BucketT *Buckets;
  unsigned NumEntries;
  unsigned NumTombstones;
  unsigned NumBuckets;

public:
  /// Create a DenseMap wth an optional \p InitialReserve that guarantee that
  /// this number of elements can be inserted in the map without grow()
  explicit DenseMap(unsigned InitialReserve = 0) { init(InitialReserve); }

  DenseMap(const DenseMap &other) : BaseT() {
    init(0);
    copyFrom(other);
  }

  DenseMap(DenseMap &&other) : BaseT() {
    init(0);
    swap(other);
  }

  template<typename InputIt>
  DenseMap(const InputIt &I, const InputIt &E) {
    init(std::distance(I, E));
    this->insert(I, E);
  }

  ~DenseMap() {
    this->destroyAll();
    operator delete(Buckets);
  }

  void swap(DenseMap& RHS) {
    this->incrementEpoch();
    RHS.incrementEpoch();
    std::swap(Buckets, RHS.Buckets);
    std::swap(NumEntries, RHS.NumEntries);
    std::swap(NumTombstones, RHS.NumTombstones);
    std::swap(NumBuckets, RHS.NumBuckets);
  }

  DenseMap& operator=(const DenseMap& other) {
    if (&other != this)
      copyFrom(other);
    return *this;
  }

  DenseMap& operator=(DenseMap &&other) {
    this->destroyAll();
    operator delete(Buckets);
    init(0);
    swap(other);
    return *this;
  }

  void copyFrom(const DenseMap& other) {
    this->destroyAll();
    operator delete(Buckets);
    if (allocateBuckets(other.NumBuckets)) {
      this->BaseT::copyFrom(other);
    } else {
      NumEntries = 0;
      NumTombstones = 0;
    }
  }

  void init(unsigned InitNumEntries) {
    auto InitBuckets = BaseT::getMinBucketToReserveForEntries(InitNumEntries);
    if (allocateBuckets(InitBuckets)) {
      this->BaseT::initEmpty();
    } else {
      NumEntries = 0;
      NumTombstones = 0;
    }
  }

  void grow(unsigned AtLeast) {
    unsigned OldNumBuckets = NumBuckets;
    BucketT *OldBuckets = Buckets;

    allocateBuckets(std::max<unsigned>(64, static_cast<unsigned>(NextPowerOf2(AtLeast-1))));
    assert(Buckets);
    if (!OldBuckets) {
      this->BaseT::initEmpty();
      return;
    }

    this->moveFromOldBuckets(OldBuckets, OldBuckets+OldNumBuckets);

    // Free the old table.
    operator delete(OldBuckets);
  }

  void shrink_and_clear() {
    unsigned OldNumEntries = NumEntries;
    this->destroyAll();

    // Reduce the number of buckets.
    unsigned NewNumBuckets = 0;
    if (OldNumEntries)
      NewNumBuckets = std::max(64, 1 << (Log2_32_Ceil(OldNumEntries) + 1));
    if (NewNumBuckets == NumBuckets) {
      this->BaseT::initEmpty();
      return;
    }

    operator delete(Buckets);
    init(NewNumBuckets);
  }

private:
  unsigned getNumEntries() const {
    return NumEntries;
  }

  void setNumEntries(unsigned Num) {
    NumEntries = Num;
  }

  unsigned getNumTombstones() const {
    return NumTombstones;
  }

  void setNumTombstones(unsigned Num) {
    NumTombstones = Num;
  }

  BucketT *getBuckets() const {
    return Buckets;
  }

  unsigned getNumBuckets() const {
    return NumBuckets;
  }

  bool allocateBuckets(unsigned Num) {
    NumBuckets = Num;
    if (NumBuckets == 0) {
      Buckets = nullptr;
      return false;
    }

    Buckets = static_cast<BucketT*>(operator new(sizeof(BucketT) * NumBuckets));
    return true;
  }
};

template <typename KeyT, typename ValueT, unsigned InlineBuckets = 4,
          typename KeyInfoT = TDenseMapInfo<KeyT>,
          typename BucketT = NDetail::DenseMapPair<KeyT, ValueT>>
class SmallDenseMap
    : public DenseMapBase<
          SmallDenseMap<KeyT, ValueT, InlineBuckets, KeyInfoT, BucketT>, KeyT,
          ValueT, KeyInfoT, BucketT> {
  friend class DenseMapBase<SmallDenseMap, KeyT, ValueT, KeyInfoT, BucketT>;

  // Lift some types from the dependent base class into this class for
  // simplicity of referring to them.
  using BaseT = DenseMapBase<SmallDenseMap, KeyT, ValueT, KeyInfoT, BucketT>;

  static_assert(isPowerOf2_64(InlineBuckets),
                "InlineBuckets must be a power of 2.");

  unsigned Small : 1;
  unsigned NumEntries : 31;
  unsigned NumTombstones;

  struct LargeRep {
    BucketT *Buckets;
    unsigned NumBuckets;
  };

  /// A "union" of an inline bucket array and the struct representing
  /// a large bucket. This union will be discriminated by the 'Small' bit.
  AlignedCharArrayUnion<BucketT[InlineBuckets], LargeRep> storage;

public:
  explicit SmallDenseMap(unsigned NumInitBuckets = 0) {
    init(NumInitBuckets);
  }

  SmallDenseMap(const SmallDenseMap &other) : BaseT() {
    init(0);
    copyFrom(other);
  }

  SmallDenseMap(SmallDenseMap &&other) : BaseT() {
    init(0);
    swap(other);
  }

  template<typename InputIt>
  SmallDenseMap(const InputIt &I, const InputIt &E) {
    init(NextPowerOf2(std::distance(I, E)));
    this->insert(I, E);
  }

  ~SmallDenseMap() {
    this->destroyAll();
    deallocateBuckets();
  }

  void swap(SmallDenseMap& RHS) {
    unsigned TmpNumEntries = RHS.NumEntries;
    RHS.NumEntries = NumEntries;
    NumEntries = TmpNumEntries;
    std::swap(NumTombstones, RHS.NumTombstones);

    const KeyT EmptyKey = this->getEmptyKey();
    const KeyT TombstoneKey = this->getTombstoneKey();
    if (Small && RHS.Small) {
      // If we're swapping inline bucket arrays, we have to cope with some of
      // the tricky bits of DenseMap's storage system: the buckets are not
      // fully initialized. Thus we swap every key, but we may have
      // a one-directional move of the value.
      for (unsigned i = 0, e = InlineBuckets; i != e; ++i) {
        BucketT *LHSB = &getInlineBuckets()[i],
                *RHSB = &RHS.getInlineBuckets()[i];
        bool hasLHSValue = (!KeyInfoT::isEqual(LHSB->getFirst(), EmptyKey) &&
                            !KeyInfoT::isEqual(LHSB->getFirst(), TombstoneKey));
        bool hasRHSValue = (!KeyInfoT::isEqual(RHSB->getFirst(), EmptyKey) &&
                            !KeyInfoT::isEqual(RHSB->getFirst(), TombstoneKey));
        if (hasLHSValue && hasRHSValue) {
          // Swap together if we can...
          std::swap(*LHSB, *RHSB);
          continue;
        }
        // Swap separately and handle any assymetry.
        std::swap(LHSB->getFirst(), RHSB->getFirst());
        if (hasLHSValue) {
          ::new (&RHSB->getSecond()) ValueT(std::move(LHSB->getSecond()));
          LHSB->getSecond().~ValueT();
        } else if (hasRHSValue) {
          ::new (&LHSB->getSecond()) ValueT(std::move(RHSB->getSecond()));
          RHSB->getSecond().~ValueT();
        }
      }
      return;
    }
    if (!Small && !RHS.Small) {
      std::swap(getLargeRep()->Buckets, RHS.getLargeRep()->Buckets);
      std::swap(getLargeRep()->NumBuckets, RHS.getLargeRep()->NumBuckets);
      return;
    }

    SmallDenseMap &SmallSide = Small ? *this : RHS;
    SmallDenseMap &LargeSide = Small ? RHS : *this;

    // First stash the large side's rep and move the small side across.
    LargeRep TmpRep = std::move(*LargeSide.getLargeRep());
    LargeSide.getLargeRep()->~LargeRep();
    LargeSide.Small = true;
    // This is similar to the standard move-from-old-buckets, but the bucket
    // count hasn't actually rotated in this case. So we have to carefully
    // move construct the keys and values into their new locations, but there
    // is no need to re-hash things.
    for (unsigned i = 0, e = InlineBuckets; i != e; ++i) {
      BucketT *NewB = &LargeSide.getInlineBuckets()[i],
              *OldB = &SmallSide.getInlineBuckets()[i];
      ::new (&NewB->getFirst()) KeyT(std::move(OldB->getFirst()));
      OldB->getFirst().~KeyT();
      if (!KeyInfoT::isEqual(NewB->getFirst(), EmptyKey) &&
          !KeyInfoT::isEqual(NewB->getFirst(), TombstoneKey)) {
        ::new (&NewB->getSecond()) ValueT(std::move(OldB->getSecond()));
        OldB->getSecond().~ValueT();
      }
    }

    // The hard part of moving the small buckets across is done, just move
    // the TmpRep into its new home.
    SmallSide.Small = false;
    new (SmallSide.getLargeRep()) LargeRep(std::move(TmpRep));
  }

  SmallDenseMap& operator=(const SmallDenseMap& other) {
    if (&other != this)
      copyFrom(other);
    return *this;
  }

  SmallDenseMap& operator=(SmallDenseMap &&other) {
    this->destroyAll();
    deallocateBuckets();
    init(0);
    swap(other);
    return *this;
  }

  void copyFrom(const SmallDenseMap& other) {
    this->destroyAll();
    deallocateBuckets();
    Small = true;
    if (other.getNumBuckets() > InlineBuckets) {
      Small = false;
      new (getLargeRep()) LargeRep(allocateBuckets(other.getNumBuckets()));
    }
    this->BaseT::copyFrom(other);
  }

  void init(unsigned InitBuckets) {
    Small = true;
    if (InitBuckets > InlineBuckets) {
      Small = false;
      new (getLargeRep()) LargeRep(allocateBuckets(InitBuckets));
    }
    this->BaseT::initEmpty();
  }

  void grow(unsigned AtLeast) {
    if (AtLeast >= InlineBuckets)
      AtLeast = std::max<unsigned>(64, NextPowerOf2(AtLeast-1));

    if (Small) {
      if (AtLeast < InlineBuckets)
        return; // Nothing to do.

      // First move the inline buckets into a temporary storage.
      AlignedCharArrayUnion<BucketT[InlineBuckets]> TmpStorage;
      BucketT *TmpBegin = reinterpret_cast<BucketT *>(TmpStorage.buffer);
      BucketT *TmpEnd = TmpBegin;

      // Loop over the buckets, moving non-empty, non-tombstones into the
      // temporary storage. Have the loop move the TmpEnd forward as it goes.
      const KeyT EmptyKey = this->getEmptyKey();
      const KeyT TombstoneKey = this->getTombstoneKey();
      for (BucketT *P = getBuckets(), *E = P + InlineBuckets; P != E; ++P) {
        if (!KeyInfoT::isEqual(P->getFirst(), EmptyKey) &&
            !KeyInfoT::isEqual(P->getFirst(), TombstoneKey)) {
          assert(size_t(TmpEnd - TmpBegin) < InlineBuckets &&
                 "Too many inline buckets!");
          ::new (&TmpEnd->getFirst()) KeyT(std::move(P->getFirst()));
          ::new (&TmpEnd->getSecond()) ValueT(std::move(P->getSecond()));
          ++TmpEnd;
          P->getSecond().~ValueT();
        }
        P->getFirst().~KeyT();
      }

      // Now make this map use the large rep, and move all the entries back
      // into it.
      Small = false;
      new (getLargeRep()) LargeRep(allocateBuckets(AtLeast));
      this->moveFromOldBuckets(TmpBegin, TmpEnd);
      return;
    }

    LargeRep OldRep = std::move(*getLargeRep());
    getLargeRep()->~LargeRep();
    if (AtLeast <= InlineBuckets) {
      Small = true;
    } else {
      new (getLargeRep()) LargeRep(allocateBuckets(AtLeast));
    }

    this->moveFromOldBuckets(OldRep.Buckets, OldRep.Buckets+OldRep.NumBuckets);

    // Free the old table.
    operator delete(OldRep.Buckets);
  }

  void shrink_and_clear() {
    unsigned OldSize = this->size();
    this->destroyAll();

    // Reduce the number of buckets.
    unsigned NewNumBuckets = 0;
    if (OldSize) {
      NewNumBuckets = 1 << (Log2_32_Ceil(OldSize) + 1);
      if (NewNumBuckets > InlineBuckets && NewNumBuckets < 64u)
        NewNumBuckets = 64;
    }
    if ((Small && NewNumBuckets <= InlineBuckets) ||
        (!Small && NewNumBuckets == getLargeRep()->NumBuckets)) {
      this->BaseT::initEmpty();
      return;
    }

    deallocateBuckets();
    init(NewNumBuckets);
  }

private:
  unsigned getNumEntries() const {
    return NumEntries;
  }

  void setNumEntries(unsigned Num) {
    // NumEntries is hardcoded to be 31 bits wide.
    assert(Num < (1U << 31) && "Cannot support more than 1<<31 entries");
    NumEntries = Num;
  }

  unsigned getNumTombstones() const {
    return NumTombstones;
  }

  void setNumTombstones(unsigned Num) {
    NumTombstones = Num;
  }

  const BucketT *getInlineBuckets() const {
    assert(Small);
    // Note that this cast does not violate aliasing rules as we assert that
    // the memory's dynamic type is the small, inline bucket buffer, and the
    // 'storage.buffer' static type is 'char *'.
    return reinterpret_cast<const BucketT *>(storage.buffer);
  }

  BucketT *getInlineBuckets() {
    return const_cast<BucketT *>(
      const_cast<const SmallDenseMap *>(this)->getInlineBuckets());
  }

  const LargeRep *getLargeRep() const {
    assert(!Small);
    // Note, same rule about aliasing as with getInlineBuckets.
    return reinterpret_cast<const LargeRep *>(storage.buffer);
  }

  LargeRep *getLargeRep() {
    return const_cast<LargeRep *>(
      const_cast<const SmallDenseMap *>(this)->getLargeRep());
  }

  const BucketT *getBuckets() const {
    return Small ? getInlineBuckets() : getLargeRep()->Buckets;
  }

  BucketT *getBuckets() {
    return const_cast<BucketT *>(
      const_cast<const SmallDenseMap *>(this)->getBuckets());
  }

  unsigned getNumBuckets() const {
    return Small ? InlineBuckets : getLargeRep()->NumBuckets;
  }

  void deallocateBuckets() {
    if (Small)
      return;

    operator delete(getLargeRep()->Buckets);
    getLargeRep()->~LargeRep();
  }

  LargeRep allocateBuckets(unsigned Num) {
    assert(Num > InlineBuckets && "Must allocate more buckets than are inline");
    LargeRep Rep = {
      static_cast<BucketT*>(operator new(sizeof(BucketT) * Num)), Num
    };
    return Rep;
  }
};

template <typename KeyT, typename ValueT, typename KeyInfoT, typename Bucket,
          bool IsConst>
class DenseMapIterator : DebugEpochBase::HandleBase {
  friend class DenseMapIterator<KeyT, ValueT, KeyInfoT, Bucket, true>;
  friend class DenseMapIterator<KeyT, ValueT, KeyInfoT, Bucket, false>;

  using ConstIterator = DenseMapIterator<KeyT, ValueT, KeyInfoT, Bucket, true>;

public:
  using difference_type = ptrdiff_t;
  using value_type =
      typename std::conditional<IsConst, const Bucket, Bucket>::type;
  using pointer = value_type *;
  using reference = value_type &;
  using iterator_category = std::forward_iterator_tag;

private:
  pointer Ptr = nullptr;
  pointer End = nullptr;

public:
  DenseMapIterator() = default;

  DenseMapIterator(pointer Pos, pointer E, const DebugEpochBase &Epoch,
                   bool NoAdvance = false)
      : DebugEpochBase::HandleBase(&Epoch), Ptr(Pos), End(E) {
    assert(isHandleInSync() && "invalid construction!");
    if (!NoAdvance) AdvancePastEmptyBuckets();
  }

  // Converting ctor from non-const iterators to const iterators. SFINAE'd out
  // for const iterator destinations so it doesn't end up as a user defined copy
  // constructor.
  template <bool IsConstSrc,
            typename = typename std::enable_if<!IsConstSrc && IsConst>::type>
  DenseMapIterator(
      const DenseMapIterator<KeyT, ValueT, KeyInfoT, Bucket, IsConstSrc> &I)
      : DebugEpochBase::HandleBase(I), Ptr(I.Ptr), End(I.End) {}

  reference operator*() const {
    assert(isHandleInSync() && "invalid iterator access!");
    return *Ptr;
  }
  pointer operator->() const {
    assert(isHandleInSync() && "invalid iterator access!");
    return Ptr;
  }

  bool operator==(const ConstIterator &RHS) const {
    assert((!Ptr || isHandleInSync()) && "handle not in sync!");
    assert((!RHS.Ptr || RHS.isHandleInSync()) && "handle not in sync!");
    assert(getEpochAddress() == RHS.getEpochAddress() &&
           "comparing incomparable iterators!");
    return Ptr == RHS.Ptr;
  }
  bool operator!=(const ConstIterator &RHS) const {
    assert((!Ptr || isHandleInSync()) && "handle not in sync!");
    assert((!RHS.Ptr || RHS.isHandleInSync()) && "handle not in sync!");
    assert(getEpochAddress() == RHS.getEpochAddress() &&
           "comparing incomparable iterators!");
    return Ptr != RHS.Ptr;
  }

  inline DenseMapIterator& operator++() {  // Preincrement
    assert(isHandleInSync() && "invalid iterator access!");
    ++Ptr;
    AdvancePastEmptyBuckets();
    return *this;
  }
  DenseMapIterator operator++(int) {  // Postincrement
    assert(isHandleInSync() && "invalid iterator access!");
    DenseMapIterator tmp = *this; ++*this; return tmp;
  }

private:
  void AdvancePastEmptyBuckets() {
    const KeyT Empty = KeyInfoT::getEmptyKey();
    const KeyT Tombstone = KeyInfoT::getTombstoneKey();

    while (Ptr != End && (KeyInfoT::isEqual(Ptr->getFirst(), Empty) ||
                          KeyInfoT::isEqual(Ptr->getFirst(), Tombstone)))
      ++Ptr;
  }
};

template<typename KeyT, typename ValueT, typename KeyInfoT>
static inline size_t
capacity_in_bytes(const DenseMap<KeyT, ValueT, KeyInfoT> &X) {
  return X.getMemorySize();
}

} // namespace NYT
