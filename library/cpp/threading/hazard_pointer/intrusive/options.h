#pragma once

namespace NHp::NOptions {

    template <class _THookType>
    struct TBaseHook;

    template <class _TValueTraits>
    struct TValueTraits;

    template <class _TBucketTraits>
    struct TBucketTraits;

    template <bool _IsAutoUnlink>
    struct TIsAutoUnlink;

    template <class _TTag>
    struct TTag;

    template <bool _IsPower2Buckets>
    struct TIsPower2Buckets;

    template <bool _StoreHash>
    struct TStoreHash;

    template <class _TSizeType>
    struct TSizeType;

    template <class _TKeyOfValue>
    struct TKeyOfValue;

    template <class _THasher>
    struct THasher;

    template <class _TEqual>
    struct TEqual;

    template <class _TVoidPointer>
    struct TVoidPointer;

} // namespace NHp::NOptions
