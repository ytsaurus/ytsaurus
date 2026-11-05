#pragma once

#include "size_tracker.h"

#include <library/cpp/threading/hazard_pointer/options/get_option.h>
#include <library/cpp/threading/hazard_pointer/utils/macro.h>
#include <library/cpp/threading/hazard_pointer/utils/marked_ptr.h>

#include <util/generic/ptr.h>
#include <util/system/compiler.h>

#include <atomic>
#include <cstddef>
#include <type_traits>
#include <utility>

namespace NLockFreeMap::NPrivate {

    template <class _TKeyCompare, class _TKeySelect, class _TReclaimer, class _TBackoff>
    struct THarrisMichaelListPolicy {
        using TKeyCompare = _TKeyCompare;
        using TKeySelect = _TKeySelect;
        using TReclaimer = _TReclaimer;
        using TBackoff = _TBackoff;
    };

    template <class TValue, class TPolicy>
    class THarrisMichaelList {
    protected:
        using TReclaimer = typename TPolicy::TReclaimer;
        using TKeySelect = typename TPolicy::TKeySelect;
        using TKeyCompare = typename TPolicy::TKeyCompare;
        using TBackoff = typename TPolicy::TBackoff;

        struct TNode;

        template <bool IsConst>
        class TIterator;

        template <bool IsConst>
        struct TAccessor;

        using TGuard = typename TReclaimer::TGuard;

        using TNodePtr = TNode*;
        using TNodeMarkedPtr = NHp::NUtil::TMarkedPtr<TNode>;

        using TKey = typename TKeySelect::TType;

    public:
        using value_type = TValue;
        using key_type = TKey;

        using iterator = TIterator<!std::is_same_v<TValue, TKey>>;
        using const_iterator = TIterator<true>;

        using accessor = TAccessor<!std::is_same_v<TValue, TKey>>;
        using const_accessor = TAccessor<true>;

        using key_select = TKeySelect;

        explicit THarrisMichaelList(const TKeyCompare& compare = {}, const TReclaimer& reclaimer = {}) noexcept;

        THarrisMichaelList(const THarrisMichaelList&) = delete;
        THarrisMichaelList& operator=(const THarrisMichaelList&) = delete;

        ~THarrisMichaelList();

        bool insert(const TValue& value);
        bool insert(TValue&& value);

        template <class _TValue>
        requires std::constructible_from<TValue, _TValue&&>
        bool insert(_TValue&& value);

        template <class... TArgs>
        requires std::constructible_from<TValue, TArgs&&...>
        bool emplace(TArgs&&... args);

        bool erase(const TKey& key);
        accessor extract(const TKey& key);

        void clear();

        accessor at(const TKey& key);
        const_accessor at(const TKey& key) const;

        iterator find(const TKey& key);
        const_iterator find(const TKey& key) const;

        iterator lower_bound(const TKey& key);
        const_iterator lower_bound(const TKey& key) const;

        bool contains(const TKey& key) const;

        bool empty() const noexcept;
        std::size_t size() const noexcept;

        iterator begin() noexcept;
        iterator end() noexcept;

        const_iterator cbegin() const noexcept;
        const_iterator cend() const noexcept;

        const_iterator begin() const noexcept;
        const_iterator end() const noexcept;

    protected:
        struct TPosition;
        TPosition MakePosition() const noexcept;

        bool Unlink(const TPosition& pos);
        bool Link(const TPosition& pos, TNodePtr newNode);
        bool Find(const TKey& key, TPosition& pos, TBackoff& backoff) const;

        bool InsertNode(THolder<TNode> newNode);

        template <class TNodeFactory>
        bool InsertNode(const TKey& key, TNodeFactory&& factory);

        std::pair<TNodePtr, TGuard> ExtractNode(const TKey& key);
        std::pair<TNodePtr, TGuard> GetHead() const;

        static TNodePtr UnpackMarkedPtr(const TNodeMarkedPtr markedPtr) noexcept;

    private:
        CACHE_LINE_ALIGNAS std::atomic<TNodeMarkedPtr> Head_{};
        Y_NO_UNIQUE_ADDRESS TSizeTracker Size_{};
        Y_NO_UNIQUE_ADDRESS TKeyCompare KeyCompare_{};
        Y_NO_UNIQUE_ADDRESS TKeySelect KeySelect_{};
        Y_NO_UNIQUE_ADDRESS TReclaimer Reclaimer_{};
    };

    template <class TValue, class... TOptions>
    struct TMakeHarrisMichaelListSet;

    template <class TKey, class TValue, class... TOptions>
    struct TMakeHarrisMichaelListMap;

} // namespace NLockFreeMap::NPrivate

namespace NLockFreeMap {

    template <class TValue, class... TOptions>
    using THarrisMichaelListSet = typename NPrivate::TMakeHarrisMichaelListSet<TValue, TOptions...>::TType;

    template <class TKey, class TValue, class... TOptions>
    using THarrisMichaelListMap = typename NPrivate::TMakeHarrisMichaelListMap<TKey, TValue, TOptions...>::TType;

} // namespace NLockFreeMap

#define INCLUDE_HARRIS_MICHAEL_LIST_INL_H
#include "harris_michael_list-inl.h"
#undef INCLUDE_HARRIS_MICHAEL_LIST_INL_H
