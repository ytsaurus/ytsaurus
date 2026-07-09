#pragma once

#include "node_holder.h"
#include "pointer_cast_traits.h"

#include <memory>

namespace NHp::NIntrusive::NPrivate {

    template <class _TValue, class _TNodeTraits, class TTag, bool _IsAutoUnlink>
    struct TBaseValueTraits {
        using TNodeTraits = _TNodeTraits;

        using TNode = typename TNodeTraits::TNode;
        using TNodePtr = typename TNodeTraits::TNodePtr;
        using TConstNodePtr = typename TNodeTraits::TConstNodePtr;

        using TValue = _TValue;
        using TPointer = typename std::pointer_traits<TNodePtr>::template rebind<TValue>;
        using TConstPointer = typename std::pointer_traits<TNodePtr>::template rebind<const TValue>;
        using TReference = TValue&;
        using TConstReference = const TValue&;

        using TDifferenceType = typename std::pointer_traits<TPointer>::difference_type;

        using TNodeHolder = TNodeHolder<TNode, TTag>;
        using TNodeHolderPtr = typename std::pointer_traits<TNodePtr>::template rebind<TNodeHolder>;
        using TConstNodeHolderPtr = typename std::pointer_traits<TNodePtr>::template rebind<const TNodeHolder>;

        static constexpr bool IsAutoUnlink = _IsAutoUnlink;

        TNodePtr ToNodePtr(TReference value) const noexcept {
            return std::pointer_traits<TNodePtr>::pointer_to(
                static_cast<TNode&>(static_cast<TNodeHolder&>(value))
            );
        }

        TConstNodePtr ToNodePtr(TConstReference value) const noexcept {
            return std::pointer_traits<TConstNodePtr>::pointer_to(
                static_cast<const TNode&>(static_cast<const TNodeHolder&>(value))
            );
        }

        TPointer ToValuePtr(TNodePtr node) const noexcept {
            return TPointerCastTraits<TPointer>::StaticCastFrom(
                TPointerCastTraits<TNodeHolderPtr>::StaticCastFrom(node)
            );
        }

        TConstPointer ToValuePtr(TConstNodePtr node) const noexcept {
            return TPointerCastTraits<TConstPointer>::StaticCastFrom(
                TPointerCastTraits<TConstNodeHolderPtr>::StaticCastFrom(node)
            );
        }
    };

} // namespace NHp::NIntrusive::NPrivate
