#pragma once

#include "node_holder.h"

#include <memory>

namespace NHp::NIntrusive::NPrivate {

    class TDefaultHookTag;

    class TNotDefaultHook {};

    template <class _TNodeTraits, class _TTag, bool _IsAutoUnlink>
    struct THookTagsImpl {
        using TNodeTraits = _TNodeTraits;
        using TTag = _TTag;
        static constexpr bool IsAutoUnlink = _IsAutoUnlink;
    };

    template <class TNodeAlgo, class TNodeTraits, class TTag, bool IsAutoUnlink>
    class TGenericHook: public TNodeHolder<typename TNodeTraits::TNode, TTag> {
    public:
        using TNode = typename TNodeTraits::TNode;
        using TNodePtr = typename TNodeTraits::TNodePtr;
        using TConstNodePtr = typename TNodeTraits::TConstNodePtr;

        using THookTags = THookTagsImpl<TNodeTraits, TTag, IsAutoUnlink>;

    public:
        TGenericHook() noexcept {
            TNodeAlgo::Init(AsNodePtr());
        }

        TGenericHook(const TGenericHook&) noexcept {
            TNodeAlgo::Init(AsNodePtr());
        }

        TGenericHook& operator=(const TGenericHook&) noexcept {
            return *this;
        }

        ~TGenericHook() {
            if constexpr (IsAutoUnlink) {
                Unlink();
            }
        }

        TNodePtr AsNodePtr() noexcept {
            return std::pointer_traits<TNodePtr>::pointer_to(static_cast<TNode&>(*this));
        }

        TConstNodePtr AsNodePtr() const noexcept {
            return std::pointer_traits<TConstNodePtr>::pointer_to(static_cast<const TNode&>(*this));
        }

        bool IsLinked() const noexcept {
            return TNodeAlgo::IsLinked(AsNodePtr());
        }

        void Unlink() noexcept
        requires IsAutoUnlink
        {
            TNodePtr thisPtr = AsNodePtr();
            if (TNodeAlgo::IsLinked(thisPtr)) {
                TNodeAlgo::Unlink(thisPtr);
                TNodeAlgo::Init(thisPtr);
            }
        }
    };

} // namespace NHp::NIntrusive::NPrivate
