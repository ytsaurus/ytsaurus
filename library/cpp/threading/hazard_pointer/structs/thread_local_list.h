#pragma once

#include "active_list.h"

#include <library/cpp/threading/hazard_pointer/intrusive/options.h>
#include <library/cpp/threading/hazard_pointer/intrusive/unordered_set.h>

#include <util/system/yassert.h>

namespace NHp::NStructs {

    namespace NPrivate {

        template <class TFunc, class T>
        struct TCallable;

        template <class TFunc, class TRet, class... TArgs>
        struct TCallable<TFunc, TRet(TArgs...)>
            : std::is_invocable_r<TRet, TFunc, TArgs...> {};

        template <class TFunc, class TRet, class... TArgs>
        struct TCallable<TFunc, TRet(TArgs...) noexcept>
            : std::is_nothrow_invocable_r<TRet, TFunc, TArgs...> {};

        template <class TFunc, class TSig>
        concept CCallable = TCallable<TFunc, TSig>::value;

        using TSetHook = NIntrusive::TUnorderedSetBaseHook<NOptions::TIsAutoUnlink<false>>;

    } // namespace NPrivate

    template <class TValue>
    class TThreadLocalListNode
        : public NPrivate::TSetHook,
          public TActiveListNode<TValue> {
    public:
        TThreadLocalListNode() = default;

        TThreadLocalListNode(const TThreadLocalListNode&);
        TThreadLocalListNode& operator=(const TThreadLocalListNode&);

    private:
        void DoAttach();
        void DoDetach();

    private:
        template <class _TValue>
        friend class TThreadLocalList;

        const void* Key_{nullptr};
    };

    template <class TValue>
    class TThreadLocalList {
        class TThreadLocalOwner;

        using TList = TActiveList<TValue>;

        using TFactorySig = TValue*();
        using TDeleterSig = void(TValue*);

    public:
        using iterator = typename TList::iterator;
        using const_iterator = typename TList::const_iterator;

        using TPointer = TValue*;
        using TReference = TValue&;

        template <NPrivate::CCallable<TFactorySig> TFactory>
        explicit TThreadLocalList(TFactory&& factory);

        template <NPrivate::CCallable<TFactorySig> TFactory, NPrivate::CCallable<TDeleterSig> TDeleter>
        TThreadLocalList(TFactory&& factory, TDeleter&& deleter);

        ~TThreadLocalList();

        TReference GetThreadLocal();

        void AttachThread();
        void DetachThread();

        iterator begin() noexcept;
        iterator end() noexcept;

        const_iterator begin() const noexcept;
        const_iterator end() const noexcept;

    private:
        TPointer FindOrCreate();

    private:
        TList List_{};

        std::function<TFactorySig> Factory_;
        std::function<TDeleterSig> Deleter_;

        inline static thread_local TThreadLocalOwner Owner_{};
        inline static thread_local bool OwnerAlive_{true};
    };

} // namespace NHp::NStructs

#define INCLUDE_THREAD_LOCAL_LIST_INL_H
#include "thread_local_list-inl.h"
#undef INCLUDE_THREAD_LOCAL_LIST_INL_H
