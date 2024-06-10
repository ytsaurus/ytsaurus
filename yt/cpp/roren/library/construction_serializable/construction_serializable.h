#pragma once

#include "helpers.h"

#include <yt/yt/core/actions/bind.h>

#include <library/cpp/yt/memory/intrusive_ptr.h>
#include <library/cpp/yt/memory/new.h>
#include <library/cpp/yt/memory/weak_ptr.h>

#include <library/cpp/yt/misc/guid.h>

#include <library/cpp/yt/string/guid.h>

#include <util/generic/function.h>
#include <util/stream/str.h>
#include <util/ysaveload.h>

#include <any>

namespace NRoren {

////////////////////////////////////////////////////////////////////////////////

//! A shared ptr that memorize serialized construction args.
//! So ptr can be serialized via saveload and transmitted between YT jobs.
template <typename T>
class TConstructionSerializablePtr
{
public:
    using TUnderlying = T;

    constexpr TConstructionSerializablePtr() noexcept = default;

    constexpr TConstructionSerializablePtr(std::nullptr_t) noexcept
    { }

    TConstructionSerializablePtr(const TConstructionSerializablePtr& other) noexcept = default;
    TConstructionSerializablePtr(TConstructionSerializablePtr&& other) noexcept = default;

    ~TConstructionSerializablePtr() = default;

    TConstructionSerializablePtr& operator=(const TConstructionSerializablePtr& other) noexcept = default;
    TConstructionSerializablePtr& operator=(TConstructionSerializablePtr&& other) noexcept = default;

    void Reset()
    {
        Storage_.Reset();
    }

    T* Get() const noexcept
    {
        if (Storage_ == nullptr) {
            return nullptr;
        }

        return Storage_->Ptr ? Storage_->Ptr.Get() : Storage_->WeakPtr.Lock().Get();
    }

    inline T& operator*() const noexcept
    {
        return *Get();
    }

    inline T* operator->() const noexcept
    {
        return Get();
    }

    explicit operator bool() const noexcept
    {
        return static_cast<bool>(Storage_);
    }

    template <auto CreatorFunc, typename ...TArgs>
    static void SaveByCreationArgs(IOutputStream* output, bool global, TArgs... args);

    void Save(IOutputStream* output) const;

    void Load(IInputStream* input);

private:
    using TSelf = TConstructionSerializablePtr<T>;

    struct TStorage final
    {
        NYT::TIntrusivePtr<T> Ptr;
        NYT::TWeakPtr<T> WeakPtr; // For testing purposes only.
        TString SerializedPtr;
    };
    using TStoragePtr = NYT::TIntrusivePtr<TStorage>;

    using TParseAndCreateFunc = NYT::TIntrusivePtr<T>(*)(IInputStream*);

    template <auto CreatorFunc, typename ...TArgs>
    static NYT::TIntrusivePtr<T> ParseAndCreate(IInputStream* input);

    template <typename T2>
    friend TConstructionSerializablePtr<T2> NewFakeConstructionSerializable(NYT::TIntrusivePtr<T2> ptr, bool saveWeakPtr);

    TStoragePtr Storage_;
};

template <typename T, typename ...TArgs>
TConstructionSerializablePtr<T> NewConstructionSerializable(TArgs... args);

//! CreatorFunc is a functor that creates NYT::TIntrusivePtr<T>.
template <auto CreatorFunc, typename ...TArgs>
auto NewConstructionSerializable(TArgs... args) -> TConstructionSerializablePtr<
    typename NPrivate::TUnwrapIntrusivePtr<decltype(CreatorFunc(args...))>::TUnwrapped
>;

//! Generates guid in initial constructing, and uses it to deduplicate instances in loading using global registry.
template <typename T, typename ...TArgs>
TConstructionSerializablePtr<T> NewGlobalConstructionSerializable(TArgs... args);

template <auto CreatorFunc, typename ...TArgs>
auto NewGlobalConstructionSerializable(TArgs... args) -> TConstructionSerializablePtr<
    typename NPrivate::TUnwrapIntrusivePtr<decltype(CreatorFunc(args...))>::TUnwrapped
>;

//! For testing purposes only.
//! This object can not be transfered to other process and parsed there.
template <typename T>
TConstructionSerializablePtr<T> NewFakeConstructionSerializable(NYT::TIntrusivePtr<T> ptr, bool saveWeakPtr = false);

//! TConstructionSerializableCallback is a mix of TConstructionSerializablePtr and std::function / NYT::TCallback.
template <class TSignature>
class TConstructionSerializableCallback;

template <class TRet, class... TArgs>
class TConstructionSerializableCallback<TRet(TArgs...)>
{
public:
    constexpr TConstructionSerializableCallback() noexcept = default;

    constexpr TConstructionSerializableCallback(std::nullptr_t) noexcept
    { }

    template <class... TReceivingArgs, class... TGivenBindingArgs>
    constexpr TConstructionSerializableCallback(TRet(*function)(TReceivingArgs...), TGivenBindingArgs... bindingArgs)
    {
        constexpr auto creator = &TStateMaker<TReceivingArgs...>::template Make<TGivenBindingArgs...>;
        if (function) {
            const ui64 ptr = reinterpret_cast<ui64>(function);
            State_ = NewGlobalConstructionSerializable<creator>(ptr, bindingArgs...);
        }
    }

    //! Make TConstructionSerializableCallback from captureless lambda (or pure function) and serializable binded arguments.
    //! Has deduction guide to use like: `TConstructionSerializableCallback([](int a, int b) { return a + b; }, 40)`.
    template <class TFunctor, class... TGivenBindingArgs>
    constexpr TConstructionSerializableCallback(TFunctor functor, TGivenBindingArgs... bindingArgs)
        : TConstructionSerializableCallback(static_cast<TFunctionSignature<TFunctor>*>(functor), std::forward<TGivenBindingArgs>(bindingArgs)...)
    { }

    TConstructionSerializableCallback(const TConstructionSerializableCallback& other) noexcept = default;
    TConstructionSerializableCallback(TConstructionSerializableCallback&& other) noexcept = default;

    ~TConstructionSerializableCallback() = default;

    TConstructionSerializableCallback& operator=(const TConstructionSerializableCallback& other) noexcept = default;
    TConstructionSerializableCallback& operator=(TConstructionSerializableCallback&& other) noexcept = default;

    TRet operator()(TArgs... args) const
    {
        return State_->Callback(std::forward<TArgs>(args)...);
    }

    explicit operator bool() const
    {
        return static_cast<bool>(State_);
    }

    Y_SAVELOAD_DEFINE(State_);
private:
    struct TState final
    {
        NYT::TCallback<TRet(TArgs...)> Callback;
    };

    template <class... TReceivingArgs>
    struct TStateMaker
    {
        template <class... TGivenBindingArgs>
        static NYT::TIntrusivePtr<TState> Make(ui64 ptr, const TGivenBindingArgs&... bindingArgs)
        {
            const auto function = reinterpret_cast<TRet(*)(TReceivingArgs...)>(reinterpret_cast<void*>(ptr));
            return NYT::New<TState>(TState{
                .Callback = BIND([function, bindingArgs...](TArgs... args) {
                    return function(bindingArgs..., args...);
                }),
            });
        }
    };
private:
    TConstructionSerializablePtr<TState> State_;
};

template <class TFunctor, class... TGivenBindingArgs>
TConstructionSerializableCallback(TFunctor, TGivenBindingArgs...) -> TConstructionSerializableCallback<
    typename NYT::NDetail::TSplit<sizeof...(TGivenBindingArgs), TFunctionSignature<TFunctor>>::TResult
>;

////////////////////////////////////////////////////////////////////////////////

} // namespace NRoren

#define CONSTRUCTION_SERIALIZABLE_INL_H_
#include "construction_serializable-inl.h"
#undef CONSTRUCTION_SERIALIZABLE_INL_H_
