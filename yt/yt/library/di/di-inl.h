#ifndef DI_INL_H_
#error "Direct inclusion of this file is not allowed, include di.h"
// For the sake of sane code completion.
#include "di.h"
#endif

namespace NYT::NDI {

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

////////////////////////////////////////////////////////////////////////////////

template <class T>
constexpr bool IsIntrusivePtr = false;

template <class T>
constexpr bool IsIntrusivePtr<TIntrusivePtr<T>> = true;

template <class T>
constexpr bool IsCyclePtr = false;

template <class T>
constexpr bool IsCyclePtr<TCyclePtr<T>> = true;

template <class T>
constexpr bool IsTaggedPtr = false;

template <class T, class EEnum, EEnum Kind>
constexpr bool IsTaggedPtr<TTaggedPtr<T, Kind>> = true;

template <class T>
struct TProviderGenerator;

template <class TReturnType, class... TArgs>
struct TProviderGenerator<TReturnType(TArgs...)>
{
    template <class T>
    static int FillDepdendency(TComponent::TProvider* provider)
    {
        if constexpr (IsCyclePtr<T>) {
            provider->CycleRequired.push_back(TObjectId::Get<TIntrusivePtr<typename T::TUnderlying>>());
        } else if constexpr (IsTaggedPtr<T>) {
            provider->Required.push_back(TObjectId::Get<TIntrusivePtr<typename T::TUnderlying>>(T::ValueKind));
        } else {
            provider->Required.push_back(TObjectId::Get<std::remove_reference_t<T>>());
        }

        return 0;
    }

    static void FillDependencies(TComponent::TProvider* provider)
    {
        int dummy[] = {
            FillDepdendency<TArgs>(provider)...
        };
        Y_UNUSED(dummy);
    }

    template <class T>
    static auto GetDependency(TInjector* injector)
    {
        if constexpr (IsCyclePtr<T>) {
            return injector->GetCycle<typename std::remove_reference_t<T>::TUnderlying>();
        } else {
            return injector->Get<std::remove_reference_t<T>>();
        }
    }

    template <class... EEnum>
    static TComponent::TProvider NewConstructor(EEnum... kind)
    {
        auto provider = TComponent::TProvider{
            .Name = "constructor",
            .Provided = TObjectId::Get<TIntrusivePtr<TReturnType>>(kind...),
            .IsConstructor = true,
            .Constructor = [kind...] (TInjector* injector) {
                injector->Set(kind..., New<TReturnType>(
                    GetDependency<TArgs>(injector)...
                ));
            }
        };

        FillDependencies(&provider);
        return provider;
    }

    template <class... EEnum>
    static TComponent::TProvider NewFunction(EEnum... kind, TReturnType (*fn)(TArgs...))
    {
        auto provider = TComponent::TProvider{
            .Name = "function",
            .Provided = TObjectId::Get<TReturnType>(),
            .ProviderId = reinterpret_cast<void*>(fn),
            .Constructor = [fn, kind...] (TInjector* injector) {
                injector->Set(kind..., fn(GetDependency<TArgs>(injector)...));
            }
        };

        FillDependencies(&provider);
        return provider;
    }

    template <class... EEnum>
    static TComponent::TProvider NewInstance(EEnum... kind, const TReturnType& instance)
    {
        void* providerId = nullptr;
        if constexpr (IsIntrusivePtr<TReturnType>) {
            providerId = reinterpret_cast<void*>(instance.Get());
        }

        return TComponent::TProvider{
            .Name = "instance",
            .Provided = TObjectId::Get<TReturnType>(),
            .ProviderId = providerId,
            .Constructor = [instance, kind...] (TInjector* injector) {
                injector->Set(kind..., instance);
            }
        };
    }

    template <class V, class... EEnum>
    static TComponent::TProvider NewMethod(EEnum... kind, TReturnType (V::*method)() const)
    {
        return TComponent::TProvider{
            .Name = "method",
            .Provided = TObjectId::Get<TReturnType>(),
            .Constructor = [method, kind...] (TInjector* injector) {
                auto instance = injector->Get<TIntrusivePtr<V>>();

                injector->Set(kind..., (instance.Get()->*method)());
            }
        };        
    }
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDI::NDetail

////////////////////////////////////////////////////////////////////////////////

template <class T>
TComponent TComponent::BindInstance(const T& instance)
{
    AddProvider(NDetail::TProviderGenerator<T()>::NewInstance(instance));
    return *this;
}

template <class T>
TComponent TComponent::Bind()
{
    using TSignature = typename T::TConstructorSignature; // Did you forget to use YT_INJECT?
    AddProvider(NDetail::TProviderGenerator<TSignature>::NewConstructor());
    return *this;
}

template <class T, class EEnum>
TComponent TComponent::Bind(EEnum kind)
{
    using TSignature = typename T::TConstructorSignature; // Did you forget to use YT_INJECT?
    AddProvider(NDetail::TProviderGenerator<TSignature>::NewConstructor(kind));
    return *this;
}

template <class T, class... TArgs>
TComponent TComponent::Bind(T (*fn)(TArgs...))
{
    AddProvider(NDetail::TProviderGenerator<T(TArgs...)>::NewFunction(fn));
    return *this;
}

template <class EEnum, class T, class... TArgs>
TComponent TComponent::Bind(EEnum kind, T (*fn)(TArgs...))
{
    AddProvider(NDetail::TProviderGenerator<T(TArgs...)>::NewFunction(kind, fn));
    return *this;
}

template <class T, class V>
TComponent TComponent::Bind(T (V::*method)() const)
{
    AddProvider(NDetail::TProviderGenerator<T(TIntrusivePtr<V>)>::NewMethod(method));
    return *this;
}

template <class TDerived, class TBase>
TComponent TComponent::UpcastFromTo()
{
    AddTypeCast(TTypeCast{
        .From = TObjectId::Get<TIntrusivePtr<TDerived>>(),
        .To = TObjectId::Get<TIntrusivePtr<TBase>>(),
        .Cast = [] (TInjector* injector, TObjectId from, TObjectId to) {
            auto ptr = std::any_cast<TIntrusivePtr<TDerived>>(injector->DoGet(from));
            injector->DoSet(to, TIntrusivePtr<TBase>{ptr});
        }
    });

    return *this;
}

////////////////////////////////////////////////////////////////////////////////

template <class T>
T TInjector::Get()
{
    static_assert(!std::is_base_of_v<TRefCounted, T>);

    if constexpr (NDetail::IsTaggedPtr<T>) {
        return T{Get<TIntrusivePtr<typename T::TUnderlying>>(T::ValueKind)};
    } else {
        auto typeId = TObjectId::Get<T>();
        return std::any_cast<T>(DoGet(typeId));
    }
}

template <class T, class EEnum>
T TInjector::Get(EEnum kind)
{
    static_assert(!std::is_base_of_v<TRefCounted, T>);

    auto typeId = TObjectId::Get<T>(kind);
    return std::any_cast<T>(DoGet(typeId));
}

template <class T>
void TInjector::Set(const T& value)
{
    static_assert(!std::is_base_of_v<TRefCounted, T>);

    if constexpr (NDetail::IsTaggedPtr<T>) {
        Set<TIntrusivePtr<typename T::TUnderlying>>(T::ValueKind, value);
    } else {
        auto typeId = TObjectId::Get<T>();
        DoSet(typeId, value);
    }
}

template <class T, class EEnum>
void TInjector::Set(EEnum kind, const T& value)
{
    static_assert(!std::is_base_of_v<TRefCounted, T>);

    auto typeId = TObjectId::Get<T>(kind);
    DoSet(typeId, value);
}

template <class T>
TCyclePtr<T> TInjector::GetCycle()
{
    auto objectId = TObjectId::Get<TIntrusivePtr<T>>();

    auto state = New<typename TCyclePtr<T>::TState>();
    SubscribeCreate(objectId, [state] (std::any value) {
        state->Ptr = std::any_cast<TIntrusivePtr<T>>(value);
    });

    return {state};
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDI
