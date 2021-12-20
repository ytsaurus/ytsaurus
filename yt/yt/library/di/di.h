#pragma once

#include "public.h"
#include "object_id.h"
#include "cycle_ptr.h"
#include "tagged_ptr.h"

#include <utility>
#include <optional>
#include <vector>
#include <any>

#include <util/generic/string.h>
#include <util/generic/hash.h>

#include <library/cpp/yt/memory/intrusive_ptr.h>
#include <library/cpp/yt/memory/new.h>

namespace NYT::NDI {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(TComponentImpl)

//! TComponent describes part of dependency graph.
/*!
 *  TComponent is a reference type. All methods modify existing object and return self.
 *
 *  Objects within dependency graph are identified by type and optional value of some enum.
 */
class TComponent
{
public:
    TComponent();

    //! Registers T constructor as provider for objectId TIntrusivePtr<T>.
    template <class T>
    TComponent Bind();

    //! Registers T constructor as provider for objectId (TIntrusivePtr<T>, kind).
    template <class T, class EEnum>
    TComponent Bind(EEnum kind);

    //! Registers fn as provider for objectId T.
    template <class T, class... TArgs>
    TComponent Bind(T (*fn)(TArgs...));

    //! Registers fn as provider for objectId (T, kind).
    template <class EEnum, class T, class... TArgs>
    TComponent Bind(EEnum kind, T (*fn)(TArgs...));

    //! Registers method of V as provider for objectId T.
    template <class T, class V>
    TComponent Bind(T (V::*method)() const);

    //! Registers instance as provider for objectId T.
    template <class T>
    TComponent BindInstance(const T& instance);

    //! Registers instance as provider for objectId (T, kind).
    template <class T, class EEnum>
    TComponent BindInstance(EEnum kind, const T& instance);

    //! Merges all providers from other into this component.
    TComponent Install(const TComponent& other);

    //! Registers upcast.
    /**
     *  Whenever instance of TIntrusivePtr<TBase> is required,
     *  injector will try to instantiate TIntrusivePtr<TDerived>.
     *
     *  Upcast preserve object kind. Tagged pointer is casted to
     *  the pointer with the came kind.
     */
    template <class TBase, class TDerived>
    TComponent UpcastFromTo();

    struct TProvider
    {
        TString Name;
        TObjectId Provided;
        std::vector<TObjectId> Required;
        std::vector<TObjectId> CycleRequired;

        bool IsConstructor;
        void* ProviderId = nullptr;

        std::function<void(TInjector*)> Constructor;
        std::function<void(TInjector*)> Initializer;

        operator size_t() const;
        bool operator == (const TProvider& other) const;
        bool HasSameId(const TProvider& other) const;
    };

    struct TTypeCast
    {
        TObjectId From;
        TObjectId To;

        std::function<void(TInjector*, TObjectId, TObjectId)> Cast;

        operator size_t() const;
        bool operator == (const TTypeCast& other) const;
    };

private:
    TComponentImplPtr Impl_;

    void AddProvider(const TProvider& provider);
    void AddTypeCast(const TTypeCast& typecast);

    friend class TInjector;

    std::optional<TProvider> FindProvider(const TObjectId& objectId);
};

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(TInjectorImpl);

//! TInjector takes dependency graph description and instantiates requested objects.
class TInjector
{
public:
    explicit TInjector(const TComponent& component);

    ~TInjector();

    //! NewScope creates new nested injector.
    /*!
     *  Object references in nested injector might get resolved from parent injector.
     *  
     *  We allow shadowing. Both parent and nested components might provide same objectId.
     */
    TInjector NewScope(const TComponent& component);

    template <class T>
    T Get();

    template <class T, class EEnum>
    T Get(EEnum kind);

private:
    TInjectorImplPtr Impl_;

    std::any DoGet(TObjectId objectId);
    void DoSet(TObjectId objectId, std::any value);

    bool CreateObject(TObjectId objectId);

    void SubscribeCreate(TObjectId objectId, std::function<void(std::any)> cb);

    template <class T>
    TCyclePtr<T> GetCycle();

    template <class T>
    void Set(const T& value);

    template <class T, class EEnum>
    void Set(EEnum kind, const T& value);

    std::vector<TString> DumpCycle(TObjectId objectId);

    template <class T>
    friend struct NDetail::TProviderGenerator;

    friend class TComponent;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDI

#define DI_INL_H_
#include "di-inl.h"
#undef DI_INL_H_
