#pragma once

#include "bind_internal.h"
#include "callback_internal.h"

namespace NYT {
/*! \internal */
////////////////////////////////////////////////////////////////////////////////
//
// See "callback.h" for how to use these functions. If reading
// the implementation, before proceeding further, you should read the top
// comment of "bind_internal.h" for a definition of common terms and concepts.
//
// IMPLEMENTATION NOTE
//
// Though #Bind()'s result is meant to be stored in a #TCallback<> type, it
// cannot actually return the exact type without requiring a large amount
// of extra template specializations. The problem is that in order to
// discern the correct specialization of #TCallback<>, #Bind() would need to
// unwrap the function signature to determine the signature's arity, and
// whether or not it is a method.
//
// Each unique combination of (arity, function_type, num_prebound) where
// |function_type| is one of {function, method, const_method} would require
// one specialization. We eventually have to do a similar number of
// specializations anyways in the implementation (see the #TInvoker<>,
// classes). However, it is avoidable in #Bind() if we return the result
// via an indirection like we do below.
//
// It is possible to move most of the compile time asserts into #TBindState<>,
// but it feels a little nicer to have the asserts here so people do not
// need to crack open "bind_internal.h". On the other hand, it makes #Bind()
// harder to read.
//

template <
#ifdef YT_ENABLE_BIND_LOCATION_TRACKING
    class TTag,
    int Counter,
#endif
    class TFunctor,
    class... TAs>
TCallback<
    typename NYT::NDetail::TBindState<
        typename NYT::NDetail::TFunctorTraits<TFunctor>::TRunnable,
        typename NYT::NDetail::TFunctorTraits<TFunctor>::TSignature,
        void(typename NMpl::TDecay<TAs>::TType...)
    >::TUnboundSignature>
Bind(
#ifdef YT_ENABLE_BIND_LOCATION_TRACKING
    const TSourceLocation& location,
#endif
    TFunctor functor,
    TAs&&... args)
{
    // Typedefs for how to store and run the functor.
    typedef NYT::NDetail::TFunctorTraits<TFunctor> TFunctorTraits;
    typedef typename TFunctorTraits::TRunnable TRunnable;
    typedef typename TFunctorTraits::TSignature TSignature;

    // Use TRunnable::TSignature instead of TSignature above because our
    // checks should below for bound references need to know what the actual
    // functor is going to interpret the argument as.

    // Do not allow binding a non-const reference parameter. Binding a
    // non-const reference parameter can make for subtle bugs because the
    // invoked function will receive a reference to the stored copy of the
    // argument and not the original.
    //
    // Do not allow binding a raw pointer parameter for a reference-counted
    // type.
    // Binding a raw pointer can result in invocation with dead parameters,
    // because #TBindState do not hold references to parameters.

    typedef NYT::NDetail::TCheckFirstArgument<TRunnable, TAs...> TCheckFirstArgument;
    typedef NYT::NDetail::TCheckReferencesInSignature<typename TRunnable::TSignature> TCheckReferencesInSignature;
    typedef NYT::NMpl::TTypesPack<NYT::NDetail::TCheckIsRawPtrToRefCountedTypeHelper<TAs>...> TCheckParamsIsRawPtrToRefCountedType;

    typedef NYT::NDetail::TBindState<
        TRunnable,
        TSignature,
        void(typename NMpl::TDecay<TAs>::TType...)
        > TTypedBindState;

    return TCallback<typename TTypedBindState::TUnboundSignature>(
#ifdef YT_ENABLE_BIND_LOCATION_TRACKING
        NewWithLocation<TTypedBindState, TTag, Counter>(
            location,
            location,
            NYT::NDetail::MakeRunnable(functor),
            std::forward<TAs>(args)...)
#else
        New<TTypedBindState>(
            NYT::NDetail::MakeRunnable(functor),
            std::forward<TAs>(args)...)
#endif
    );
}

#ifdef YT_ENABLE_BIND_LOCATION_TRACKING
#define BIND(...) ::NYT::Bind<::NYT::TCurrentTranslationUnitTag, __COUNTER__>(FROM_HERE, __VA_ARGS__)
#else
#define BIND(...) ::NYT::Bind(__VA_ARGS__)
#endif

////////////////////////////////////////////////////////////////////////////////
/*! \endinternal */
} // namespace NYT
