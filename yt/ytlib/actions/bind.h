// WARNING: This file was auto-generated.
// Please, consider incorporating any changes into the generator.

// Generated on Wed Oct 16 13:07:52 2013.


#pragma once

/*
//=============================================================================
// The following code is merely an adaptation of Chromium's
// Binds and Callbacks. Kudos to them.
//
// Original Chromium revision:
//   - git-treeish: 206a2ae8a1ebd2b040753fff7da61bbca117757f
//   - git-svn-id:  svn://svn.chromium.org/chrome/trunk/src@115607
//
// The following modifications were made while adopting the code
// to our cosy codebase.
//   - code style was adapted,
//   - reference counting semantics were altered to match YT's,
//   - rvalue-references were introduced for more efficieny.
//
// The comments are mainly by courtesy of Chromium authors.
//=============================================================================
*/

#include "bind_internal.h"
#include "callback_internal.h"

#ifdef ENABLE_BIND_LOCATION_TRACKING
#define BIND(...) ::NYT::Bind(FROM_HERE, __VA_ARGS__)
#else
#define BIND(...) ::NYT::Bind(__VA_ARGS__)
#endif

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

template <class Functor>
TCallback<
    typename NYT::NDetail::TBindState<
        typename NYT::NDetail::TFunctorTraits<Functor>::TRunnableType,
        typename NYT::NDetail::TFunctorTraits<Functor>::Signature,
        void()
    >::UnboundSignature>
Bind(
#ifdef ENABLE_BIND_LOCATION_TRACKING
    const TSourceLocation& location,
#endif
    Functor functor) {

    // Typedefs for how to store and run the functor.
    typedef NYT::NDetail::TFunctorTraits<Functor> TFunctorTraits;
    typedef typename TFunctorTraits::TRunnableType TRunnableType;
    typedef typename TFunctorTraits::Signature Signature;

    // Use TRunnableType::Signature instead of Signature above because our
    // checks should below for bound references need to know what the actual
    // functor is going to interpret the argument as.
    typedef NYT::NDetail::TSignatureTraits<typename TRunnableType::Signature>
        TBoundSignatureTraits;

    typedef NYT::NDetail::TBindState<TRunnableType, Signature,
        void()> TTypedBindState;
    return TCallback<typename TTypedBindState::UnboundSignature>(
        New<TTypedBindState>(
#ifdef ENABLE_BIND_LOCATION_TRACKING
            location,
#endif
            NYT::NDetail::MakeRunnable(functor)));
}

template <class Functor, class P1>
TCallback<
    typename NYT::NDetail::TBindState<
        typename NYT::NDetail::TFunctorTraits<Functor>::TRunnableType,
        typename NYT::NDetail::TFunctorTraits<Functor>::Signature,
        void(typename NMpl::TDecay<P1>::TType)
    >::UnboundSignature>
Bind(
#ifdef ENABLE_BIND_LOCATION_TRACKING
    const TSourceLocation& location,
#endif
    Functor functor, P1&& p1) {

    // Typedefs for how to store and run the functor.
    typedef NYT::NDetail::TFunctorTraits<Functor> TFunctorTraits;
    typedef typename TFunctorTraits::TRunnableType TRunnableType;
    typedef typename TFunctorTraits::Signature Signature;

    // Use TRunnableType::Signature instead of Signature above because our
    // checks should below for bound references need to know what the actual
    // functor is going to interpret the argument as.
    typedef NYT::NDetail::TSignatureTraits<typename TRunnableType::Signature>
        TBoundSignatureTraits;

    // Do not allow binding a non-const reference parameter. Binding a
    // non-const reference parameter can make for subtle bugs because the
    // invoked function will receive a reference to the stored copy of the
    // argument and not the original.
    //
    // Do not allow binding a raw pointer parameter for a reference-counted
    // type.
    // Binding a raw pointer can result in invocation with dead parameters,
    // because #TBindState do not hold references to parameters.
    static_assert(!(
        NYT::NDetail::TIsMethodHelper<TRunnableType>::Value &&
        NMpl::TIsArray<P1>::Value),
        "First bound argument to a method cannot be an array");

    static_assert(
        !NYT::NDetail::TIsNonConstReference<typename
            TBoundSignatureTraits::A1Type>::Value,
        "p1 is a non-const reference and should not be bound.");
    static_assert(
        !NYT::NDetail::TRawPtrToRefCountedTypeHelper<P1>::Value,
        "p1 has reference-counted type and should not be bound \
by the raw pointer");

    typedef NYT::NDetail::TBindState<TRunnableType, Signature,
        void(typename NMpl::TDecay<P1>::TType)> TTypedBindState;
    return TCallback<typename TTypedBindState::UnboundSignature>(
        New<TTypedBindState>(
#ifdef ENABLE_BIND_LOCATION_TRACKING
            location,
#endif
            NYT::NDetail::MakeRunnable(functor), std::forward<P1>(p1)));
}

template <class Functor, class P1, class P2>
TCallback<
    typename NYT::NDetail::TBindState<
        typename NYT::NDetail::TFunctorTraits<Functor>::TRunnableType,
        typename NYT::NDetail::TFunctorTraits<Functor>::Signature,
        void(typename NMpl::TDecay<P1>::TType, typename NMpl::TDecay<P2>::TType)
    >::UnboundSignature>
Bind(
#ifdef ENABLE_BIND_LOCATION_TRACKING
    const TSourceLocation& location,
#endif
    Functor functor, P1&& p1, P2&& p2) {

    // Typedefs for how to store and run the functor.
    typedef NYT::NDetail::TFunctorTraits<Functor> TFunctorTraits;
    typedef typename TFunctorTraits::TRunnableType TRunnableType;
    typedef typename TFunctorTraits::Signature Signature;

    // Use TRunnableType::Signature instead of Signature above because our
    // checks should below for bound references need to know what the actual
    // functor is going to interpret the argument as.
    typedef NYT::NDetail::TSignatureTraits<typename TRunnableType::Signature>
        TBoundSignatureTraits;

    // Do not allow binding a non-const reference parameter. Binding a
    // non-const reference parameter can make for subtle bugs because the
    // invoked function will receive a reference to the stored copy of the
    // argument and not the original.
    //
    // Do not allow binding a raw pointer parameter for a reference-counted
    // type.
    // Binding a raw pointer can result in invocation with dead parameters,
    // because #TBindState do not hold references to parameters.
    static_assert(!(
        NYT::NDetail::TIsMethodHelper<TRunnableType>::Value &&
        NMpl::TIsArray<P1>::Value),
        "First bound argument to a method cannot be an array");

    static_assert(
        !NYT::NDetail::TIsNonConstReference<typename
            TBoundSignatureTraits::A1Type>::Value,
        "p1 is a non-const reference and should not be bound.");
    static_assert(
        !NYT::NDetail::TRawPtrToRefCountedTypeHelper<P1>::Value,
        "p1 has reference-counted type and should not be bound \
by the raw pointer");

    static_assert(
        !NYT::NDetail::TIsNonConstReference<typename
            TBoundSignatureTraits::A2Type>::Value,
        "p2 is a non-const reference and should not be bound.");
    static_assert(
        !NYT::NDetail::TRawPtrToRefCountedTypeHelper<P2>::Value,
        "p2 has reference-counted type and should not be bound \
by the raw pointer");

    typedef NYT::NDetail::TBindState<TRunnableType, Signature,
        void(typename NMpl::TDecay<P1>::TType,
        typename NMpl::TDecay<P2>::TType)> TTypedBindState;
    return TCallback<typename TTypedBindState::UnboundSignature>(
        New<TTypedBindState>(
#ifdef ENABLE_BIND_LOCATION_TRACKING
            location,
#endif
            NYT::NDetail::MakeRunnable(functor), std::forward<P1>(p1),
                std::forward<P2>(p2)));
}

template <class Functor, class P1, class P2, class P3>
TCallback<
    typename NYT::NDetail::TBindState<
        typename NYT::NDetail::TFunctorTraits<Functor>::TRunnableType,
        typename NYT::NDetail::TFunctorTraits<Functor>::Signature,
        void(typename NMpl::TDecay<P1>::TType,
            typename NMpl::TDecay<P2>::TType, typename NMpl::TDecay<P3>::TType)
    >::UnboundSignature>
Bind(
#ifdef ENABLE_BIND_LOCATION_TRACKING
    const TSourceLocation& location,
#endif
    Functor functor, P1&& p1, P2&& p2, P3&& p3) {

    // Typedefs for how to store and run the functor.
    typedef NYT::NDetail::TFunctorTraits<Functor> TFunctorTraits;
    typedef typename TFunctorTraits::TRunnableType TRunnableType;
    typedef typename TFunctorTraits::Signature Signature;

    // Use TRunnableType::Signature instead of Signature above because our
    // checks should below for bound references need to know what the actual
    // functor is going to interpret the argument as.
    typedef NYT::NDetail::TSignatureTraits<typename TRunnableType::Signature>
        TBoundSignatureTraits;

    // Do not allow binding a non-const reference parameter. Binding a
    // non-const reference parameter can make for subtle bugs because the
    // invoked function will receive a reference to the stored copy of the
    // argument and not the original.
    //
    // Do not allow binding a raw pointer parameter for a reference-counted
    // type.
    // Binding a raw pointer can result in invocation with dead parameters,
    // because #TBindState do not hold references to parameters.
    static_assert(!(
        NYT::NDetail::TIsMethodHelper<TRunnableType>::Value &&
        NMpl::TIsArray<P1>::Value),
        "First bound argument to a method cannot be an array");

    static_assert(
        !NYT::NDetail::TIsNonConstReference<typename
            TBoundSignatureTraits::A1Type>::Value,
        "p1 is a non-const reference and should not be bound.");
    static_assert(
        !NYT::NDetail::TRawPtrToRefCountedTypeHelper<P1>::Value,
        "p1 has reference-counted type and should not be bound \
by the raw pointer");

    static_assert(
        !NYT::NDetail::TIsNonConstReference<typename
            TBoundSignatureTraits::A2Type>::Value,
        "p2 is a non-const reference and should not be bound.");
    static_assert(
        !NYT::NDetail::TRawPtrToRefCountedTypeHelper<P2>::Value,
        "p2 has reference-counted type and should not be bound \
by the raw pointer");

    static_assert(
        !NYT::NDetail::TIsNonConstReference<typename
            TBoundSignatureTraits::A3Type>::Value,
        "p3 is a non-const reference and should not be bound.");
    static_assert(
        !NYT::NDetail::TRawPtrToRefCountedTypeHelper<P3>::Value,
        "p3 has reference-counted type and should not be bound \
by the raw pointer");

    typedef NYT::NDetail::TBindState<TRunnableType, Signature,
        void(typename NMpl::TDecay<P1>::TType,
        typename NMpl::TDecay<P2>::TType,
        typename NMpl::TDecay<P3>::TType)> TTypedBindState;
    return TCallback<typename TTypedBindState::UnboundSignature>(
        New<TTypedBindState>(
#ifdef ENABLE_BIND_LOCATION_TRACKING
            location,
#endif
            NYT::NDetail::MakeRunnable(functor), std::forward<P1>(p1),
                std::forward<P2>(p2), std::forward<P3>(p3)));
}

template <class Functor, class P1, class P2, class P3, class P4>
TCallback<
    typename NYT::NDetail::TBindState<
        typename NYT::NDetail::TFunctorTraits<Functor>::TRunnableType,
        typename NYT::NDetail::TFunctorTraits<Functor>::Signature,
        void(typename NMpl::TDecay<P1>::TType,
            typename NMpl::TDecay<P2>::TType, typename NMpl::TDecay<P3>::TType,
            typename NMpl::TDecay<P4>::TType)
    >::UnboundSignature>
Bind(
#ifdef ENABLE_BIND_LOCATION_TRACKING
    const TSourceLocation& location,
#endif
    Functor functor, P1&& p1, P2&& p2, P3&& p3, P4&& p4) {

    // Typedefs for how to store and run the functor.
    typedef NYT::NDetail::TFunctorTraits<Functor> TFunctorTraits;
    typedef typename TFunctorTraits::TRunnableType TRunnableType;
    typedef typename TFunctorTraits::Signature Signature;

    // Use TRunnableType::Signature instead of Signature above because our
    // checks should below for bound references need to know what the actual
    // functor is going to interpret the argument as.
    typedef NYT::NDetail::TSignatureTraits<typename TRunnableType::Signature>
        TBoundSignatureTraits;

    // Do not allow binding a non-const reference parameter. Binding a
    // non-const reference parameter can make for subtle bugs because the
    // invoked function will receive a reference to the stored copy of the
    // argument and not the original.
    //
    // Do not allow binding a raw pointer parameter for a reference-counted
    // type.
    // Binding a raw pointer can result in invocation with dead parameters,
    // because #TBindState do not hold references to parameters.
    static_assert(!(
        NYT::NDetail::TIsMethodHelper<TRunnableType>::Value &&
        NMpl::TIsArray<P1>::Value),
        "First bound argument to a method cannot be an array");

    static_assert(
        !NYT::NDetail::TIsNonConstReference<typename
            TBoundSignatureTraits::A1Type>::Value,
        "p1 is a non-const reference and should not be bound.");
    static_assert(
        !NYT::NDetail::TRawPtrToRefCountedTypeHelper<P1>::Value,
        "p1 has reference-counted type and should not be bound \
by the raw pointer");

    static_assert(
        !NYT::NDetail::TIsNonConstReference<typename
            TBoundSignatureTraits::A2Type>::Value,
        "p2 is a non-const reference and should not be bound.");
    static_assert(
        !NYT::NDetail::TRawPtrToRefCountedTypeHelper<P2>::Value,
        "p2 has reference-counted type and should not be bound \
by the raw pointer");

    static_assert(
        !NYT::NDetail::TIsNonConstReference<typename
            TBoundSignatureTraits::A3Type>::Value,
        "p3 is a non-const reference and should not be bound.");
    static_assert(
        !NYT::NDetail::TRawPtrToRefCountedTypeHelper<P3>::Value,
        "p3 has reference-counted type and should not be bound \
by the raw pointer");

    static_assert(
        !NYT::NDetail::TIsNonConstReference<typename
            TBoundSignatureTraits::A4Type>::Value,
        "p4 is a non-const reference and should not be bound.");
    static_assert(
        !NYT::NDetail::TRawPtrToRefCountedTypeHelper<P4>::Value,
        "p4 has reference-counted type and should not be bound \
by the raw pointer");

    typedef NYT::NDetail::TBindState<TRunnableType, Signature,
        void(typename NMpl::TDecay<P1>::TType,
        typename NMpl::TDecay<P2>::TType, typename NMpl::TDecay<P3>::TType,
        typename NMpl::TDecay<P4>::TType)> TTypedBindState;
    return TCallback<typename TTypedBindState::UnboundSignature>(
        New<TTypedBindState>(
#ifdef ENABLE_BIND_LOCATION_TRACKING
            location,
#endif
            NYT::NDetail::MakeRunnable(functor), std::forward<P1>(p1),
                std::forward<P2>(p2), std::forward<P3>(p3),
                std::forward<P4>(p4)));
}

template <class Functor, class P1, class P2, class P3, class P4, class P5>
TCallback<
    typename NYT::NDetail::TBindState<
        typename NYT::NDetail::TFunctorTraits<Functor>::TRunnableType,
        typename NYT::NDetail::TFunctorTraits<Functor>::Signature,
        void(typename NMpl::TDecay<P1>::TType,
            typename NMpl::TDecay<P2>::TType, typename NMpl::TDecay<P3>::TType,
            typename NMpl::TDecay<P4>::TType, typename NMpl::TDecay<P5>::TType)
    >::UnboundSignature>
Bind(
#ifdef ENABLE_BIND_LOCATION_TRACKING
    const TSourceLocation& location,
#endif
    Functor functor, P1&& p1, P2&& p2, P3&& p3, P4&& p4, P5&& p5) {

    // Typedefs for how to store and run the functor.
    typedef NYT::NDetail::TFunctorTraits<Functor> TFunctorTraits;
    typedef typename TFunctorTraits::TRunnableType TRunnableType;
    typedef typename TFunctorTraits::Signature Signature;

    // Use TRunnableType::Signature instead of Signature above because our
    // checks should below for bound references need to know what the actual
    // functor is going to interpret the argument as.
    typedef NYT::NDetail::TSignatureTraits<typename TRunnableType::Signature>
        TBoundSignatureTraits;

    // Do not allow binding a non-const reference parameter. Binding a
    // non-const reference parameter can make for subtle bugs because the
    // invoked function will receive a reference to the stored copy of the
    // argument and not the original.
    //
    // Do not allow binding a raw pointer parameter for a reference-counted
    // type.
    // Binding a raw pointer can result in invocation with dead parameters,
    // because #TBindState do not hold references to parameters.
    static_assert(!(
        NYT::NDetail::TIsMethodHelper<TRunnableType>::Value &&
        NMpl::TIsArray<P1>::Value),
        "First bound argument to a method cannot be an array");

    static_assert(
        !NYT::NDetail::TIsNonConstReference<typename
            TBoundSignatureTraits::A1Type>::Value,
        "p1 is a non-const reference and should not be bound.");
    static_assert(
        !NYT::NDetail::TRawPtrToRefCountedTypeHelper<P1>::Value,
        "p1 has reference-counted type and should not be bound \
by the raw pointer");

    static_assert(
        !NYT::NDetail::TIsNonConstReference<typename
            TBoundSignatureTraits::A2Type>::Value,
        "p2 is a non-const reference and should not be bound.");
    static_assert(
        !NYT::NDetail::TRawPtrToRefCountedTypeHelper<P2>::Value,
        "p2 has reference-counted type and should not be bound \
by the raw pointer");

    static_assert(
        !NYT::NDetail::TIsNonConstReference<typename
            TBoundSignatureTraits::A3Type>::Value,
        "p3 is a non-const reference and should not be bound.");
    static_assert(
        !NYT::NDetail::TRawPtrToRefCountedTypeHelper<P3>::Value,
        "p3 has reference-counted type and should not be bound \
by the raw pointer");

    static_assert(
        !NYT::NDetail::TIsNonConstReference<typename
            TBoundSignatureTraits::A4Type>::Value,
        "p4 is a non-const reference and should not be bound.");
    static_assert(
        !NYT::NDetail::TRawPtrToRefCountedTypeHelper<P4>::Value,
        "p4 has reference-counted type and should not be bound \
by the raw pointer");

    static_assert(
        !NYT::NDetail::TIsNonConstReference<typename
            TBoundSignatureTraits::A5Type>::Value,
        "p5 is a non-const reference and should not be bound.");
    static_assert(
        !NYT::NDetail::TRawPtrToRefCountedTypeHelper<P5>::Value,
        "p5 has reference-counted type and should not be bound \
by the raw pointer");

    typedef NYT::NDetail::TBindState<TRunnableType, Signature,
        void(typename NMpl::TDecay<P1>::TType,
        typename NMpl::TDecay<P2>::TType, typename NMpl::TDecay<P3>::TType,
        typename NMpl::TDecay<P4>::TType,
        typename NMpl::TDecay<P5>::TType)> TTypedBindState;
    return TCallback<typename TTypedBindState::UnboundSignature>(
        New<TTypedBindState>(
#ifdef ENABLE_BIND_LOCATION_TRACKING
            location,
#endif
            NYT::NDetail::MakeRunnable(functor), std::forward<P1>(p1),
                std::forward<P2>(p2), std::forward<P3>(p3),
                std::forward<P4>(p4), std::forward<P5>(p5)));
}

template <class Functor, class P1, class P2, class P3, class P4, class P5,
    class P6>
TCallback<
    typename NYT::NDetail::TBindState<
        typename NYT::NDetail::TFunctorTraits<Functor>::TRunnableType,
        typename NYT::NDetail::TFunctorTraits<Functor>::Signature,
        void(typename NMpl::TDecay<P1>::TType,
            typename NMpl::TDecay<P2>::TType, typename NMpl::TDecay<P3>::TType,
            typename NMpl::TDecay<P4>::TType, typename NMpl::TDecay<P5>::TType,
            typename NMpl::TDecay<P6>::TType)
    >::UnboundSignature>
Bind(
#ifdef ENABLE_BIND_LOCATION_TRACKING
    const TSourceLocation& location,
#endif
    Functor functor, P1&& p1, P2&& p2, P3&& p3, P4&& p4, P5&& p5, P6&& p6) {

    // Typedefs for how to store and run the functor.
    typedef NYT::NDetail::TFunctorTraits<Functor> TFunctorTraits;
    typedef typename TFunctorTraits::TRunnableType TRunnableType;
    typedef typename TFunctorTraits::Signature Signature;

    // Use TRunnableType::Signature instead of Signature above because our
    // checks should below for bound references need to know what the actual
    // functor is going to interpret the argument as.
    typedef NYT::NDetail::TSignatureTraits<typename TRunnableType::Signature>
        TBoundSignatureTraits;

    // Do not allow binding a non-const reference parameter. Binding a
    // non-const reference parameter can make for subtle bugs because the
    // invoked function will receive a reference to the stored copy of the
    // argument and not the original.
    //
    // Do not allow binding a raw pointer parameter for a reference-counted
    // type.
    // Binding a raw pointer can result in invocation with dead parameters,
    // because #TBindState do not hold references to parameters.
    static_assert(!(
        NYT::NDetail::TIsMethodHelper<TRunnableType>::Value &&
        NMpl::TIsArray<P1>::Value),
        "First bound argument to a method cannot be an array");

    static_assert(
        !NYT::NDetail::TIsNonConstReference<typename
            TBoundSignatureTraits::A1Type>::Value,
        "p1 is a non-const reference and should not be bound.");
    static_assert(
        !NYT::NDetail::TRawPtrToRefCountedTypeHelper<P1>::Value,
        "p1 has reference-counted type and should not be bound \
by the raw pointer");

    static_assert(
        !NYT::NDetail::TIsNonConstReference<typename
            TBoundSignatureTraits::A2Type>::Value,
        "p2 is a non-const reference and should not be bound.");
    static_assert(
        !NYT::NDetail::TRawPtrToRefCountedTypeHelper<P2>::Value,
        "p2 has reference-counted type and should not be bound \
by the raw pointer");

    static_assert(
        !NYT::NDetail::TIsNonConstReference<typename
            TBoundSignatureTraits::A3Type>::Value,
        "p3 is a non-const reference and should not be bound.");
    static_assert(
        !NYT::NDetail::TRawPtrToRefCountedTypeHelper<P3>::Value,
        "p3 has reference-counted type and should not be bound \
by the raw pointer");

    static_assert(
        !NYT::NDetail::TIsNonConstReference<typename
            TBoundSignatureTraits::A4Type>::Value,
        "p4 is a non-const reference and should not be bound.");
    static_assert(
        !NYT::NDetail::TRawPtrToRefCountedTypeHelper<P4>::Value,
        "p4 has reference-counted type and should not be bound \
by the raw pointer");

    static_assert(
        !NYT::NDetail::TIsNonConstReference<typename
            TBoundSignatureTraits::A5Type>::Value,
        "p5 is a non-const reference and should not be bound.");
    static_assert(
        !NYT::NDetail::TRawPtrToRefCountedTypeHelper<P5>::Value,
        "p5 has reference-counted type and should not be bound \
by the raw pointer");

    static_assert(
        !NYT::NDetail::TIsNonConstReference<typename
            TBoundSignatureTraits::A6Type>::Value,
        "p6 is a non-const reference and should not be bound.");
    static_assert(
        !NYT::NDetail::TRawPtrToRefCountedTypeHelper<P6>::Value,
        "p6 has reference-counted type and should not be bound \
by the raw pointer");

    typedef NYT::NDetail::TBindState<TRunnableType, Signature,
        void(typename NMpl::TDecay<P1>::TType,
        typename NMpl::TDecay<P2>::TType, typename NMpl::TDecay<P3>::TType,
        typename NMpl::TDecay<P4>::TType, typename NMpl::TDecay<P5>::TType,
        typename NMpl::TDecay<P6>::TType)> TTypedBindState;
    return TCallback<typename TTypedBindState::UnboundSignature>(
        New<TTypedBindState>(
#ifdef ENABLE_BIND_LOCATION_TRACKING
            location,
#endif
            NYT::NDetail::MakeRunnable(functor), std::forward<P1>(p1),
                std::forward<P2>(p2), std::forward<P3>(p3),
                std::forward<P4>(p4), std::forward<P5>(p5),
                std::forward<P6>(p6)));
}

template <class Functor, class P1, class P2, class P3, class P4, class P5,
    class P6, class P7>
TCallback<
    typename NYT::NDetail::TBindState<
        typename NYT::NDetail::TFunctorTraits<Functor>::TRunnableType,
        typename NYT::NDetail::TFunctorTraits<Functor>::Signature,
        void(typename NMpl::TDecay<P1>::TType,
            typename NMpl::TDecay<P2>::TType, typename NMpl::TDecay<P3>::TType,
            typename NMpl::TDecay<P4>::TType, typename NMpl::TDecay<P5>::TType,
            typename NMpl::TDecay<P6>::TType, typename NMpl::TDecay<P7>::TType)
    >::UnboundSignature>
Bind(
#ifdef ENABLE_BIND_LOCATION_TRACKING
    const TSourceLocation& location,
#endif
    Functor functor, P1&& p1, P2&& p2, P3&& p3, P4&& p4, P5&& p5, P6&& p6,
        P7&& p7) {

    // Typedefs for how to store and run the functor.
    typedef NYT::NDetail::TFunctorTraits<Functor> TFunctorTraits;
    typedef typename TFunctorTraits::TRunnableType TRunnableType;
    typedef typename TFunctorTraits::Signature Signature;

    // Use TRunnableType::Signature instead of Signature above because our
    // checks should below for bound references need to know what the actual
    // functor is going to interpret the argument as.
    typedef NYT::NDetail::TSignatureTraits<typename TRunnableType::Signature>
        TBoundSignatureTraits;

    // Do not allow binding a non-const reference parameter. Binding a
    // non-const reference parameter can make for subtle bugs because the
    // invoked function will receive a reference to the stored copy of the
    // argument and not the original.
    //
    // Do not allow binding a raw pointer parameter for a reference-counted
    // type.
    // Binding a raw pointer can result in invocation with dead parameters,
    // because #TBindState do not hold references to parameters.
    static_assert(!(
        NYT::NDetail::TIsMethodHelper<TRunnableType>::Value &&
        NMpl::TIsArray<P1>::Value),
        "First bound argument to a method cannot be an array");

    static_assert(
        !NYT::NDetail::TIsNonConstReference<typename
            TBoundSignatureTraits::A1Type>::Value,
        "p1 is a non-const reference and should not be bound.");
    static_assert(
        !NYT::NDetail::TRawPtrToRefCountedTypeHelper<P1>::Value,
        "p1 has reference-counted type and should not be bound \
by the raw pointer");

    static_assert(
        !NYT::NDetail::TIsNonConstReference<typename
            TBoundSignatureTraits::A2Type>::Value,
        "p2 is a non-const reference and should not be bound.");
    static_assert(
        !NYT::NDetail::TRawPtrToRefCountedTypeHelper<P2>::Value,
        "p2 has reference-counted type and should not be bound \
by the raw pointer");

    static_assert(
        !NYT::NDetail::TIsNonConstReference<typename
            TBoundSignatureTraits::A3Type>::Value,
        "p3 is a non-const reference and should not be bound.");
    static_assert(
        !NYT::NDetail::TRawPtrToRefCountedTypeHelper<P3>::Value,
        "p3 has reference-counted type and should not be bound \
by the raw pointer");

    static_assert(
        !NYT::NDetail::TIsNonConstReference<typename
            TBoundSignatureTraits::A4Type>::Value,
        "p4 is a non-const reference and should not be bound.");
    static_assert(
        !NYT::NDetail::TRawPtrToRefCountedTypeHelper<P4>::Value,
        "p4 has reference-counted type and should not be bound \
by the raw pointer");

    static_assert(
        !NYT::NDetail::TIsNonConstReference<typename
            TBoundSignatureTraits::A5Type>::Value,
        "p5 is a non-const reference and should not be bound.");
    static_assert(
        !NYT::NDetail::TRawPtrToRefCountedTypeHelper<P5>::Value,
        "p5 has reference-counted type and should not be bound \
by the raw pointer");

    static_assert(
        !NYT::NDetail::TIsNonConstReference<typename
            TBoundSignatureTraits::A6Type>::Value,
        "p6 is a non-const reference and should not be bound.");
    static_assert(
        !NYT::NDetail::TRawPtrToRefCountedTypeHelper<P6>::Value,
        "p6 has reference-counted type and should not be bound \
by the raw pointer");

    static_assert(
        !NYT::NDetail::TIsNonConstReference<typename
            TBoundSignatureTraits::A7Type>::Value,
        "p7 is a non-const reference and should not be bound.");
    static_assert(
        !NYT::NDetail::TRawPtrToRefCountedTypeHelper<P7>::Value,
        "p7 has reference-counted type and should not be bound \
by the raw pointer");

    typedef NYT::NDetail::TBindState<TRunnableType, Signature,
        void(typename NMpl::TDecay<P1>::TType,
        typename NMpl::TDecay<P2>::TType, typename NMpl::TDecay<P3>::TType,
        typename NMpl::TDecay<P4>::TType, typename NMpl::TDecay<P5>::TType,
        typename NMpl::TDecay<P6>::TType,
        typename NMpl::TDecay<P7>::TType)> TTypedBindState;
    return TCallback<typename TTypedBindState::UnboundSignature>(
        New<TTypedBindState>(
#ifdef ENABLE_BIND_LOCATION_TRACKING
            location,
#endif
            NYT::NDetail::MakeRunnable(functor), std::forward<P1>(p1),
                std::forward<P2>(p2), std::forward<P3>(p3),
                std::forward<P4>(p4), std::forward<P5>(p5),
                std::forward<P6>(p6), std::forward<P7>(p7)));
}

////////////////////////////////////////////////////////////////////////////////
/*! \endinternal */
} // namespace NYT
