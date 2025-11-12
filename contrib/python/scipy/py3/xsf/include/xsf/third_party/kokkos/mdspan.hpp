#ifndef MDSPAN_SINGLE_HEADER_INCLUDE_GUARD_
#define MDSPAN_SINGLE_HEADER_INCLUDE_GUARD_

// BEGIN_FILE_INCLUDE: mdspan/include/mdspan/mdspan.hpp
//@HEADER
//  ************************************************************************
//
//                         Kokkos v. 4.0
//        Copyright (2022) National Technology & Engineering
//                Solutions of Sandia, LLC (NTESS).
//
//  Under the terms of Contract DE-NA0003525 with NTESS,
//  the U.S. Government retains certain rights in this software.
//
//  Part of Kokkos, under the Apache License v2.0 with LLVM Exceptions.
//  See https://kokkos.org/LICENSE for license information.
//  SPDX-License-Identifier: Apache-2.0 WITH LLVM-exception
//
//@HEADER

#ifndef MDSPAN_HPP_
#define MDSPAN_HPP_

#ifndef MDSPAN_IMPL_STANDARD_NAMESPACE
#define MDSPAN_IMPL_STANDARD_NAMESPACE std
#endif

#ifndef MDSPAN_IMPL_PROPOSED_NAMESPACE
#define MDSPAN_IMPL_PROPOSED_NAMESPACE experimental
#endif

// BEGIN_FILE_INCLUDE: mdspan/include/experimental/__p0009_bits/default_accessor.hpp
//@HEADER
//  ************************************************************************
//
//                         Kokkos v. 4.0
//        Copyright (2022) National Technology & Engineering
//                Solutions of Sandia, LLC (NTESS).
//
//  Under the terms of Contract DE-NA0003525 with NTESS,
//  the U.S. Government retains certain rights in this software.
//
//  Part of Kokkos, under the Apache License v2.0 with LLVM Exceptions.
//  See https://kokkos.org/LICENSE for license information.
//  SPDX-License-Identifier: Apache-2.0 WITH LLVM-exception
//
//@HEADER

// BEGIN_FILE_INCLUDE: mdspan/include/experimental/__p0009_bits/macros.hpp
//@HEADER
//  ************************************************************************
//
//                         Kokkos v. 4.0
//        Copyright (2022) National Technology & Engineering
//                Solutions of Sandia, LLC (NTESS).
//
//  Under the terms of Contract DE-NA0003525 with NTESS,
//  the U.S. Government retains certain rights in this software.
//
//  Part of Kokkos, under the Apache License v2.0 with LLVM Exceptions.
//  See https://kokkos.org/LICENSE for license information.
//  SPDX-License-Identifier: Apache-2.0 WITH LLVM-exception
//
//@HEADER

// BEGIN_FILE_INCLUDE: mdspan/include/experimental/__p0009_bits/config.hpp
//@HEADER
//  ************************************************************************
//
//                         Kokkos v. 4.0
//        Copyright (2022) National Technology & Engineering
//                Solutions of Sandia, LLC (NTESS).
//
//  Under the terms of Contract DE-NA0003525 with NTESS,
//  the U.S. Government retains certain rights in this software.
//
//  Part of Kokkos, under the Apache License v2.0 with LLVM Exceptions.
//  See https://kokkos.org/LICENSE for license information.
//  SPDX-License-Identifier: Apache-2.0 WITH LLVM-exception
//
//@HEADER

#ifndef MDSPAN_IMPL_HAS_INCLUDE
#ifndef __has_include
#define MDSPAN_IMPL_HAS_INCLUDE(x) 0
#else
#define MDSPAN_IMPL_HAS_INCLUDE(x) __has_include(x)
#endif
#endif

#if MDSPAN_IMPL_HAS_INCLUDE(<version>)
#include <version>
#else
#include <type_traits>
#include <utility>
#endif

#ifdef _MSVC_LANG
#define MDSPAN_IMPL_CPLUSPLUS _MSVC_LANG
#else
#define MDSPAN_IMPL_CPLUSPLUS __cplusplus
#endif

#define MDSPAN_CXX_STD_14 201402L
#define MDSPAN_CXX_STD_17 201703L
#define MDSPAN_CXX_STD_20 202002L
// Note GCC has not updated this in version 13
#ifdef __clang__
#define MDSPAN_CXX_STD_23 202302L
#else
#define MDSPAN_CXX_STD_23 202100L
#endif

#define MDSPAN_HAS_CXX_14 (MDSPAN_IMPL_CPLUSPLUS >= MDSPAN_CXX_STD_14)
#define MDSPAN_HAS_CXX_17 (MDSPAN_IMPL_CPLUSPLUS >= MDSPAN_CXX_STD_17)
#define MDSPAN_HAS_CXX_20 (MDSPAN_IMPL_CPLUSPLUS >= MDSPAN_CXX_STD_20)
#define MDSPAN_HAS_CXX_23 (MDSPAN_IMPL_CPLUSPLUS >= MDSPAN_CXX_STD_23)

static_assert(MDSPAN_IMPL_CPLUSPLUS >= MDSPAN_CXX_STD_14, "mdspan requires C++14 or later.");

#ifndef MDSPAN_IMPL_COMPILER_CLANG
#if defined(__clang__)
#define MDSPAN_IMPL_COMPILER_CLANG __clang__
#endif
#endif

#if !defined(MDSPAN_IMPL_COMPILER_MSVC) && !defined(MDSPAN_IMPL_COMPILER_MSVC_CLANG)
#if defined(_MSC_VER)
#if !defined(MDSPAN_IMPL_COMPILER_CLANG)
#define MDSPAN_IMPL_COMPILER_MSVC _MSC_VER
#else
#define MDSPAN_IMPL_COMPILER_MSVC_CLANG _MSC_VER
#endif
#endif
#endif

#ifndef MDSPAN_IMPL_COMPILER_INTEL
#ifdef __INTEL_COMPILER
#define MDSPAN_IMPL_COMPILER_INTEL __INTEL_COMPILER
#endif
#endif

#ifndef MDSPAN_IMPL_COMPILER_APPLECLANG
#ifdef __apple_build_version__
#define MDSPAN_IMPL_COMPILER_APPLECLANG __apple_build_version__
#endif
#endif

#ifndef MDSPAN_IMPL_HAS_CUDA
#if defined(__CUDACC__)
#define MDSPAN_IMPL_HAS_CUDA __CUDACC__
#endif
#endif

#ifndef MDSPAN_IMPL_HAS_HIP
#if defined(__HIPCC__)
#define MDSPAN_IMPL_HAS_HIP __HIPCC__
#endif
#endif

#ifndef MDSPAN_IMPL_HAS_SYCL
#if defined(SYCL_LANGUAGE_VERSION)
#define MDSPAN_IMPL_HAS_SYCL SYCL_LANGUAGE_VERSION
#endif
#endif

#ifndef MDSPAN_IMPL_HAS_CPP_ATTRIBUTE
#ifndef __has_cpp_attribute
#define MDSPAN_IMPL_HAS_CPP_ATTRIBUTE(x) 0
#else
#define MDSPAN_IMPL_HAS_CPP_ATTRIBUTE(x) __has_cpp_attribute(x)
#endif
#endif

#ifndef MDSPAN_IMPL_PRESERVE_STANDARD_LAYOUT
// Preserve standard layout by default, but we're not removing the old version
// that turns this off until we're sure this doesn't have an unreasonable cost
// to the compiler or optimizer.
#define MDSPAN_IMPL_PRESERVE_STANDARD_LAYOUT 1
#endif

#if !defined(MDSPAN_IMPL_USE_ATTRIBUTE_NO_UNIQUE_ADDRESS)
#if ((MDSPAN_IMPL_HAS_CPP_ATTRIBUTE(no_unique_address) >= 201803L) && (!defined(__NVCC__) || MDSPAN_HAS_CXX_20) &&     \
     (!defined(MDSPAN_IMPL_COMPILER_MSVC) || MDSPAN_HAS_CXX_20))
#define MDSPAN_IMPL_USE_ATTRIBUTE_NO_UNIQUE_ADDRESS 1
#define MDSPAN_IMPL_NO_UNIQUE_ADDRESS [[no_unique_address]]
#else
#define MDSPAN_IMPL_NO_UNIQUE_ADDRESS
#endif
#endif

// NVCC older than 11.6 chokes on the no-unique-address-emulation
// so just pretend to use it (to avoid the full blown EBO workaround
// which NVCC also doesn't like ...), and leave the macro empty
#ifndef MDSPAN_IMPL_NO_UNIQUE_ADDRESS
#if defined(__NVCC__)
#define MDSPAN_IMPL_USE_ATTRIBUTE_NO_UNIQUE_ADDRESS 1
#define MDSPAN_IMPL_USE_FAKE_ATTRIBUTE_NO_UNIQUE_ADDRESS
#endif
#define MDSPAN_IMPL_NO_UNIQUE_ADDRESS
#endif

// AMDs HIP compiler seems to have issues with concepts
// it pretends concepts exist, but doesn't ship <concept>
#ifndef __HIPCC__
#ifndef MDSPAN_IMPL_USE_CONCEPTS
#if defined(__cpp_concepts) && __cpp_concepts >= 201507L
#define MDSPAN_IMPL_USE_CONCEPTS 1
#endif
#endif
#endif

#ifndef MDSPAN_IMPL_USE_FOLD_EXPRESSIONS
#if (defined(__cpp_fold_expressions) && __cpp_fold_expressions >= 201603L) ||                                          \
    (!defined(__cpp_fold_expressions) && MDSPAN_HAS_CXX_17)
#define MDSPAN_IMPL_USE_FOLD_EXPRESSIONS 1
#endif
#endif

#ifndef MDSPAN_IMPL_USE_INLINE_VARIABLES
#if defined(__cpp_inline_variables) && __cpp_inline_variables >= 201606L ||                                            \
    (!defined(__cpp_inline_variables) && MDSPAN_HAS_CXX_17)
#define MDSPAN_IMPL_USE_INLINE_VARIABLES 1
#endif
#endif

#ifndef MDSPAN_IMPL_NEEDS_TRAIT_VARIABLE_TEMPLATE_BACKPORTS
#if (!(defined(__cpp_lib_type_trait_variable_templates) && __cpp_lib_type_trait_variable_templates >= 201510L) ||      \
     !MDSPAN_HAS_CXX_17)
#if !(defined(MDSPAN_IMPL_COMPILER_APPLECLANG) && MDSPAN_HAS_CXX_17)
#define MDSPAN_IMPL_NEEDS_TRAIT_VARIABLE_TEMPLATE_BACKPORTS 1
#endif
#endif
#endif

#ifndef MDSPAN_IMPL_USE_VARIABLE_TEMPLATES
#if (defined(__cpp_variable_templates) && __cpp_variable_templates >= 201304 && MDSPAN_HAS_CXX_17) ||                  \
    (!defined(__cpp_variable_templates) && MDSPAN_HAS_CXX_17)
#define MDSPAN_IMPL_USE_VARIABLE_TEMPLATES 1
#endif
#endif // MDSPAN_IMPL_USE_VARIABLE_TEMPLATES

#ifndef MDSPAN_IMPL_USE_CONSTEXPR_14
#if (defined(__cpp_constexpr) && __cpp_constexpr >= 201304) ||                                                         \
    (!defined(__cpp_constexpr) && MDSPAN_HAS_CXX_14) && (!(defined(__INTEL_COMPILER) && __INTEL_COMPILER <= 1700))
#define MDSPAN_IMPL_USE_CONSTEXPR_14 1
#endif
#endif

#ifndef MDSPAN_IMPL_USE_IF_CONSTEXPR_17
#if (defined(__cpp_if_constexpr) && __cpp_if_constexpr >= 201606) || (!defined(__cpp_constexpr) && MDSPAN_HAS_CXX_17)
#define MDSPAN_IMPL_USE_IF_CONSTEXPR_17 1
#endif
#endif

#ifndef MDSPAN_IMPL_USE_INTEGER_SEQUENCE_14
#if defined(MDSPAN_IMPL_COMPILER_MSVC)
#if (defined(__cpp_lib_integer_sequence) && __cpp_lib_integer_sequence >= 201304)
#define MDSPAN_IMPL_USE_INTEGER_SEQUENCE_14 1
#endif
#endif
#endif
#ifndef MDSPAN_IMPL_USE_INTEGER_SEQUENCE_14
#if (defined(__cpp_lib_integer_sequence) && __cpp_lib_integer_sequence >= 201304) ||                                   \
    (!defined(__cpp_lib_integer_sequence) &&                                                                           \
     MDSPAN_HAS_CXX_14) /* as far as I can tell, libc++ seems to think this is a C++11 feature... */                   \
    || (defined(__GLIBCXX__) && __GLIBCXX__ > 20150422 && __GNUC__ < 5 && !defined(__INTEL_CXX11_MODE__))
// several compilers lie about integer_sequence working properly unless the C++14 standard is used
#define MDSPAN_IMPL_USE_INTEGER_SEQUENCE_14 1
#elif defined(MDSPAN_IMPL_COMPILER_APPLECLANG) && MDSPAN_HAS_CXX_14
// appleclang seems to be missing the __cpp_lib_... macros, but doesn't seem to lie about C++14 making
// integer_sequence work
#define MDSPAN_IMPL_USE_INTEGER_SEQUENCE_14 1
#endif
#endif

#ifndef MDSPAN_IMPL_USE_RETURN_TYPE_DEDUCTION
#if (defined(__cpp_return_type_deduction) && __cpp_return_type_deduction >= 201304) ||                                 \
    (!defined(__cpp_return_type_deduction) && MDSPAN_HAS_CXX_14)
#define MDSPAN_IMPL_USE_RETURN_TYPE_DEDUCTION 1
#endif
#endif

#ifndef MDSPAN_IMPL_USE_CLASS_TEMPLATE_ARGUMENT_DEDUCTION
#if (!defined(__NVCC__) || (__CUDACC_VER_MAJOR__ * 100 + __CUDACC_VER_MINOR__ * 10 >= 1170)) &&                        \
    ((defined(__cpp_deduction_guides) && __cpp_deduction_guides >= 201703) ||                                          \
     (!defined(__cpp_deduction_guides) && MDSPAN_HAS_CXX_17))
#define MDSPAN_IMPL_USE_CLASS_TEMPLATE_ARGUMENT_DEDUCTION 1
#endif
#endif

#ifndef MDSPAN_IMPL_USE_STANDARD_TRAIT_ALIASES
#if (defined(__cpp_lib_transformation_trait_aliases) && __cpp_lib_transformation_trait_aliases >= 201304) ||           \
    (!defined(__cpp_lib_transformation_trait_aliases) && MDSPAN_HAS_CXX_14)
#define MDSPAN_IMPL_USE_STANDARD_TRAIT_ALIASES 1
#elif defined(MDSPAN_IMPL_COMPILER_APPLECLANG) && MDSPAN_HAS_CXX_14
// appleclang seems to be missing the __cpp_lib_... macros, but doesn't seem to lie about C++14
#define MDSPAN_IMPL_USE_STANDARD_TRAIT_ALIASES 1
#endif
#endif

#ifndef MDSPAN_IMPL_DEFAULTED_CONSTRUCTORS_INHERITANCE_WORKAROUND
#ifdef __GNUC__
#if __GNUC__ < 9
#define MDSPAN_IMPL_DEFAULTED_CONSTRUCTORS_INHERITANCE_WORKAROUND 1
#endif
#endif
#endif

#ifndef MDSPAN_CONDITIONAL_EXPLICIT
#if MDSPAN_HAS_CXX_20
#define MDSPAN_CONDITIONAL_EXPLICIT(COND) explicit(COND)
#else
#define MDSPAN_CONDITIONAL_EXPLICIT(COND)
#endif
#endif

#ifndef MDSPAN_USE_BRACKET_OPERATOR
#if defined(__cpp_multidimensional_subscript)
// The following if/else is necessary to workaround a clang issue
// relative to using a parameter pack inside a bracket operator in C++2b/C++23 mode
#if defined(MDSPAN_IMPL_COMPILER_CLANG) &&                                                                             \
    ((__clang_major__ < 17) || (__clang_major__ == 17 && __clang_minor__ == 0 && __clang_patchlevel__ == 0))
#define MDSPAN_USE_BRACKET_OPERATOR 0
#else
#define MDSPAN_USE_BRACKET_OPERATOR 1
#endif
#else
#define MDSPAN_USE_BRACKET_OPERATOR 0
#endif
#endif

#ifndef MDSPAN_USE_PAREN_OPERATOR
#if !MDSPAN_USE_BRACKET_OPERATOR
#define MDSPAN_USE_PAREN_OPERATOR 1
#else
#define MDSPAN_USE_PAREN_OPERATOR 0
#endif
#endif

#if MDSPAN_USE_BRACKET_OPERATOR
#define MDSPAN_IMPL_OP(mds, ...) mds[__VA_ARGS__]
// Corentins demo compiler for subscript chokes on empty [] call,
// though I believe the proposal supports it?
#ifdef MDSPAN_NO_EMPTY_BRACKET_OPERATOR
#define MDSPAN_IMPL_OP0(mds) mds.accessor().access(mds.data_handle(), 0)
#else
#define MDSPAN_IMPL_OP0(mds) mds[]
#endif
#define MDSPAN_IMPL_OP1(mds, a) mds[a]
#define MDSPAN_IMPL_OP2(mds, a, b) mds[a, b]
#define MDSPAN_IMPL_OP3(mds, a, b, c) mds[a, b, c]
#define MDSPAN_IMPL_OP4(mds, a, b, c, d) mds[a, b, c, d]
#define MDSPAN_IMPL_OP5(mds, a, b, c, d, e) mds[a, b, c, d, e]
#define MDSPAN_IMPL_OP6(mds, a, b, c, d, e, f) mds[a, b, c, d, e, f]
#else
#define MDSPAN_IMPL_OP(mds, ...) mds(__VA_ARGS__)
#define MDSPAN_IMPL_OP0(mds) mds()
#define MDSPAN_IMPL_OP1(mds, a) mds(a)
#define MDSPAN_IMPL_OP2(mds, a, b) mds(a, b)
#define MDSPAN_IMPL_OP3(mds, a, b, c) mds(a, b, c)
#define MDSPAN_IMPL_OP4(mds, a, b, c, d) mds(a, b, c, d)
#define MDSPAN_IMPL_OP5(mds, a, b, c, d, e) mds(a, b, c, d, e)
#define MDSPAN_IMPL_OP6(mds, a, b, c, d, e, f) mds(a, b, c, d, e, f)
#endif
// END_FILE_INCLUDE: mdspan/include/experimental/__p0009_bits/config.hpp

#include <cstdio>
#include <cstdlib>
#include <type_traits> // std::is_void
#if defined(MDSPAN_IMPL_HAS_CUDA) || defined(MDSPAN_IMPL_HAS_HIP) || defined(MDSPAN_IMPL_HAS_SYCL)
#include "assert.h"
#endif

#ifndef MDSPAN_IMPL_HOST_DEVICE
#if defined(MDSPAN_IMPL_HAS_CUDA) || defined(MDSPAN_IMPL_HAS_HIP)
#define MDSPAN_IMPL_HOST_DEVICE __host__ __device__
#else
#define MDSPAN_IMPL_HOST_DEVICE
#endif
#endif

#ifndef MDSPAN_FORCE_INLINE_FUNCTION
#ifdef MDSPAN_IMPL_COMPILER_MSVC // Microsoft compilers
#define MDSPAN_FORCE_INLINE_FUNCTION __forceinline MDSPAN_IMPL_HOST_DEVICE
#else
#define MDSPAN_FORCE_INLINE_FUNCTION __attribute__((always_inline)) MDSPAN_IMPL_HOST_DEVICE
#endif
#endif

#ifndef MDSPAN_INLINE_FUNCTION
#define MDSPAN_INLINE_FUNCTION inline MDSPAN_IMPL_HOST_DEVICE
#endif

#ifndef MDSPAN_FUNCTION
#define MDSPAN_FUNCTION MDSPAN_IMPL_HOST_DEVICE
#endif

#ifdef MDSPAN_IMPL_HAS_HIP
#define MDSPAN_DEDUCTION_GUIDE MDSPAN_IMPL_HOST_DEVICE
#else
#define MDSPAN_DEDUCTION_GUIDE
#endif

// In CUDA defaulted functions do not need host device markup
#ifndef MDSPAN_INLINE_FUNCTION_DEFAULTED
#define MDSPAN_INLINE_FUNCTION_DEFAULTED
#endif

//==============================================================================
// <editor-fold desc="Preprocessor helpers"> {{{1

#define MDSPAN_PP_COUNT(...) MDSPAN_IMPL_PP_INTERNAL_EXPAND_ARGS(MDSPAN_IMPL_PP_INTERNAL_ARGS_AUGMENTER(__VA_ARGS__))

#define MDSPAN_IMPL_PP_INTERNAL_ARGS_AUGMENTER(...) unused, __VA_ARGS__
#define MDSPAN_IMPL_PP_INTERNAL_EXPAND(x) x
#define MDSPAN_IMPL_PP_INTERNAL_EXPAND_ARGS(...)                                                                       \
    MDSPAN_IMPL_PP_INTERNAL_EXPAND(MDSPAN_IMPL_PP_INTERNAL_COUNT(                                                      \
        __VA_ARGS__, 69, 68, 67, 66, 65, 64, 63, 62, 61, 60, 59, 58, 57, 56, 55, 54, 53, 52, 51, 50, 49, 48, 47, 46,   \
        45, 44, 43, 42, 41, 40, 39, 38, 37, 36, 35, 34, 33, 32, 31, 30, 29, 28, 27, 26, 25, 24, 23, 22, 21, 20, 19,    \
        18, 17, 16, 15, 14, 13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1, 0                                               \
    ))
#define MDSPAN_IMPL_PP_INTERNAL_COUNT(                                                                                 \
    _1_, _2_, _3_, _4_, _5_, _6_, _7_, _8_, _9_, _10, _11, _12, _13, _14, _15, _16, _17, _18, _19, _20, _21, _22, _23, \
    _24, _25, _26, _27, _28, _29, _30, _31, _32, _33, _34, _35, _36, _37, _38, _39, _40, _41, _42, _43, _44, _45, _46, \
    _47, _48, _49, _50, _51, _52, _53, _54, _55, _56, _57, _58, _59, _60, _61, _62, _63, _64, _65, _66, _67, _68, _69, \
    _70, count, ...                                                                                                    \
)                                                                                                                      \
    count /**/

#define MDSPAN_PP_STRINGIFY_IMPL(x) #x
#define MDSPAN_PP_STRINGIFY(x) MDSPAN_PP_STRINGIFY_IMPL(x)

#define MDSPAN_PP_CAT_IMPL(x, y) x##y
#define MDSPAN_PP_CAT(x, y) MDSPAN_PP_CAT_IMPL(x, y)

#define MDSPAN_PP_EVAL(X, ...) X(__VA_ARGS__)

#define MDSPAN_PP_REMOVE_PARENS_IMPL(...) __VA_ARGS__
#define MDSPAN_PP_REMOVE_PARENS(...) MDSPAN_PP_REMOVE_PARENS_IMPL __VA_ARGS__

#define MDSPAN_IMPL_STANDARD_NAMESPACE_STRING MDSPAN_PP_STRINGIFY(MDSPAN_IMPL_STANDARD_NAMESPACE)
#define MDSPAN_IMPL_PROPOSED_NAMESPACE_STRING                                                                          \
    MDSPAN_PP_STRINGIFY(MDSPAN_IMPL_STANDARD_NAMESPACE) "::" MDSPAN_PP_STRINGIFY(MDSPAN_IMPL_PROPOSED_NAMESPACE)

namespace MDSPAN_IMPL_STANDARD_NAMESPACE {
namespace detail {

#if defined(MDSPAN_IMPL_HAS_CUDA) || defined(MDSPAN_IMPL_HAS_HIP)
    MDSPAN_FUNCTION inline void
    default_precondition_violation_handler(const char *cond, const char *file, unsigned line) {
        ::printf("%s:%u: precondition failure: `%s`\n", file, line, cond);
        assert(0);
    }
#elif defined(MDSPAN_IMPL_HAS_SYCL)
    MDSPAN_FUNCTION inline void
    default_precondition_violation_handler(const char *cond, const char *file, unsigned line) {
#ifdef __INTEL_LLVM_COMPILER
        sycl::ext::oneapi::experimental::printf("%s:%u: precondition failure: `%s`\n", file, line, cond);
#else
        (void)cond;
        (void)file;
        (void)line;
#endif
        assert(0);
    }
#else
    MDSPAN_FUNCTION inline void
    default_precondition_violation_handler(const char *cond, const char *file, unsigned line) {
        std::fprintf(stderr, "%s:%u: precondition failure: `%s`\n", file, line, cond);
        std::abort();
    }
#endif

} // namespace detail
} // namespace MDSPAN_IMPL_STANDARD_NAMESPACE

#ifndef MDSPAN_IMPL_PRECONDITION_VIOLATION_HANDLER
#define MDSPAN_IMPL_PRECONDITION_VIOLATION_HANDLER(cond, file, line)                                                   \
    MDSPAN_IMPL_STANDARD_NAMESPACE::detail::default_precondition_violation_handler(cond, file, line)
#endif

#ifndef MDSPAN_IMPL_CHECK_PRECONDITION
#ifndef NDEBUG
#define MDSPAN_IMPL_CHECK_PRECONDITION 0
#else
#define MDSPAN_IMPL_CHECK_PRECONDITION 1
#endif
#endif

namespace MDSPAN_IMPL_STANDARD_NAMESPACE {
namespace detail {

    template <bool check = MDSPAN_IMPL_CHECK_PRECONDITION>
    MDSPAN_FUNCTION constexpr void precondition(const char *cond, const char *file, unsigned line) {
        if (!check) {
            return;
        }
        // in case the macro doesn't use the arguments for custom macros
        (void)cond;
        (void)file;
        (void)line;
        MDSPAN_IMPL_PRECONDITION_VIOLATION_HANDLER(cond, file, line);
    }

} // namespace detail
} // namespace MDSPAN_IMPL_STANDARD_NAMESPACE

#define MDSPAN_IMPL_PRECONDITION(...)                                                                                  \
    do {                                                                                                               \
        if (!(__VA_ARGS__)) {                                                                                          \
            MDSPAN_IMPL_STANDARD_NAMESPACE::detail::precondition(#__VA_ARGS__, __FILE__, __LINE__);                    \
        }                                                                                                              \
    } while (0)

// </editor-fold> end Preprocessor helpers }}}1
//==============================================================================

//==============================================================================
// <editor-fold desc="Concept emulation"> {{{1

// These compatibility macros don't help with partial ordering, but they should do the trick
// for what we need to do with concepts in mdspan
#ifdef MDSPAN_IMPL_USE_CONCEPTS
#define MDSPAN_CLOSE_ANGLE_REQUIRES(REQ) > requires REQ
#define MDSPAN_FUNCTION_REQUIRES(PAREN_PREQUALS, FNAME, PAREN_PARAMS, QUALS, REQ)                                      \
    MDSPAN_PP_REMOVE_PARENS(PAREN_PREQUALS) FNAME PAREN_PARAMS QUALS requires REQ /**/
#else
#define MDSPAN_CLOSE_ANGLE_REQUIRES(REQ) , typename ::std::enable_if<(REQ), int>::type = 0 >
#define MDSPAN_FUNCTION_REQUIRES(PAREN_PREQUALS, FNAME, PAREN_PARAMS, QUALS, REQ)                                      \
    MDSPAN_TEMPLATE_REQUIRES(                                                                                          \
        class function_requires_ignored = void, (std::is_void<function_requires_ignored>::value && REQ)                \
    )                                                                                                                  \
    MDSPAN_PP_REMOVE_PARENS(PAREN_PREQUALS) FNAME PAREN_PARAMS QUALS /**/
#endif

#if defined(MDSPAN_IMPL_COMPILER_MSVC) && (!defined(_MSVC_TRADITIONAL) || _MSVC_TRADITIONAL)
#define MDSPAN_TEMPLATE_REQUIRES(...)                                                                                  \
    MDSPAN_PP_CAT(MDSPAN_PP_CAT(MDSPAN_TEMPLATE_REQUIRES_, MDSPAN_PP_COUNT(__VA_ARGS__))(__VA_ARGS__), )               \
    /**/
#else
#define MDSPAN_TEMPLATE_REQUIRES(...)                                                                                  \
    MDSPAN_PP_EVAL(MDSPAN_PP_CAT(MDSPAN_TEMPLATE_REQUIRES_, MDSPAN_PP_COUNT(__VA_ARGS__)), __VA_ARGS__)                \
    /**/
#endif

#define MDSPAN_TEMPLATE_REQUIRES_2(TP1, REQ) template <TP1 MDSPAN_CLOSE_ANGLE_REQUIRES(REQ)                     /**/
#define MDSPAN_TEMPLATE_REQUIRES_3(TP1, TP2, REQ) template <TP1, TP2 MDSPAN_CLOSE_ANGLE_REQUIRES(REQ)           /**/
#define MDSPAN_TEMPLATE_REQUIRES_4(TP1, TP2, TP3, REQ) template <TP1, TP2, TP3 MDSPAN_CLOSE_ANGLE_REQUIRES(REQ) /**/
#define MDSPAN_TEMPLATE_REQUIRES_5(TP1, TP2, TP3, TP4, REQ)                                                            \
    template <TP1, TP2, TP3, TP4 MDSPAN_CLOSE_ANGLE_REQUIRES(REQ) /**/
#define MDSPAN_TEMPLATE_REQUIRES_6(TP1, TP2, TP3, TP4, TP5, REQ)                                                       \
    template <TP1, TP2, TP3, TP4, TP5 MDSPAN_CLOSE_ANGLE_REQUIRES(REQ) /**/
#define MDSPAN_TEMPLATE_REQUIRES_7(TP1, TP2, TP3, TP4, TP5, TP6, REQ)                                                  \
    template <TP1, TP2, TP3, TP4, TP5, TP6 MDSPAN_CLOSE_ANGLE_REQUIRES(REQ) /**/
#define MDSPAN_TEMPLATE_REQUIRES_8(TP1, TP2, TP3, TP4, TP5, TP6, TP7, REQ)                                             \
    template <TP1, TP2, TP3, TP4, TP5, TP6, TP7 MDSPAN_CLOSE_ANGLE_REQUIRES(REQ) /**/
#define MDSPAN_TEMPLATE_REQUIRES_9(TP1, TP2, TP3, TP4, TP5, TP6, TP7, TP8, REQ)                                        \
    template <TP1, TP2, TP3, TP4, TP5, TP6, TP7, TP8 MDSPAN_CLOSE_ANGLE_REQUIRES(REQ) /**/
#define MDSPAN_TEMPLATE_REQUIRES_10(TP1, TP2, TP3, TP4, TP5, TP6, TP7, TP8, TP9, REQ)                                  \
    template <TP1, TP2, TP3, TP4, TP5, TP6, TP7, TP8, TP9 MDSPAN_CLOSE_ANGLE_REQUIRES(REQ) /**/
#define MDSPAN_TEMPLATE_REQUIRES_11(TP1, TP2, TP3, TP4, TP5, TP6, TP7, TP8, TP9, TP10, REQ)                            \
    template <TP1, TP2, TP3, TP4, TP5, TP6, TP7, TP8, TP9, TP10 MDSPAN_CLOSE_ANGLE_REQUIRES(REQ) /**/
#define MDSPAN_TEMPLATE_REQUIRES_12(TP1, TP2, TP3, TP4, TP5, TP6, TP7, TP8, TP9, TP10, TP11, REQ)                      \
    template <TP1, TP2, TP3, TP4, TP5, TP6, TP7, TP8, TP9, TP10, TP11 MDSPAN_CLOSE_ANGLE_REQUIRES(REQ) /**/
#define MDSPAN_TEMPLATE_REQUIRES_13(TP1, TP2, TP3, TP4, TP5, TP6, TP7, TP8, TP9, TP10, TP11, TP12, REQ)                \
    template <TP1, TP2, TP3, TP4, TP5, TP6, TP7, TP8, TP9, TP10, TP11, TP12 MDSPAN_CLOSE_ANGLE_REQUIRES(REQ) /**/
#define MDSPAN_TEMPLATE_REQUIRES_14(TP1, TP2, TP3, TP4, TP5, TP6, TP7, TP8, TP9, TP10, TP11, TP12, TP13, REQ)          \
    template <TP1, TP2, TP3, TP4, TP5, TP6, TP7, TP8, TP9, TP10, TP11, TP12, TP13 MDSPAN_CLOSE_ANGLE_REQUIRES(REQ) /**/
#define MDSPAN_TEMPLATE_REQUIRES_15(TP1, TP2, TP3, TP4, TP5, TP6, TP7, TP8, TP9, TP10, TP11, TP12, TP13, TP14, REQ)    \
    template <                                                                                                         \
        TP1, TP2, TP3, TP4, TP5, TP6, TP7, TP8, TP9, TP10, TP11, TP12, TP13,                                           \
        TP14 MDSPAN_CLOSE_ANGLE_REQUIRES(REQ) /**/
#define MDSPAN_TEMPLATE_REQUIRES_16(                                                                                   \
    TP1, TP2, TP3, TP4, TP5, TP6, TP7, TP8, TP9, TP10, TP11, TP12, TP13, TP14, TP15, REQ                               \
)                                                                                                                      \
    template <                                                                                                         \
        TP1, TP2, TP3, TP4, TP5, TP6, TP7, TP8, TP9, TP10, TP11, TP12, TP13, TP14,                                     \
        TP15 MDSPAN_CLOSE_ANGLE_REQUIRES(REQ) /**/
#define MDSPAN_TEMPLATE_REQUIRES_17(                                                                                   \
    TP1, TP2, TP3, TP4, TP5, TP6, TP7, TP8, TP9, TP10, TP11, TP12, TP13, TP14, TP15, TP16, REQ                         \
)                                                                                                                      \
    template <                                                                                                         \
        TP1, TP2, TP3, TP4, TP5, TP6, TP7, TP8, TP9, TP10, TP11, TP12, TP13, TP14, TP15,                               \
        TP16 MDSPAN_CLOSE_ANGLE_REQUIRES(REQ) /**/
#define MDSPAN_TEMPLATE_REQUIRES_18(                                                                                   \
    TP1, TP2, TP3, TP4, TP5, TP6, TP7, TP8, TP9, TP10, TP11, TP12, TP13, TP14, TP15, TP16, TP17, REQ                   \
)                                                                                                                      \
    template <                                                                                                         \
        TP1, TP2, TP3, TP4, TP5, TP6, TP7, TP8, TP9, TP10, TP11, TP12, TP13, TP14, TP15, TP16,                         \
        TP17 MDSPAN_CLOSE_ANGLE_REQUIRES(REQ) /**/
#define MDSPAN_TEMPLATE_REQUIRES_19(                                                                                   \
    TP1, TP2, TP3, TP4, TP5, TP6, TP7, TP8, TP9, TP10, TP11, TP12, TP13, TP14, TP15, TP16, TP17, TP18, REQ             \
)                                                                                                                      \
    template <                                                                                                         \
        TP1, TP2, TP3, TP4, TP5, TP6, TP7, TP8, TP9, TP10, TP11, TP12, TP13, TP14, TP15, TP16, TP17,                   \
        TP18 MDSPAN_CLOSE_ANGLE_REQUIRES(REQ) /**/
#define MDSPAN_TEMPLATE_REQUIRES_20(                                                                                   \
    TP1, TP2, TP3, TP4, TP5, TP6, TP7, TP8, TP9, TP10, TP11, TP12, TP13, TP14, TP15, TP16, TP17, TP18, TP19, REQ       \
)                                                                                                                      \
    template <                                                                                                         \
        TP1, TP2, TP3, TP4, TP5, TP6, TP7, TP8, TP9, TP10, TP11, TP12, TP13, TP14, TP15, TP16, TP17, TP18,             \
        TP19 MDSPAN_CLOSE_ANGLE_REQUIRES(REQ) /**/

#define MDSPAN_INSTANTIATE_ONLY_IF_USED                                                                                \
    MDSPAN_TEMPLATE_REQUIRES(                                                                                          \
        class instantiate_only_if_used_tparam = void,                                                                  \
        (MDSPAN_IMPL_TRAIT(std::is_void, instantiate_only_if_used_tparam))                                             \
    )                                                                                                                  \
    /**/

// </editor-fold> end Concept emulation }}}1
//==============================================================================

//==============================================================================
// <editor-fold desc="inline variables"> {{{1

#ifdef MDSPAN_IMPL_USE_INLINE_VARIABLES
#define MDSPAN_IMPL_INLINE_VARIABLE inline
#else
#define MDSPAN_IMPL_INLINE_VARIABLE
#endif

// </editor-fold> end inline variables }}}1
//==============================================================================

//==============================================================================
// <editor-fold desc="Return type deduction"> {{{1

#if MDSPAN_IMPL_USE_RETURN_TYPE_DEDUCTION
#define MDSPAN_IMPL_DEDUCE_RETURN_TYPE_SINGLE_LINE(SIGNATURE, BODY)                                                    \
    auto MDSPAN_PP_REMOVE_PARENS(SIGNATURE) { return MDSPAN_PP_REMOVE_PARENS(BODY); }
#define MDSPAN_IMPL_DEDUCE_DECLTYPE_AUTO_RETURN_TYPE_SINGLE_LINE(SIGNATURE, BODY)                                      \
    decltype(auto) MDSPAN_PP_REMOVE_PARENS(SIGNATURE) { return MDSPAN_PP_REMOVE_PARENS(BODY); }
#else
#define MDSPAN_IMPL_DEDUCE_RETURN_TYPE_SINGLE_LINE(SIGNATURE, BODY)                                                    \
    auto MDSPAN_PP_REMOVE_PARENS(SIGNATURE) -> std::remove_cv_t<std::remove_reference_t<decltype(BODY)>> {             \
        return MDSPAN_PP_REMOVE_PARENS(BODY);                                                                          \
    }
#define MDSPAN_IMPL_DEDUCE_DECLTYPE_AUTO_RETURN_TYPE_SINGLE_LINE(SIGNATURE, BODY)                                      \
    auto MDSPAN_PP_REMOVE_PARENS(SIGNATURE) -> decltype(BODY) { return MDSPAN_PP_REMOVE_PARENS(BODY); }

#endif

// </editor-fold> end Return type deduction }}}1
//==============================================================================

//==============================================================================
// <editor-fold desc="fold expressions"> {{{1

struct enable_fold_comma {};

#ifdef MDSPAN_IMPL_USE_FOLD_EXPRESSIONS
#define MDSPAN_IMPL_FOLD_AND(...) ((__VA_ARGS__) && ...)
#define MDSPAN_IMPL_FOLD_AND_TEMPLATE(...) ((__VA_ARGS__) && ...)
#define MDSPAN_IMPL_FOLD_OR(...) ((__VA_ARGS__) || ...)
#define MDSPAN_IMPL_FOLD_ASSIGN_LEFT(INIT, ...) (INIT = ... = (__VA_ARGS__))
#define MDSPAN_IMPL_FOLD_ASSIGN_RIGHT(PACK, ...) (PACK = ... = (__VA_ARGS__))
#define MDSPAN_IMPL_FOLD_TIMES_RIGHT(PACK, ...) (PACK * ... * (__VA_ARGS__))
#define MDSPAN_IMPL_FOLD_PLUS_RIGHT(PACK, ...) (PACK + ... + (__VA_ARGS__))
#define MDSPAN_IMPL_FOLD_COMMA(...) ((__VA_ARGS__), ...)
#else

namespace MDSPAN_IMPL_STANDARD_NAMESPACE {

namespace fold_compatibility_impl {

    // We could probably be more clever here, but at the (small) risk of losing some compiler understanding.  For the
    // few operations we need, it's not worth generalizing over the operation

#if MDSPAN_IMPL_USE_RETURN_TYPE_DEDUCTION

    MDSPAN_FORCE_INLINE_FUNCTION
    constexpr decltype(auto) fold_right_and_impl() { return true; }

    template <class Arg, class... Args>
    MDSPAN_FORCE_INLINE_FUNCTION constexpr decltype(auto) fold_right_and_impl(Arg &&arg, Args &&...args) {
        return ((Arg &&)arg) && fold_compatibility_impl::fold_right_and_impl((Args &&)args...);
    }

    MDSPAN_FORCE_INLINE_FUNCTION
    constexpr decltype(auto) fold_right_or_impl() { return false; }

    template <class Arg, class... Args>
    MDSPAN_FORCE_INLINE_FUNCTION constexpr auto fold_right_or_impl(Arg &&arg, Args &&...args) {
        return ((Arg &&)arg) || fold_compatibility_impl::fold_right_or_impl((Args &&)args...);
    }

    template <class Arg1>
    MDSPAN_FORCE_INLINE_FUNCTION constexpr auto fold_left_assign_impl(Arg1 &&arg1) {
        return (Arg1 &&)arg1;
    }

    template <class Arg1, class Arg2, class... Args>
    MDSPAN_FORCE_INLINE_FUNCTION constexpr auto fold_left_assign_impl(Arg1 &&arg1, Arg2 &&arg2, Args &&...args) {
        return fold_compatibility_impl::fold_left_assign_impl((((Arg1 &&)arg1) = ((Arg2 &&)arg2)), (Args &&)args...);
    }

    template <class Arg1>
    MDSPAN_FORCE_INLINE_FUNCTION constexpr auto fold_right_assign_impl(Arg1 &&arg1) {
        return (Arg1 &&)arg1;
    }

    template <class Arg1, class Arg2, class... Args>
    MDSPAN_FORCE_INLINE_FUNCTION constexpr auto fold_right_assign_impl(Arg1 &&arg1, Arg2 &&arg2, Args &&...args) {
        return ((Arg1 &&)arg1) = fold_compatibility_impl::fold_right_assign_impl((Arg2 &&)arg2, (Args &&)args...);
    }

    template <class Arg1>
    MDSPAN_FORCE_INLINE_FUNCTION constexpr auto fold_right_plus_impl(Arg1 &&arg1) {
        return (Arg1 &&)arg1;
    }

    template <class Arg1, class Arg2, class... Args>
    MDSPAN_FORCE_INLINE_FUNCTION constexpr auto fold_right_plus_impl(Arg1 &&arg1, Arg2 &&arg2, Args &&...args) {
        return ((Arg1 &&)arg1) + fold_compatibility_impl::fold_right_plus_impl((Arg2 &&)arg2, (Args &&)args...);
    }

    template <class Arg1>
    MDSPAN_FORCE_INLINE_FUNCTION constexpr auto fold_right_times_impl(Arg1 &&arg1) {
        return (Arg1 &&)arg1;
    }

    template <class Arg1, class Arg2, class... Args>
    MDSPAN_FORCE_INLINE_FUNCTION constexpr auto fold_right_times_impl(Arg1 &&arg1, Arg2 &&arg2, Args &&...args) {
        return ((Arg1 &&)arg1) * fold_compatibility_impl::fold_right_times_impl((Arg2 &&)arg2, (Args &&)args...);
    }

#else

    //------------------------------------------------------------------------------
    // <editor-fold desc="right and"> {{{2

    template <class... Args>
    struct fold_right_and_impl_;
    template <>
    struct fold_right_and_impl_<> {
        using rv = bool;
        MDSPAN_FORCE_INLINE_FUNCTION
        static constexpr rv impl() noexcept { return true; }
    };
    template <class Arg, class... Args>
    struct fold_right_and_impl_<Arg, Args...> {
        using next_t = fold_right_and_impl_<Args...>;
        using rv = decltype(std::declval<Arg>() && std::declval<typename next_t::rv>());
        MDSPAN_FORCE_INLINE_FUNCTION
        static constexpr rv impl(Arg &&arg, Args &&...args) noexcept {
            return ((Arg &&)arg) && next_t::impl((Args &&)args...);
        }
    };

    template <class... Args>
    MDSPAN_FORCE_INLINE_FUNCTION constexpr typename fold_right_and_impl_<Args...>::rv
    fold_right_and_impl(Args &&...args) {
        return fold_right_and_impl_<Args...>::impl((Args &&)args...);
    }

    // </editor-fold> end right and }}}2
    //------------------------------------------------------------------------------

    //------------------------------------------------------------------------------
    // <editor-fold desc="right or"> {{{2

    template <class... Args>
    struct fold_right_or_impl_;
    template <>
    struct fold_right_or_impl_<> {
        using rv = bool;
        MDSPAN_FORCE_INLINE_FUNCTION
        static constexpr rv impl() noexcept { return false; }
    };
    template <class Arg, class... Args>
    struct fold_right_or_impl_<Arg, Args...> {
        using next_t = fold_right_or_impl_<Args...>;
        using rv = decltype(std::declval<Arg>() || std::declval<typename next_t::rv>());
        MDSPAN_FORCE_INLINE_FUNCTION
        static constexpr rv impl(Arg &&arg, Args &&...args) noexcept {
            return ((Arg &&)arg) || next_t::impl((Args &&)args...);
        }
    };

    template <class... Args>
    MDSPAN_FORCE_INLINE_FUNCTION constexpr typename fold_right_or_impl_<Args...>::rv
    fold_right_or_impl(Args &&...args) {
        return fold_right_or_impl_<Args...>::impl((Args &&)args...);
    }

    // </editor-fold> end right or }}}2
    //------------------------------------------------------------------------------

    //------------------------------------------------------------------------------
    // <editor-fold desc="right plus"> {{{2

    template <class... Args>
    struct fold_right_plus_impl_;
    template <class Arg>
    struct fold_right_plus_impl_<Arg> {
        using rv = Arg &&;
        MDSPAN_FORCE_INLINE_FUNCTION
        static constexpr rv impl(Arg &&arg) noexcept { return (Arg &&)arg; }
    };
    template <class Arg1, class Arg2, class... Args>
    struct fold_right_plus_impl_<Arg1, Arg2, Args...> {
        using next_t = fold_right_plus_impl_<Arg2, Args...>;
        using rv = decltype(std::declval<Arg1>() + std::declval<typename next_t::rv>());
        MDSPAN_FORCE_INLINE_FUNCTION
        static constexpr rv impl(Arg1 &&arg, Arg2 &&arg2, Args &&...args) noexcept {
            return ((Arg1 &&)arg) + next_t::impl((Arg2 &&)arg2, (Args &&)args...);
        }
    };

    template <class... Args>
    MDSPAN_FORCE_INLINE_FUNCTION constexpr typename fold_right_plus_impl_<Args...>::rv
    fold_right_plus_impl(Args &&...args) {
        return fold_right_plus_impl_<Args...>::impl((Args &&)args...);
    }

    // </editor-fold> end right plus }}}2
    //------------------------------------------------------------------------------

    //------------------------------------------------------------------------------
    // <editor-fold desc="right times"> {{{2

    template <class... Args>
    struct fold_right_times_impl_;
    template <class Arg>
    struct fold_right_times_impl_<Arg> {
        using rv = Arg &&;
        MDSPAN_FORCE_INLINE_FUNCTION
        static constexpr rv impl(Arg &&arg) noexcept { return (Arg &&)arg; }
    };
    template <class Arg1, class Arg2, class... Args>
    struct fold_right_times_impl_<Arg1, Arg2, Args...> {
        using next_t = fold_right_times_impl_<Arg2, Args...>;
        using rv = decltype(std::declval<Arg1>() * std::declval<typename next_t::rv>());
        MDSPAN_FORCE_INLINE_FUNCTION
        static constexpr rv impl(Arg1 &&arg, Arg2 &&arg2, Args &&...args) noexcept {
            return ((Arg1 &&)arg) * next_t::impl((Arg2 &&)arg2, (Args &&)args...);
        }
    };

    template <class... Args>
    MDSPAN_FORCE_INLINE_FUNCTION constexpr typename fold_right_times_impl_<Args...>::rv
    fold_right_times_impl(Args &&...args) {
        return fold_right_times_impl_<Args...>::impl((Args &&)args...);
    }

    // </editor-fold> end right times }}}2
    //------------------------------------------------------------------------------

    //------------------------------------------------------------------------------
    // <editor-fold desc="right assign"> {{{2

    template <class... Args>
    struct fold_right_assign_impl_;
    template <class Arg>
    struct fold_right_assign_impl_<Arg> {
        using rv = Arg &&;
        MDSPAN_FORCE_INLINE_FUNCTION
        static constexpr rv impl(Arg &&arg) noexcept { return (Arg &&)arg; }
    };
    template <class Arg1, class Arg2, class... Args>
    struct fold_right_assign_impl_<Arg1, Arg2, Args...> {
        using next_t = fold_right_assign_impl_<Arg2, Args...>;
        using rv = decltype(std::declval<Arg1>() = std::declval<typename next_t::rv>());
        MDSPAN_FORCE_INLINE_FUNCTION
        static constexpr rv impl(Arg1 &&arg, Arg2 &&arg2, Args &&...args) noexcept {
            return ((Arg1 &&)arg) = next_t::impl((Arg2 &&)arg2, (Args &&)args...);
        }
    };

    template <class... Args>
    MDSPAN_FORCE_INLINE_FUNCTION constexpr typename fold_right_assign_impl_<Args...>::rv
    fold_right_assign_impl(Args &&...args) {
        return fold_right_assign_impl_<Args...>::impl((Args &&)args...);
    }

    // </editor-fold> end right assign }}}2
    //------------------------------------------------------------------------------

    //------------------------------------------------------------------------------
    // <editor-fold desc="left assign"> {{{2

    template <class... Args>
    struct fold_left_assign_impl_;
    template <class Arg>
    struct fold_left_assign_impl_<Arg> {
        using rv = Arg &&;
        MDSPAN_FORCE_INLINE_FUNCTION
        static constexpr rv impl(Arg &&arg) noexcept { return (Arg &&)arg; }
    };
    template <class Arg1, class Arg2, class... Args>
    struct fold_left_assign_impl_<Arg1, Arg2, Args...> {
        using assign_result_t = decltype(std::declval<Arg1>() = std::declval<Arg2>());
        using next_t = fold_left_assign_impl_<assign_result_t, Args...>;
        using rv = typename next_t::rv;
        MDSPAN_FORCE_INLINE_FUNCTION
        static constexpr rv impl(Arg1 &&arg, Arg2 &&arg2, Args &&...args) noexcept {
            return next_t::impl(((Arg1 &&)arg) = (Arg2 &&)arg2, (Args &&)args...);
        }
    };

    template <class... Args>
    MDSPAN_FORCE_INLINE_FUNCTION constexpr typename fold_left_assign_impl_<Args...>::rv
    fold_left_assign_impl(Args &&...args) {
        return fold_left_assign_impl_<Args...>::impl((Args &&)args...);
    }

    // </editor-fold> end left assign }}}2
    //------------------------------------------------------------------------------

#endif

    template <class... Args>
    constexpr enable_fold_comma fold_comma_impl(Args &&...) noexcept {
        return {};
    }

    template <bool... Bs>
    struct fold_bools;

} // namespace fold_compatibility_impl

} // end namespace MDSPAN_IMPL_STANDARD_NAMESPACE

#define MDSPAN_IMPL_FOLD_AND(...)                                                                                      \
    MDSPAN_IMPL_STANDARD_NAMESPACE::fold_compatibility_impl::fold_right_and_impl((__VA_ARGS__)...)
#define MDSPAN_IMPL_FOLD_OR(...)                                                                                       \
    MDSPAN_IMPL_STANDARD_NAMESPACE::fold_compatibility_impl::fold_right_or_impl((__VA_ARGS__)...)
#define MDSPAN_IMPL_FOLD_ASSIGN_LEFT(INIT, ...)                                                                        \
    MDSPAN_IMPL_STANDARD_NAMESPACE::fold_compatibility_impl::fold_left_assign_impl(INIT, (__VA_ARGS__)...)
#define MDSPAN_IMPL_FOLD_ASSIGN_RIGHT(PACK, ...)                                                                       \
    MDSPAN_IMPL_STANDARD_NAMESPACE::fold_compatibility_impl::fold_right_assign_impl((PACK)..., __VA_ARGS__)
#define MDSPAN_IMPL_FOLD_TIMES_RIGHT(PACK, ...)                                                                        \
    MDSPAN_IMPL_STANDARD_NAMESPACE::fold_compatibility_impl::fold_right_times_impl((PACK)..., __VA_ARGS__)
#define MDSPAN_IMPL_FOLD_PLUS_RIGHT(PACK, ...)                                                                         \
    MDSPAN_IMPL_STANDARD_NAMESPACE::fold_compatibility_impl::fold_right_plus_impl((PACK)..., __VA_ARGS__)
#define MDSPAN_IMPL_FOLD_COMMA(...)                                                                                    \
    MDSPAN_IMPL_STANDARD_NAMESPACE::fold_compatibility_impl::fold_comma_impl((__VA_ARGS__)...)

#define MDSPAN_IMPL_FOLD_AND_TEMPLATE(...)                                                                             \
    MDSPAN_IMPL_TRAIT(                                                                                                 \
        std::is_same, fold_compatibility_impl::fold_bools<(__VA_ARGS__)..., true>,                                     \
        fold_compatibility_impl::fold_bools<true, (__VA_ARGS__)...>                                                    \
    )

#endif

// </editor-fold> end fold expressions }}}1
//==============================================================================

//==============================================================================
// <editor-fold desc="Variable template compatibility"> {{{1

#if MDSPAN_IMPL_USE_VARIABLE_TEMPLATES
#define MDSPAN_IMPL_TRAIT(TRAIT, ...) TRAIT##_v<__VA_ARGS__>
#else
#define MDSPAN_IMPL_TRAIT(TRAIT, ...) TRAIT<__VA_ARGS__>::value
#endif

// </editor-fold> end Variable template compatibility }}}1
//==============================================================================

//==============================================================================
// <editor-fold desc="Pre-C++14 constexpr"> {{{1

#if MDSPAN_IMPL_USE_CONSTEXPR_14
#define MDSPAN_IMPL_CONSTEXPR_14 constexpr
// Workaround for a bug (I think?) in EDG frontends
#ifdef __EDG__
#define MDSPAN_IMPL_CONSTEXPR_14_DEFAULTED
#else
#define MDSPAN_IMPL_CONSTEXPR_14_DEFAULTED constexpr
#endif
#else
#define MDSPAN_IMPL_CONSTEXPR_14
#define MDSPAN_IMPL_CONSTEXPR_14_DEFAULTED
#endif

// </editor-fold> end Pre-C++14 constexpr }}}1
//==============================================================================

#if MDSPAN_IMPL_USE_IF_CONSTEXPR_17
#define MDSPAN_IMPL_IF_CONSTEXPR_17 constexpr
#else
#define MDSPAN_IMPL_IF_CONSTEXPR_17
#endif
// END_FILE_INCLUDE: mdspan/include/experimental/__p0009_bits/macros.hpp

#include <cstddef> // size_t

namespace MDSPAN_IMPL_STANDARD_NAMESPACE {

template <class ElementType>
struct default_accessor {

    using offset_policy = default_accessor;
    using element_type = ElementType;
    using reference = ElementType &;
    using data_handle_type = ElementType *;

    MDSPAN_INLINE_FUNCTION_DEFAULTED constexpr default_accessor() noexcept = default;

    MDSPAN_TEMPLATE_REQUIRES(
        class OtherElementType,
        /* requires */ (MDSPAN_IMPL_TRAIT(std::is_convertible, OtherElementType (*)[], element_type (*)[]))
    )
    MDSPAN_INLINE_FUNCTION
    constexpr default_accessor(default_accessor<OtherElementType>) noexcept {}

    MDSPAN_INLINE_FUNCTION
    constexpr data_handle_type offset(data_handle_type p, size_t i) const noexcept { return p + i; }

    MDSPAN_FORCE_INLINE_FUNCTION
    constexpr reference access(data_handle_type p, size_t i) const noexcept { return p[i]; }
};

} // end namespace MDSPAN_IMPL_STANDARD_NAMESPACE
// END_FILE_INCLUDE: mdspan/include/experimental/__p0009_bits/default_accessor.hpp
// BEGIN_FILE_INCLUDE: mdspan/include/experimental/__p0009_bits/full_extent_t.hpp
//@HEADER
//  ************************************************************************
//
//                         Kokkos v. 4.0
//        Copyright (2022) National Technology & Engineering
//                Solutions of Sandia, LLC (NTESS).
//
//  Under the terms of Contract DE-NA0003525 with NTESS,
//  the U.S. Government retains certain rights in this software.
//
//  Part of Kokkos, under the Apache License v2.0 with LLVM Exceptions.
//  See https://kokkos.org/LICENSE for license information.
//  SPDX-License-Identifier: Apache-2.0 WITH LLVM-exception
//
//@HEADER

namespace MDSPAN_IMPL_STANDARD_NAMESPACE {

struct full_extent_t {
    explicit full_extent_t() = default;
};

MDSPAN_IMPL_INLINE_VARIABLE constexpr auto full_extent = full_extent_t{};

} // namespace MDSPAN_IMPL_STANDARD_NAMESPACE
// END_FILE_INCLUDE: mdspan/include/experimental/__p0009_bits/full_extent_t.hpp
// BEGIN_FILE_INCLUDE: mdspan/include/experimental/__p0009_bits/mdspan.hpp
//@HEADER
//  ************************************************************************
//
//                         Kokkos v. 4.0
//        Copyright (2022) National Technology & Engineering
//                Solutions of Sandia, LLC (NTESS).
//
//  Under the terms of Contract DE-NA0003525 with NTESS,
//  the U.S. Government retains certain rights in this software.
//
//  Part of Kokkos, under the Apache License v2.0 with LLVM Exceptions.
//  See https://kokkos.org/LICENSE for license information.
//  SPDX-License-Identifier: Apache-2.0 WITH LLVM-exception
//
//@HEADER

// BEGIN_FILE_INCLUDE: mdspan/include/experimental/__p0009_bits/layout_right.hpp
//@HEADER
//  ************************************************************************
//
//                         Kokkos v. 4.0
//        Copyright (2022) National Technology & Engineering
//                Solutions of Sandia, LLC (NTESS).
//
//  Under the terms of Contract DE-NA0003525 with NTESS,
//  the U.S. Government retains certain rights in this software.
//
//  Part of Kokkos, under the Apache License v2.0 with LLVM Exceptions.
//  See https://kokkos.org/LICENSE for license information.
//  SPDX-License-Identifier: Apache-2.0 WITH LLVM-exception
//
//@HEADER

// BEGIN_FILE_INCLUDE: mdspan/include/experimental/__p0009_bits/trait_backports.hpp
//@HEADER
//  ************************************************************************
//
//                         Kokkos v. 4.0
//        Copyright (2022) National Technology & Engineering
//                Solutions of Sandia, LLC (NTESS).
//
//  Under the terms of Contract DE-NA0003525 with NTESS,
//  the U.S. Government retains certain rights in this software.
//
//  Part of Kokkos, under the Apache License v2.0 with LLVM Exceptions.
//  See https://kokkos.org/LICENSE for license information.
//  SPDX-License-Identifier: Apache-2.0 WITH LLVM-exception
//
//@HEADER
#ifndef MDSPAN_INCLUDE_EXPERIMENTAL_BITS_TRAIT_BACKPORTS_HPP_
#define MDSPAN_INCLUDE_EXPERIMENTAL_BITS_TRAIT_BACKPORTS_HPP_

#include <type_traits>
#include <utility> // integer_sequence

//==============================================================================
// <editor-fold desc="Variable template trait backports (e.g., is_void_v)"> {{{1

#ifdef MDSPAN_IMPL_NEEDS_TRAIT_VARIABLE_TEMPLATE_BACKPORTS

#if MDSPAN_IMPL_USE_VARIABLE_TEMPLATES
namespace MDSPAN_IMPL_STANDARD_NAMESPACE {

#define MDSPAN_IMPL_BACKPORT_TRAIT(TRAIT)                                                                              \
    template <class... Args>                                                                                           \
    MDSPAN_IMPL_INLINE_VARIABLE constexpr auto TRAIT##_v = TRAIT<Args...>::value;

MDSPAN_IMPL_BACKPORT_TRAIT(is_assignable)
MDSPAN_IMPL_BACKPORT_TRAIT(is_constructible)
MDSPAN_IMPL_BACKPORT_TRAIT(is_convertible)
MDSPAN_IMPL_BACKPORT_TRAIT(is_default_constructible)
MDSPAN_IMPL_BACKPORT_TRAIT(is_trivially_destructible)
MDSPAN_IMPL_BACKPORT_TRAIT(is_same)
MDSPAN_IMPL_BACKPORT_TRAIT(is_empty)
MDSPAN_IMPL_BACKPORT_TRAIT(is_void)

#undef MDSPAN_IMPL_BACKPORT_TRAIT

} // end namespace MDSPAN_IMPL_STANDARD_NAMESPACE

#endif // MDSPAN_IMPL_USE_VARIABLE_TEMPLATES

#endif // MDSPAN_IMPL_NEEDS_TRAIT_VARIABLE_TEMPLATE_BACKPORTS

// </editor-fold> end Variable template trait backports (e.g., is_void_v) }}}1
//==============================================================================

//==============================================================================
// <editor-fold desc="integer sequence (ugh...)"> {{{1

#if !defined(MDSPAN_IMPL_USE_INTEGER_SEQUENCE_14) || !MDSPAN_IMPL_USE_INTEGER_SEQUENCE_14

namespace MDSPAN_IMPL_STANDARD_NAMESPACE {

template <class T, T... Vals>
struct integer_sequence {
    static constexpr size_t size() noexcept { return sizeof...(Vals); }
    using value_type = T;
};

template <size_t... Vals>
using index_sequence = std::integer_sequence<size_t, Vals...>;

namespace __detail {

    template <class T, T N, T I, class Result>
    struct __make_int_seq_impl;

    template <class T, T N, T... Vals>
    struct __make_int_seq_impl<T, N, N, integer_sequence<T, Vals...>> {
        using type = integer_sequence<T, Vals...>;
    };

    template <class T, T N, T I, T... Vals>
    struct __make_int_seq_impl<T, N, I, integer_sequence<T, Vals...>>
        : __make_int_seq_impl<T, N, I + 1, integer_sequence<T, Vals..., I>> {};

} // end namespace __detail

template <class T, T N>
using make_integer_sequence = typename __detail::__make_int_seq_impl<T, N, 0, integer_sequence<T>>::type;

template <size_t N>
using make_index_sequence = typename __detail::__make_int_seq_impl<size_t, N, 0, integer_sequence<size_t>>::type;

template <class... T>
using index_sequence_for = make_index_sequence<sizeof...(T)>;

} // end namespace MDSPAN_IMPL_STANDARD_NAMESPACE

#endif

// </editor-fold> end integer sequence (ugh...) }}}1
//==============================================================================

//==============================================================================
// <editor-fold desc="standard trait aliases"> {{{1

#if !defined(MDSPAN_IMPL_USE_STANDARD_TRAIT_ALIASES) || !MDSPAN_IMPL_USE_STANDARD_TRAIT_ALIASES

namespace MDSPAN_IMPL_STANDARD_NAMESPACE {

#define MDSPAN_IMPL_BACKPORT_TRAIT_ALIAS(TRAIT)                                                                        \
    template <class... Args>                                                                                           \
    using TRAIT##_t = typename TRAIT<Args...>::type;

MDSPAN_IMPL_BACKPORT_TRAIT_ALIAS(remove_cv)
MDSPAN_IMPL_BACKPORT_TRAIT_ALIAS(remove_reference)

template <bool _B, class T = void>
using enable_if_t = typename enable_if<_B, T>::type;

#undef MDSPAN_IMPL_BACKPORT_TRAIT_ALIAS

} // end namespace MDSPAN_IMPL_STANDARD_NAMESPACE

#endif

// </editor-fold> end standard trait aliases }}}1
//==============================================================================

#endif // MDSPAN_INCLUDE_EXPERIMENTAL_BITS_TRAIT_BACKPORTS_HPP_
// END_FILE_INCLUDE: mdspan/include/experimental/__p0009_bits/trait_backports.hpp
// BEGIN_FILE_INCLUDE: mdspan/include/experimental/__p0009_bits/extents.hpp
//@HEADER
//  ************************************************************************
//
//                         Kokkos v. 4.0
//        Copyright (2022) National Technology & Engineering
//                Solutions of Sandia, LLC (NTESS).
//
//  Under the terms of Contract DE-NA0003525 with NTESS,
//  the U.S. Government retains certain rights in this software.
//
//  Part of Kokkos, under the Apache License v2.0 with LLVM Exceptions.
//  See https://kokkos.org/LICENSE for license information.
//  SPDX-License-Identifier: Apache-2.0 WITH LLVM-exception
//
//@HEADER

// BEGIN_FILE_INCLUDE: mdspan/include/experimental/__p0009_bits/dynamic_extent.hpp
//@HEADER
//  ************************************************************************
//
//                         Kokkos v. 4.0
//        Copyright (2022) National Technology & Engineering
//                Solutions of Sandia, LLC (NTESS).
//
//  Under the terms of Contract DE-NA0003525 with NTESS,
//  the U.S. Government retains certain rights in this software.
//
//  Part of Kokkos, under the Apache License v2.0 with LLVM Exceptions.
//  See https://kokkos.org/LICENSE for license information.
//  SPDX-License-Identifier: Apache-2.0 WITH LLVM-exception
//
//@HEADER

#if defined(__cpp_lib_span)
#include <span>
#endif

#include <cstddef> // size_t
#include <limits>  // numeric_limits

namespace MDSPAN_IMPL_STANDARD_NAMESPACE {
#if defined(__cpp_lib_span)
using std::dynamic_extent;
#else
MDSPAN_IMPL_INLINE_VARIABLE constexpr auto dynamic_extent = std::numeric_limits<size_t>::max();
#endif
} // namespace MDSPAN_IMPL_STANDARD_NAMESPACE

//==============================================================================================================
// END_FILE_INCLUDE: mdspan/include/experimental/__p0009_bits/dynamic_extent.hpp
// BEGIN_FILE_INCLUDE: mdspan/include/experimental/__p0009_bits/utility.hpp

#include <array>
#include <cstddef>
#include <type_traits>
#include <utility>

namespace MDSPAN_IMPL_STANDARD_NAMESPACE {
namespace detail {

    // type alias used for rank-based tag dispatch
    //
    // this is used to enable alternatives to constexpr if when building for C++14
    //
    template <std::size_t N>
    using with_rank = std::integral_constant<std::size_t, N>;

    template <class I1, class I2>
    MDSPAN_INLINE_FUNCTION constexpr bool common_integral_compare(I1 x, I2 y) {
        static_assert(std::is_integral<I1>::value && std::is_integral<I2>::value, "");

        using I = std::common_type_t<I1, I2>;
        return static_cast<I>(x) == static_cast<I>(y);
    }

    template <class T1, class T2, class F>
    MDSPAN_INLINE_FUNCTION constexpr bool rankwise_equal(with_rank<0>, const T1 &, const T2 &, F) {
        return true;
    }

    template <std::size_t N, class T1, class T2, class F>
    MDSPAN_INLINE_FUNCTION constexpr bool rankwise_equal(with_rank<N>, const T1 &x, const T2 &y, F func) {
        bool match = true;

        for (std::size_t r = 0; r < N; r++) {
            match = match && common_integral_compare(func(x, r), func(y, r));
        }

        return match;
    }

#if MDSPAN_HAS_CXX_17
    inline
#endif
        constexpr struct extent_functor {
        template <class T, class I>
        MDSPAN_INLINE_FUNCTION constexpr auto operator()(const T &x, I i) const {
            return x.extent(i);
        }
    } extent;

#if MDSPAN_HAS_CXX_17
    inline
#endif
        constexpr struct stride_functor {
        template <class T, class I>
        MDSPAN_INLINE_FUNCTION constexpr auto operator()(const T &x, I i) const {
            return x.stride(i);
        }
    } stride;

    // same as std::integral_constant but with __host__ __device__ annotations on
    // the implicit conversion function and the call operator
    template <class T, T v>
    struct integral_constant {
        using value_type = T;
        using type = integral_constant<T, v>;

        static constexpr T value = v;

        MDSPAN_INLINE_FUNCTION_DEFAULTED
        constexpr integral_constant() = default;

        // These interop functions work, because other than the value_type operator
        // everything of std::integral_constant works on device (defaulted functions)
        MDSPAN_FUNCTION
        constexpr integral_constant(std::integral_constant<T, v>) {}

        MDSPAN_FUNCTION constexpr operator std::integral_constant<T, v>() const noexcept {
            return std::integral_constant<T, v>{};
        }

        MDSPAN_FUNCTION constexpr operator value_type() const noexcept { return value; }

        MDSPAN_FUNCTION constexpr value_type operator()() const noexcept { return value; }
    };

// The tuple implementation only comes in play when using capabilities
// such as submdspan which require C++17 anyway
#if MDSPAN_HAS_CXX_17
    template <class T, size_t Idx>
    struct tuple_member {
        using type = T;
        static constexpr size_t idx = Idx;
        T val;
        MDSPAN_FUNCTION constexpr T &get() { return val; }
        MDSPAN_FUNCTION constexpr const T &get() const { return val; }
    };

    // A helper class which will be used via a fold expression to
    // select the type with the correct Idx in a pack of tuple_member
    template <size_t SearchIdx, size_t Idx, class T>
    struct tuple_idx_matcher {
        using type = tuple_member<T, Idx>;
        template <class Other>
        MDSPAN_FUNCTION constexpr auto operator|([[maybe_unused]] Other v) const {
            if constexpr (Idx == SearchIdx) {
                return *this;
            } else {
                return v;
            }
        }
    };

    template <class IdxSeq, class... Elements>
    struct tuple_impl;

    template <size_t... Idx, class... Elements>
    struct tuple_impl<std::index_sequence<Idx...>, Elements...> : public tuple_member<Elements, Idx>... {

        MDSPAN_FUNCTION
        constexpr tuple_impl(Elements... vals) : tuple_member<Elements, Idx>{vals}... {}

        template <size_t N>
        MDSPAN_FUNCTION constexpr auto &get() {
            using base_t = decltype((tuple_idx_matcher<N, Idx, Elements>() | ...));
            return base_t::type::get();
        }
        template <size_t N>
        MDSPAN_FUNCTION constexpr const auto &get() const {
            using base_t = decltype((tuple_idx_matcher<N, Idx, Elements>() | ...));
            return base_t::type::get();
        }
    };

    // A simple tuple-like class for representing slices internally and is compatible with device code
    // This doesn't support type access since we don't need it
    // This is not meant as an external API
    template <class... Elements>
    struct tuple : public tuple_impl<decltype(std::make_index_sequence<sizeof...(Elements)>()), Elements...> {
        MDSPAN_FUNCTION
        constexpr tuple(Elements... vals)
            : tuple_impl<decltype(std::make_index_sequence<sizeof...(Elements)>()), Elements...>(vals...) {}
    };

    template <size_t Idx, class... Args>
    MDSPAN_FUNCTION constexpr auto &get(tuple<Args...> &vals) {
        return vals.template get<Idx>();
    }

    template <size_t Idx, class... Args>
    MDSPAN_FUNCTION constexpr const auto &get(const tuple<Args...> &vals) {
        return vals.template get<Idx>();
    }

    template <class... Elements>
    tuple(Elements...) -> tuple<Elements...>;
#endif

#if MDSPAN_HAS_CXX_17
    // std::in_range and friends, tagged for device execution
    // Backport from https://en.cppreference.com/w/cpp/utility/intcmp
    // and https://en.cppreference.com/w/cpp/utility/in_range
    template <class T, class U>
    MDSPAN_INLINE_FUNCTION constexpr bool cmp_less(T t, U u) noexcept {
        if constexpr (std::is_signed_v<T> == std::is_signed_v<U>)
            return t < u;
        else if constexpr (std::is_signed_v<T>)
            return t < 0 || std::make_unsigned_t<T>(t) < u;
        else
            return u >= 0 && t < std::make_unsigned_t<U>(u);
    }

    template <class T, class U>
    MDSPAN_INLINE_FUNCTION constexpr bool cmp_less_equal(T t, U u) noexcept {
        return !cmp_less(u, t);
    }

    template <class T, class U>
    MDSPAN_INLINE_FUNCTION constexpr bool cmp_greater_equal(T t, U u) noexcept {
        return !cmp_less(t, u);
    }

    template <class R, class T>
    MDSPAN_INLINE_FUNCTION constexpr bool in_range(T t) noexcept {
        return cmp_greater_equal(t, std::numeric_limits<R>::min()) && cmp_less_equal(t, std::numeric_limits<R>::max());
    }

    template <typename T>
    MDSPAN_INLINE_FUNCTION constexpr bool check_mul_result_is_nonnegative_and_representable(T a, T b) {
// FIXME_SYCL The code below compiles to old_llvm.umul.with.overflow.i64
// which isn't defined in device code
#ifdef __SYCL_DEVICE_ONLY__
        return true;
#else
        if (b == 0 || a == 0)
            return true;

        if constexpr (std::is_signed_v<T>) {
            if (a < 0 || b < 0)
                return false;
        }
        return a <= std::numeric_limits<T>::max() / b;
#endif
    }
#endif
} // namespace detail

#if MDSPAN_HAS_CXX_17
inline
#endif
    constexpr struct mdspan_non_standard_tag {
} mdspan_non_standard;

} // namespace MDSPAN_IMPL_STANDARD_NAMESPACE
// END_FILE_INCLUDE: mdspan/include/experimental/__p0009_bits/utility.hpp

#ifdef __cpp_lib_span
#include <span>
#endif
#include <array>
#include <type_traits>

#include <cassert>
#include <cinttypes>

namespace MDSPAN_IMPL_STANDARD_NAMESPACE {
namespace detail {

    // Function used to check compatibility of extents in converting constructor
    // can't be a private member function for some reason.
    template <size_t... Extents, size_t... OtherExtents>
    MDSPAN_INLINE_FUNCTION constexpr std::integral_constant<bool, false> impl_check_compatible_extents(
        std::integral_constant<bool, false>, std::integer_sequence<size_t, Extents...>,
        std::integer_sequence<size_t, OtherExtents...>
    ) noexcept {
        return {};
    }

    // This helper prevents ICE's on MSVC.
    template <size_t Lhs, size_t Rhs>
    struct impl_compare_extent_compatible
        : std::integral_constant<bool, Lhs == dynamic_extent || Rhs == dynamic_extent || Lhs == Rhs> {};

    template <size_t... Extents, size_t... OtherExtents>
    MDSPAN_INLINE_FUNCTION constexpr std::integral_constant<
        bool, MDSPAN_IMPL_FOLD_AND(impl_compare_extent_compatible<Extents, OtherExtents>::value)>
    impl_check_compatible_extents(
        std::integral_constant<bool, true>, std::integer_sequence<size_t, Extents...>,
        std::integer_sequence<size_t, OtherExtents...>
    ) noexcept {
        return {};
    }

    template <class IndexType, class... Arguments>
    MDSPAN_INLINE_FUNCTION constexpr bool are_valid_indices() {
        return MDSPAN_IMPL_FOLD_AND(std::is_convertible<Arguments, IndexType>::value) &&
               MDSPAN_IMPL_FOLD_AND(std::is_nothrow_constructible<IndexType, Arguments>::value);
    }

    // ------------------------------------------------------------------
    // ------------ static_array ----------------------------------------
    // ------------------------------------------------------------------

    // array like class which provides an array of static values with get
    // function and operator [].

    // Implementation of Static Array with recursive implementation of get.
    template <size_t R, class T, T... Extents>
    struct static_array_impl;

    template <size_t R, class T, T FirstExt, T... Extents>
    struct static_array_impl<R, T, FirstExt, Extents...> {
        MDSPAN_INLINE_FUNCTION
        constexpr static T get(size_t r) {
            if (r == R)
                return FirstExt;
            else
                return static_array_impl<R + 1, T, Extents...>::get(r);
        }
        template <size_t r>
        MDSPAN_INLINE_FUNCTION constexpr static T get() {
#if MDSPAN_HAS_CXX_17
            if constexpr (r == R)
                return FirstExt;
            else
                return static_array_impl<R + 1, T, Extents...>::template get<r>();
#else
            get(r);
#endif
        }
    };

    // End the recursion
    template <size_t R, class T, T FirstExt>
    struct static_array_impl<R, T, FirstExt> {
        MDSPAN_INLINE_FUNCTION
        constexpr static T get(size_t) { return FirstExt; }
        template <size_t>
        MDSPAN_INLINE_FUNCTION constexpr static T get() {
            return FirstExt;
        }
    };

    // Don't start recursion if size 0
    template <class T>
    struct static_array_impl<0, T> {
        MDSPAN_INLINE_FUNCTION
        constexpr static T get(size_t) { return T(); }
        template <size_t>
        MDSPAN_INLINE_FUNCTION constexpr static T get() {
            return T();
        }
    };

    // Static array, provides get<r>(), get(r) and operator[r]
    template <class T, T... Values>
    struct static_array : public static_array_impl<0, T, Values...> {

      public:
        using value_type = T;

        MDSPAN_INLINE_FUNCTION
        constexpr static size_t size() { return sizeof...(Values); }
    };

    // ------------------------------------------------------------------
    // ------------ index_sequence_scan ---------------------------------
    // ------------------------------------------------------------------

    // index_sequence_scan takes compile time values and provides get(r)
    // and get<r>() which return the sum of the first r-1 values.

    // Recursive implementation for get
    template <size_t R, size_t... Values>
    struct index_sequence_scan_impl;

    template <size_t R, size_t FirstVal, size_t... Values>
    struct index_sequence_scan_impl<R, FirstVal, Values...> {
        MDSPAN_INLINE_FUNCTION
        constexpr static size_t get(size_t r) {
            if (r > R)
                return FirstVal + index_sequence_scan_impl<R + 1, Values...>::get(r);
            else
                return 0;
        }
    };

    template <size_t R, size_t FirstVal>
    struct index_sequence_scan_impl<R, FirstVal> {
#if defined(__NVCC__) || defined(__NVCOMPILER) || defined(MDSPAN_IMPL_COMPILER_INTEL)
        // NVCC warns about pointless comparison with 0 for R==0 and r being const
        // evaluatable and also 0.
        MDSPAN_INLINE_FUNCTION
        constexpr static size_t get(size_t r) {
            return static_cast<int64_t>(R) > static_cast<int64_t>(r) ? FirstVal : 0;
        }
#else
        MDSPAN_INLINE_FUNCTION
        constexpr static size_t get(size_t r) { return R > r ? FirstVal : 0; }
#endif
    };
    template <>
    struct index_sequence_scan_impl<0> {
        MDSPAN_INLINE_FUNCTION
        constexpr static size_t get(size_t) { return 0; }
    };

    // ------------------------------------------------------------------
    // ------------ possibly_empty_array  -------------------------------
    // ------------------------------------------------------------------

    // array like class which provides get function and operator [], and
    // has a specialization for the size 0 case.
    // This is needed to make the maybe_static_array be truly empty, for
    // all static values.

    template <class T, size_t N>
    struct possibly_empty_array {
        T vals[N]{};
        MDSPAN_INLINE_FUNCTION
        constexpr T &operator[](size_t r) { return vals[r]; }
        MDSPAN_INLINE_FUNCTION
        constexpr const T &operator[](size_t r) const { return vals[r]; }
    };

    template <class T>
    struct possibly_empty_array<T, 0> {
        MDSPAN_INLINE_FUNCTION
        constexpr T operator[](size_t) { return T(); }
        MDSPAN_INLINE_FUNCTION
        constexpr const T operator[](size_t) const { return T(); }
    };

    // ------------------------------------------------------------------
    // ------------ maybe_static_array ----------------------------------
    // ------------------------------------------------------------------

    // array like class which has a mix of static and runtime values but
    // only stores the runtime values.
    // The type of the static and the runtime values can be different.
    // The position of a dynamic value is indicated through a tag value.
    template <class TDynamic, class TStatic, TStatic dyn_tag, TStatic... Values>
    struct maybe_static_array {

        static_assert(
            std::is_convertible<TStatic, TDynamic>::value, "maybe_static_array: TStatic must be convertible to TDynamic"
        );
        static_assert(
            std::is_convertible<TDynamic, TStatic>::value, "maybe_static_array: TDynamic must be convertible to TStatic"
        );

      private:
        // Static values member
        using static_vals_t = static_array<TStatic, Values...>;
        constexpr static size_t m_size = sizeof...(Values);
        constexpr static size_t m_size_dynamic = MDSPAN_IMPL_FOLD_PLUS_RIGHT((Values == dyn_tag), 0);

        // Dynamic values member
        MDSPAN_IMPL_NO_UNIQUE_ADDRESS possibly_empty_array<TDynamic, m_size_dynamic> m_dyn_vals;

        // static mapping of indices to the position in the dynamic values array
        using dyn_map_t = index_sequence_scan_impl<0, static_cast<size_t>(Values == dyn_tag)...>;

      public:
        // two types for static and dynamic values
        using value_type = TDynamic;
        using static_value_type = TStatic;
        // tag value indicating dynamic value
        constexpr static static_value_type tag_value = dyn_tag;

        constexpr maybe_static_array() = default;

        // constructor for all static values
        // TODO: add precondition check?
        MDSPAN_TEMPLATE_REQUIRES(
            class... Vals,
            /* requires */ ((m_size_dynamic == 0) && (sizeof...(Vals) > 0))
        )
        MDSPAN_INLINE_FUNCTION
        constexpr maybe_static_array(Vals...) : m_dyn_vals{} {}

        // constructors from dynamic values only
        MDSPAN_TEMPLATE_REQUIRES(
            class... DynVals,
            /* requires */ (sizeof...(DynVals) == m_size_dynamic && m_size_dynamic > 0)
        )
        MDSPAN_INLINE_FUNCTION
        constexpr maybe_static_array(DynVals... vals) : m_dyn_vals{static_cast<TDynamic>(vals)...} {}

        MDSPAN_TEMPLATE_REQUIRES(
            class T, size_t N,
            /* requires */ (N == m_size_dynamic && N > 0)
        )
        MDSPAN_INLINE_FUNCTION
        constexpr maybe_static_array(const std::array<T, N> &vals) {
            for (size_t r = 0; r < N; r++)
                m_dyn_vals[r] = static_cast<TDynamic>(vals[r]);
        }

        MDSPAN_TEMPLATE_REQUIRES(
            class T, size_t N,
            /* requires */ (N == m_size_dynamic && N == 0)
        )
        MDSPAN_INLINE_FUNCTION
        constexpr maybe_static_array(const std::array<T, N> &) : m_dyn_vals{} {}

#ifdef __cpp_lib_span
        MDSPAN_TEMPLATE_REQUIRES(
            class T, size_t N,
            /* requires */ (N == m_size_dynamic && N > 0)
        )
        MDSPAN_INLINE_FUNCTION
        constexpr maybe_static_array(const std::span<T, N> &vals) {
            for (size_t r = 0; r < N; r++)
                m_dyn_vals[r] = static_cast<TDynamic>(vals[r]);
        }

        MDSPAN_TEMPLATE_REQUIRES(
            class T, size_t N,
            /* requires */ (N == m_size_dynamic && N == 0)
        )
        MDSPAN_INLINE_FUNCTION
        constexpr maybe_static_array(const std::span<T, N> &) : m_dyn_vals{} {}
#endif

        // constructors from all values
        MDSPAN_TEMPLATE_REQUIRES(
            class... DynVals,
            /* requires */ (sizeof...(DynVals) != m_size_dynamic && m_size_dynamic > 0)
        )
        MDSPAN_INLINE_FUNCTION
        constexpr maybe_static_array(DynVals... vals) : m_dyn_vals{} {
            static_assert((sizeof...(DynVals) == m_size), "Invalid number of values.");
            TDynamic values[m_size]{static_cast<TDynamic>(vals)...};
            for (size_t r = 0; r < m_size; r++) {
                TStatic static_val = static_vals_t::get(r);
                if (static_val == dyn_tag) {
                    m_dyn_vals[dyn_map_t::get(r)] = values[r];
                }
// Precondition check
#ifdef MDSPAN_DEBUG
                else {
                    assert(values[r] == static_cast<TDynamic>(static_val));
                }
#endif
            }
        }

        MDSPAN_TEMPLATE_REQUIRES(
            class T, size_t N,
            /* requires */ (N != m_size_dynamic && m_size_dynamic > 0)
        )
        MDSPAN_INLINE_FUNCTION
        constexpr maybe_static_array(const std::array<T, N> &vals) {
            static_assert((N == m_size), "Invalid number of values.");
// Precondition check
#ifdef MDSPAN_DEBUG
            assert(N == m_size);
#endif
            for (size_t r = 0; r < m_size; r++) {
                TStatic static_val = static_vals_t::get(r);
                if (static_val == dyn_tag) {
                    m_dyn_vals[dyn_map_t::get(r)] = static_cast<TDynamic>(vals[r]);
                }
// Precondition check
#ifdef MDSPAN_DEBUG
                else {
                    assert(static_cast<TDynamic>(vals[r]) == static_cast<TDynamic>(static_val));
                }
#endif
            }
        }

#ifdef __cpp_lib_span
        MDSPAN_TEMPLATE_REQUIRES(
            class T, size_t N,
            /* requires */ (N != m_size_dynamic && m_size_dynamic > 0)
        )
        MDSPAN_INLINE_FUNCTION
        constexpr maybe_static_array(const std::span<T, N> &vals) {
            static_assert((N == m_size) || (m_size == dynamic_extent));
#ifdef MDSPAN_DEBUG
            assert(N == m_size);
#endif
            for (size_t r = 0; r < m_size; r++) {
                TStatic static_val = static_vals_t::get(r);
                if (static_val == dyn_tag) {
                    m_dyn_vals[dyn_map_t::get(r)] = static_cast<TDynamic>(vals[r]);
                }
#ifdef MDSPAN_DEBUG
                else {
                    assert(static_cast<TDynamic>(vals[r]) == static_cast<TDynamic>(static_val));
                }
#endif
            }
        }
#endif

        // access functions
        MDSPAN_INLINE_FUNCTION
        constexpr static TStatic static_value(size_t r) { return static_vals_t::get(r); }

        MDSPAN_INLINE_FUNCTION
        constexpr TDynamic value(size_t r) const {
            TStatic static_val = static_vals_t::get(r);

            // FIXME: workaround for nvhpc OpenACC compiler bug
            TStatic dyn_tag_copy = dyn_tag;
            return static_val == dyn_tag_copy ? m_dyn_vals[dyn_map_t::get(r)] : static_cast<TDynamic>(static_val);
        }
        MDSPAN_INLINE_FUNCTION
        constexpr TDynamic operator[](size_t r) const { return value(r); }

        // observers
        MDSPAN_INLINE_FUNCTION
        constexpr static size_t size() { return m_size; }
        MDSPAN_INLINE_FUNCTION
        constexpr static size_t size_dynamic() { return m_size_dynamic; }
    };

} // namespace detail
} // namespace MDSPAN_IMPL_STANDARD_NAMESPACE

namespace MDSPAN_IMPL_STANDARD_NAMESPACE {

// ------------------------------------------------------------------
// ------------ extents ---------------------------------------------
// ------------------------------------------------------------------

// Class to describe the extents of a multi dimensional array.
// Used by mdspan, mdarray and layout mappings.
// See ISO C++ standard [mdspan.extents]

template <class IndexType, size_t... Extents>
class extents {
  public:
    // typedefs for integral types used
    using index_type = IndexType;
    using size_type = std::make_unsigned_t<index_type>;
    using rank_type = size_t;

    static_assert(
        std::is_integral<index_type>::value && !std::is_same<index_type, bool>::value,
        MDSPAN_IMPL_STANDARD_NAMESPACE_STRING "::extents::index_type must be a signed or unsigned integer type"
    );

  private:
    constexpr static rank_type m_rank = sizeof...(Extents);
    constexpr static rank_type m_rank_dynamic =
        MDSPAN_IMPL_FOLD_PLUS_RIGHT((Extents == dynamic_extent), /* + ... + */ 0);

    // internal storage type using maybe_static_array
    using vals_t = detail::maybe_static_array<IndexType, size_t, dynamic_extent, Extents...>;
    MDSPAN_IMPL_NO_UNIQUE_ADDRESS vals_t m_vals;

  public:
    // [mdspan.extents.obs], observers of multidimensional index space
    MDSPAN_INLINE_FUNCTION
    constexpr static rank_type rank() noexcept { return m_rank; }
    MDSPAN_INLINE_FUNCTION
    constexpr static rank_type rank_dynamic() noexcept { return m_rank_dynamic; }

    MDSPAN_INLINE_FUNCTION
    constexpr index_type extent(rank_type r) const noexcept { return m_vals.value(r); }
    MDSPAN_INLINE_FUNCTION
    constexpr static size_t static_extent(rank_type r) noexcept { return vals_t::static_value(r); }

    // [mdspan.extents.cons], constructors
    MDSPAN_INLINE_FUNCTION_DEFAULTED
    constexpr extents() noexcept = default;

    // Construction from just dynamic or all values.
    // Precondition check is deferred to maybe_static_array constructor
    MDSPAN_TEMPLATE_REQUIRES(
        class... OtherIndexTypes,
        /* requires */ (
            MDSPAN_IMPL_FOLD_AND(MDSPAN_IMPL_TRAIT(std::is_convertible, OtherIndexTypes, index_type) /* && ... */) &&
            MDSPAN_IMPL_FOLD_AND(MDSPAN_IMPL_TRAIT(
                std::is_nothrow_constructible, index_type, OtherIndexTypes
            ) /* && ... */) &&
            (sizeof...(OtherIndexTypes) == m_rank || sizeof...(OtherIndexTypes) == m_rank_dynamic)
        )
    )
    MDSPAN_INLINE_FUNCTION
    constexpr explicit extents(OtherIndexTypes... dynvals) noexcept : m_vals(static_cast<index_type>(dynvals)...) {}

    MDSPAN_TEMPLATE_REQUIRES(
        class OtherIndexType, size_t N,
        /* requires */
        (MDSPAN_IMPL_TRAIT(std::is_convertible, const OtherIndexType &, index_type) &&
         MDSPAN_IMPL_TRAIT(std::is_nothrow_constructible, index_type, const OtherIndexType &) &&
         (N == m_rank || N == m_rank_dynamic))
    )
    MDSPAN_INLINE_FUNCTION
    MDSPAN_CONDITIONAL_EXPLICIT(N != m_rank_dynamic)
    constexpr extents(const std::array<OtherIndexType, N> &exts) noexcept : m_vals(std::move(exts)) {}

#ifdef __cpp_lib_span
    MDSPAN_TEMPLATE_REQUIRES(
        class OtherIndexType, size_t N,
        /* requires */
        (MDSPAN_IMPL_TRAIT(std::is_convertible, const OtherIndexType &, index_type) &&
         MDSPAN_IMPL_TRAIT(std::is_nothrow_constructible, index_type, const OtherIndexType &) &&
         (N == m_rank || N == m_rank_dynamic))
    )
    MDSPAN_INLINE_FUNCTION
    MDSPAN_CONDITIONAL_EXPLICIT(N != m_rank_dynamic)
    constexpr extents(const std::span<OtherIndexType, N> &exts) noexcept : m_vals(std::move(exts)) {}
#endif

  private:
    // Function to construct extents storage from other extents.
    // With C++ 17 the first two variants could be collapsed using if constexpr
    // in which case you don't need all the requires clauses.
    // in C++ 14 mode that doesn't work due to infinite recursion
    MDSPAN_TEMPLATE_REQUIRES(
        size_t DynCount, size_t R, class OtherExtents, class... DynamicValues,
        /* requires */ ((R < m_rank) && (static_extent(R) == dynamic_extent))
    )
    MDSPAN_INLINE_FUNCTION
    constexpr vals_t impl_construct_vals_from_extents(
        std::integral_constant<size_t, DynCount>, std::integral_constant<size_t, R>, const OtherExtents &exts,
        DynamicValues... dynamic_values
    ) noexcept {
        return impl_construct_vals_from_extents(
            std::integral_constant<size_t, DynCount + 1>(), std::integral_constant<size_t, R + 1>(), exts,
            dynamic_values..., exts.extent(R)
        );
    }

    MDSPAN_TEMPLATE_REQUIRES(
        size_t DynCount, size_t R, class OtherExtents, class... DynamicValues,
        /* requires */ ((R < m_rank) && (static_extent(R) != dynamic_extent))
    )
    MDSPAN_INLINE_FUNCTION
    constexpr vals_t impl_construct_vals_from_extents(
        std::integral_constant<size_t, DynCount>, std::integral_constant<size_t, R>, const OtherExtents &exts,
        DynamicValues... dynamic_values
    ) noexcept {
        return impl_construct_vals_from_extents(
            std::integral_constant<size_t, DynCount>(), std::integral_constant<size_t, R + 1>(), exts, dynamic_values...
        );
    }

    MDSPAN_TEMPLATE_REQUIRES(
        size_t DynCount, size_t R, class OtherExtents, class... DynamicValues,
        /* requires */ ((R == m_rank) && (DynCount == m_rank_dynamic))
    )
    MDSPAN_INLINE_FUNCTION
    constexpr vals_t impl_construct_vals_from_extents(
        std::integral_constant<size_t, DynCount>, std::integral_constant<size_t, R>, const OtherExtents &,
        DynamicValues... dynamic_values
    ) noexcept {
        return vals_t{static_cast<index_type>(dynamic_values)...};
    }

  public:
    // Converting constructor from other extents specializations
    MDSPAN_TEMPLATE_REQUIRES(
        class OtherIndexType, size_t... OtherExtents,
        /* requires */
        (
            /* multi-stage check to protect from invalid pack expansion when sizes
            don't match? */
            decltype(detail::impl_check_compatible_extents(
                // using: sizeof...(Extents) == sizeof...(OtherExtents) as the second argument fails with MSVC+NVCC with
                // some obscure expansion error MSVC: 19.38.33133 NVCC: 12.0
                std::integral_constant < bool,
                extents<int, Extents...>::rank() == extents<int, OtherExtents...>::rank() > {},
                std::integer_sequence<size_t, Extents...>{}, std::integer_sequence<size_t, OtherExtents...>{}
            ))::value
        )
    )
    MDSPAN_INLINE_FUNCTION
    MDSPAN_CONDITIONAL_EXPLICIT(
        (((Extents != dynamic_extent) && (OtherExtents == dynamic_extent)) || ...) ||
        (std::numeric_limits<index_type>::max() < std::numeric_limits<OtherIndexType>::max())
    )
    constexpr extents(const extents<OtherIndexType, OtherExtents...> &other) noexcept
        : m_vals(impl_construct_vals_from_extents(
              std::integral_constant<size_t, 0>(), std::integral_constant<size_t, 0>(), other
          )) {}

    // Comparison operator
    template <class OtherIndexType, size_t... OtherExtents>
    MDSPAN_INLINE_FUNCTION friend constexpr bool
    operator==(const extents &lhs, const extents<OtherIndexType, OtherExtents...> &rhs) noexcept {
        return rank() == extents<OtherIndexType, OtherExtents...>::rank() &&
               detail::rankwise_equal(detail::with_rank<rank()>{}, rhs, lhs, detail::extent);
    }

#if !(MDSPAN_HAS_CXX_20)
    template <class OtherIndexType, size_t... OtherExtents>
    MDSPAN_INLINE_FUNCTION friend constexpr bool
    operator!=(extents const &lhs, extents<OtherIndexType, OtherExtents...> const &rhs) noexcept {
        return !(lhs == rhs);
    }
#endif
};

// Recursive helper classes to implement dextents alias for extents
namespace detail {

    template <class IndexType, size_t Rank, class Extents = ::MDSPAN_IMPL_STANDARD_NAMESPACE::extents<IndexType>>
    struct impl_make_dextents;

    template <class IndexType, size_t Rank, size_t... ExtentsPack>
    struct impl_make_dextents<IndexType, Rank, ::MDSPAN_IMPL_STANDARD_NAMESPACE::extents<IndexType, ExtentsPack...>> {
        using type = typename impl_make_dextents<
            IndexType, Rank - 1,
            ::MDSPAN_IMPL_STANDARD_NAMESPACE::extents<
                IndexType, ::MDSPAN_IMPL_STANDARD_NAMESPACE::dynamic_extent, ExtentsPack...>>::type;
    };

    template <class IndexType, size_t... ExtentsPack>
    struct impl_make_dextents<IndexType, 0, ::MDSPAN_IMPL_STANDARD_NAMESPACE::extents<IndexType, ExtentsPack...>> {
        using type = ::MDSPAN_IMPL_STANDARD_NAMESPACE::extents<IndexType, ExtentsPack...>;
    };

} // end namespace detail

// [mdspan.extents.dextents], alias template
template <class IndexType, size_t Rank>
using dextents = typename detail::impl_make_dextents<IndexType, Rank>::type;

// Deduction guide for extents
#if defined(MDSPAN_IMPL_USE_CLASS_TEMPLATE_ARGUMENT_DEDUCTION)
template <class... IndexTypes>
extents(IndexTypes...)
    -> extents<size_t, ((void)sizeof(IndexTypes), ::MDSPAN_IMPL_STANDARD_NAMESPACE::dynamic_extent)...>;
#endif

// Helper type traits for identifying a class as extents.
namespace detail {

    template <class T>
    struct impl_is_extents : ::std::false_type {};

    template <class IndexType, size_t... ExtentsPack>
    struct impl_is_extents<::MDSPAN_IMPL_STANDARD_NAMESPACE::extents<IndexType, ExtentsPack...>> : ::std::true_type {};

    template <class T>
#if MDSPAN_HAS_CXX_17
    inline
#else
    static
#endif
        constexpr bool impl_is_extents_v = impl_is_extents<T>::value;

    template <class InputIndexType, class ExtentsIndexType>
    MDSPAN_INLINE_FUNCTION constexpr void check_lower_bound(
        InputIndexType user_index, ExtentsIndexType /* current_extent */, std::true_type /* is_signed */
    ) {
        (void)user_index; // prevent unused variable warning
#ifdef MDSPAN_DEBUG
        assert(static_cast<ExtentsIndexType>(user_index) >= 0);
#endif
    }

    template <class InputIndexType, class ExtentsIndexType>
    MDSPAN_INLINE_FUNCTION constexpr void check_lower_bound(
        InputIndexType /* user_index */, ExtentsIndexType /* current_extent */, std::false_type /* is_signed */
    ) {}

    template <class InputIndexType, class ExtentsIndexType>
    MDSPAN_INLINE_FUNCTION constexpr void
    check_upper_bound(InputIndexType user_index, ExtentsIndexType current_extent) {
        (void)user_index; // prevent unused variable warnings
        (void)current_extent;
#ifdef MDSPAN_DEBUG
        assert(static_cast<ExtentsIndexType>(user_index) < current_extent);
#endif
    }

    // Returning true to use AND fold instead of comma
    // CPP14 mode doesn't like the use of void expressions
    // with the way the MDSPAN_IMPL_FOLD_AND is set up
    template <class InputIndex, class ExtentsIndexType>
    MDSPAN_INLINE_FUNCTION constexpr bool check_one_index(InputIndex user_index, ExtentsIndexType current_extent) {
        check_lower_bound(
            user_index, current_extent, std::integral_constant<bool, std::is_signed<ExtentsIndexType>::value>{}
        );
        check_upper_bound(user_index, current_extent);
        return true;
    }

    template <size_t... RankIndices, class ExtentsIndexType, size_t... Exts, class... Indices>
    MDSPAN_INLINE_FUNCTION constexpr void check_all_indices_helper(
        std::index_sequence<RankIndices...>, const extents<ExtentsIndexType, Exts...> &exts, Indices... indices
    ) {
        // Suppress warning about statement has no effect
        (void)MDSPAN_IMPL_FOLD_AND((check_one_index(indices, exts.extent(RankIndices))));
    }

    template <class ExtentsIndexType, size_t... Exts, class... Indices>
    MDSPAN_INLINE_FUNCTION constexpr void
    check_all_indices(const extents<ExtentsIndexType, Exts...> &exts, Indices... indices) {
        check_all_indices_helper(std::make_index_sequence<sizeof...(Indices)>(), exts, indices...);
    }

} // namespace detail
} // namespace MDSPAN_IMPL_STANDARD_NAMESPACE
// END_FILE_INCLUDE: mdspan/include/experimental/__p0009_bits/extents.hpp
// BEGIN_FILE_INCLUDE: mdspan/include/experimental/__p0009_bits/layout_stride.hpp
//@HEADER
//  ************************************************************************
//
//                         Kokkos v. 4.0
//        Copyright (2022) National Technology & Engineering
//                Solutions of Sandia, LLC (NTESS).
//
//  Under the terms of Contract DE-NA0003525 with NTESS,
//  the U.S. Government retains certain rights in this software.
//
//  Part of Kokkos, under the Apache License v2.0 with LLVM Exceptions.
//  See https://kokkos.org/LICENSE for license information.
//  SPDX-License-Identifier: Apache-2.0 WITH LLVM-exception
//
//@HEADER

// BEGIN_FILE_INCLUDE: mdspan/include/experimental/__p0009_bits/compressed_pair.hpp
//@HEADER
//  ************************************************************************
//
//                         Kokkos v. 4.0
//        Copyright (2022) National Technology & Engineering
//                Solutions of Sandia, LLC (NTESS).
//
//  Under the terms of Contract DE-NA0003525 with NTESS,
//  the U.S. Government retains certain rights in this software.
//
//  Part of Kokkos, under the Apache License v2.0 with LLVM Exceptions.
//  See https://kokkos.org/LICENSE for license information.
//  SPDX-License-Identifier: Apache-2.0 WITH LLVM-exception
//
//@HEADER

#if !defined(MDSPAN_IMPL_USE_ATTRIBUTE_NO_UNIQUE_ADDRESS)
// BEGIN_FILE_INCLUDE: mdspan/include/experimental/__p0009_bits/no_unique_address.hpp
//@HEADER
//  ************************************************************************
//
//                         Kokkos v. 4.0
//        Copyright (2022) National Technology & Engineering
//                Solutions of Sandia, LLC (NTESS).
//
//  Under the terms of Contract DE-NA0003525 with NTESS,
//  the U.S. Government retains certain rights in this software.
//
//  Part of Kokkos, under the Apache License v2.0 with LLVM Exceptions.
//  See https://kokkos.org/LICENSE for license information.
//  SPDX-License-Identifier: Apache-2.0 WITH LLVM-exception
//
//@HEADER

namespace MDSPAN_IMPL_STANDARD_NAMESPACE {
namespace detail {

    //==============================================================================

    template <class T, size_t Disambiguator = 0, class Enable = void>
    struct no_unique_address_emulation {
        using stored_type = T;
        T m_v;
        MDSPAN_FORCE_INLINE_FUNCTION constexpr T const &ref() const noexcept { return m_v; }
        MDSPAN_FORCE_INLINE_FUNCTION MDSPAN_IMPL_CONSTEXPR_14 T &ref() noexcept { return m_v; }
    };

    // Empty case
    // This doesn't work if T is final, of course, but we're not using anything
    // like that currently. That kind of thing could be added pretty easily though
    template <class T, size_t Disambiguator>
    struct no_unique_address_emulation<
        T, Disambiguator,
        std::enable_if_t<
            MDSPAN_IMPL_TRAIT(std::is_empty, T) &&
            // If the type isn't trivially destructible, its destructor
            // won't be called at the right time, so don't use this
            // specialization
            MDSPAN_IMPL_TRAIT(std::is_trivially_destructible, T)>> :
#ifdef MDSPAN_IMPL_COMPILER_MSVC
        // MSVC doesn't allow you to access public static member functions of a type
        // when you *happen* to privately inherit from that type.
        protected
#else
        // But we still want this to be private if possible so that we don't accidentally
        // access members of T directly rather than calling ref() first, which wouldn't
        // work if T happens to be stateful and thus we're using the unspecialized definition
        // of no_unique_address_emulation above.
        private
#endif
        T {
        using stored_type = T;
        MDSPAN_FORCE_INLINE_FUNCTION constexpr T const &ref() const noexcept { return *static_cast<T const *>(this); }
        MDSPAN_FORCE_INLINE_FUNCTION MDSPAN_IMPL_CONSTEXPR_14 T &ref() noexcept { return *static_cast<T *>(this); }

        MDSPAN_INLINE_FUNCTION_DEFAULTED
        constexpr no_unique_address_emulation() noexcept = default;
        MDSPAN_INLINE_FUNCTION_DEFAULTED
        constexpr no_unique_address_emulation(no_unique_address_emulation const &) noexcept = default;
        MDSPAN_INLINE_FUNCTION_DEFAULTED
        constexpr no_unique_address_emulation(no_unique_address_emulation &&) noexcept = default;
        MDSPAN_INLINE_FUNCTION_DEFAULTED
        MDSPAN_IMPL_CONSTEXPR_14_DEFAULTED no_unique_address_emulation &
        operator=(no_unique_address_emulation const &) noexcept = default;
        MDSPAN_INLINE_FUNCTION_DEFAULTED
        MDSPAN_IMPL_CONSTEXPR_14_DEFAULTED no_unique_address_emulation &
        operator=(no_unique_address_emulation &&) noexcept = default;
        MDSPAN_INLINE_FUNCTION_DEFAULTED
        ~no_unique_address_emulation() noexcept = default;

        // Explicitly make this not a reference so that the copy or move
        // constructor still gets called.
        MDSPAN_INLINE_FUNCTION
        explicit constexpr no_unique_address_emulation(T const &v) noexcept : T(v) {}
        MDSPAN_INLINE_FUNCTION
        explicit constexpr no_unique_address_emulation(T &&v) noexcept : T(::std::move(v)) {}
    };

    //==============================================================================

} // end namespace detail
} // end namespace MDSPAN_IMPL_STANDARD_NAMESPACE
// END_FILE_INCLUDE: mdspan/include/experimental/__p0009_bits/no_unique_address.hpp
#endif

namespace MDSPAN_IMPL_STANDARD_NAMESPACE {
namespace detail {

    // For no unique address emulation, this is the case taken when neither are empty.
    // For real `[[no_unique_address]]`, this case is always taken.
    template <class T1, class T2, class Enable = void>
    struct impl_compressed_pair {
        MDSPAN_IMPL_NO_UNIQUE_ADDRESS T1 m_t1_val{};
        MDSPAN_IMPL_NO_UNIQUE_ADDRESS T2 m_t2_val{};
        MDSPAN_FORCE_INLINE_FUNCTION MDSPAN_IMPL_CONSTEXPR_14 T1 &first() noexcept { return m_t1_val; }
        MDSPAN_FORCE_INLINE_FUNCTION constexpr T1 const &first() const noexcept { return m_t1_val; }
        MDSPAN_FORCE_INLINE_FUNCTION MDSPAN_IMPL_CONSTEXPR_14 T2 &second() noexcept { return m_t2_val; }
        MDSPAN_FORCE_INLINE_FUNCTION constexpr T2 const &second() const noexcept { return m_t2_val; }

        MDSPAN_INLINE_FUNCTION_DEFAULTED
        constexpr impl_compressed_pair() = default;
        MDSPAN_INLINE_FUNCTION_DEFAULTED
        constexpr impl_compressed_pair(impl_compressed_pair const &) = default;
        MDSPAN_INLINE_FUNCTION_DEFAULTED
        constexpr impl_compressed_pair(impl_compressed_pair &&) = default;
        MDSPAN_INLINE_FUNCTION_DEFAULTED
        MDSPAN_IMPL_CONSTEXPR_14_DEFAULTED impl_compressed_pair &operator=(impl_compressed_pair const &) = default;
        MDSPAN_INLINE_FUNCTION_DEFAULTED
        MDSPAN_IMPL_CONSTEXPR_14_DEFAULTED impl_compressed_pair &operator=(impl_compressed_pair &&) = default;
        MDSPAN_INLINE_FUNCTION_DEFAULTED
        ~impl_compressed_pair() = default;
        template <class T1Like, class T2Like>
        MDSPAN_INLINE_FUNCTION constexpr impl_compressed_pair(T1Like &&t1, T2Like &&t2)
            : m_t1_val((T1Like &&)t1), m_t2_val((T2Like &&)t2) {}
    };

#if !defined(MDSPAN_IMPL_USE_ATTRIBUTE_NO_UNIQUE_ADDRESS)

    // First empty.
    template <class T1, class T2>
    struct impl_compressed_pair<
        T1, T2, std::enable_if_t<MDSPAN_IMPL_TRAIT(std::is_empty, T1) && !MDSPAN_IMPL_TRAIT(std::is_empty, T2)>>
        : private T1 {
        T2 m_t2_val{};
        MDSPAN_FORCE_INLINE_FUNCTION MDSPAN_IMPL_CONSTEXPR_14 T1 &first() noexcept { return *static_cast<T1 *>(this); }
        MDSPAN_FORCE_INLINE_FUNCTION constexpr T1 const &first() const noexcept {
            return *static_cast<T1 const *>(this);
        }
        MDSPAN_FORCE_INLINE_FUNCTION MDSPAN_IMPL_CONSTEXPR_14 T2 &second() noexcept { return m_t2_val; }
        MDSPAN_FORCE_INLINE_FUNCTION constexpr T2 const &second() const noexcept { return m_t2_val; }

        MDSPAN_INLINE_FUNCTION_DEFAULTED
        constexpr impl_compressed_pair() = default;
        MDSPAN_INLINE_FUNCTION_DEFAULTED
        constexpr impl_compressed_pair(impl_compressed_pair const &) = default;
        MDSPAN_INLINE_FUNCTION_DEFAULTED
        constexpr impl_compressed_pair(impl_compressed_pair &&) = default;
        MDSPAN_INLINE_FUNCTION_DEFAULTED
        MDSPAN_IMPL_CONSTEXPR_14_DEFAULTED impl_compressed_pair &operator=(impl_compressed_pair const &) = default;
        MDSPAN_INLINE_FUNCTION_DEFAULTED
        MDSPAN_IMPL_CONSTEXPR_14_DEFAULTED impl_compressed_pair &operator=(impl_compressed_pair &&) = default;
        MDSPAN_INLINE_FUNCTION_DEFAULTED
        ~impl_compressed_pair() = default;
        template <class T1Like, class T2Like>
        MDSPAN_INLINE_FUNCTION constexpr impl_compressed_pair(T1Like &&t1, T2Like &&t2)
            : T1((T1Like &&)t1), m_t2_val((T2Like &&)t2) {}
    };

    // Second empty.
    template <class T1, class T2>
    struct impl_compressed_pair<
        T1, T2, std::enable_if_t<!MDSPAN_IMPL_TRAIT(std::is_empty, T1) && MDSPAN_IMPL_TRAIT(std::is_empty, T2)>>
        : private T2 {
        T1 m_t1_val{};
        MDSPAN_FORCE_INLINE_FUNCTION MDSPAN_IMPL_CONSTEXPR_14 T1 &first() noexcept { return m_t1_val; }
        MDSPAN_FORCE_INLINE_FUNCTION constexpr T1 const &first() const noexcept { return m_t1_val; }
        MDSPAN_FORCE_INLINE_FUNCTION MDSPAN_IMPL_CONSTEXPR_14 T2 &second() noexcept { return *static_cast<T2 *>(this); }
        MDSPAN_FORCE_INLINE_FUNCTION constexpr T2 const &second() const noexcept {
            return *static_cast<T2 const *>(this);
        }

        MDSPAN_INLINE_FUNCTION_DEFAULTED
        constexpr impl_compressed_pair() = default;
        MDSPAN_INLINE_FUNCTION_DEFAULTED
        constexpr impl_compressed_pair(impl_compressed_pair const &) = default;
        MDSPAN_INLINE_FUNCTION_DEFAULTED
        constexpr impl_compressed_pair(impl_compressed_pair &&) = default;
        MDSPAN_INLINE_FUNCTION_DEFAULTED
        MDSPAN_IMPL_CONSTEXPR_14_DEFAULTED impl_compressed_pair &operator=(impl_compressed_pair const &) = default;
        MDSPAN_INLINE_FUNCTION_DEFAULTED
        MDSPAN_IMPL_CONSTEXPR_14_DEFAULTED impl_compressed_pair &operator=(impl_compressed_pair &&) = default;
        MDSPAN_INLINE_FUNCTION_DEFAULTED
        ~impl_compressed_pair() = default;

        template <class T1Like, class T2Like>
        MDSPAN_INLINE_FUNCTION constexpr impl_compressed_pair(T1Like &&t1, T2Like &&t2)
            : T2((T2Like &&)t2), m_t1_val((T1Like &&)t1) {}
    };

    // Both empty.
    template <class T1, class T2>
    struct impl_compressed_pair<
        T1, T2, std::enable_if_t<MDSPAN_IMPL_TRAIT(std::is_empty, T1) && MDSPAN_IMPL_TRAIT(std::is_empty, T2)>>
    // We need to use the no_unique_address_emulation wrapper here to avoid
    // base class ambiguities.
#ifdef MDSPAN_IMPL_COMPILER_MSVC
        // MSVC doesn't allow you to access public static member functions of a type
        // when you *happen* to privately inherit from that type.
        : protected no_unique_address_emulation<T1, 0>,
          protected no_unique_address_emulation<T2, 1>
#else
        : private no_unique_address_emulation<T1, 0>,
          private no_unique_address_emulation<T2, 1>
#endif
    {
        using first_base_t = no_unique_address_emulation<T1, 0>;
        using second_base_t = no_unique_address_emulation<T2, 1>;

        MDSPAN_FORCE_INLINE_FUNCTION MDSPAN_IMPL_CONSTEXPR_14 T1 &first() noexcept { return this->first_base_t::ref(); }
        MDSPAN_FORCE_INLINE_FUNCTION constexpr T1 const &first() const noexcept { return this->first_base_t::ref(); }
        MDSPAN_FORCE_INLINE_FUNCTION MDSPAN_IMPL_CONSTEXPR_14 T2 &second() noexcept {
            return this->second_base_t::ref();
        }
        MDSPAN_FORCE_INLINE_FUNCTION constexpr T2 const &second() const noexcept { return this->second_base_t::ref(); }

        MDSPAN_INLINE_FUNCTION_DEFAULTED
        constexpr impl_compressed_pair() = default;
        MDSPAN_INLINE_FUNCTION_DEFAULTED
        constexpr impl_compressed_pair(impl_compressed_pair const &) = default;
        MDSPAN_INLINE_FUNCTION_DEFAULTED
        constexpr impl_compressed_pair(impl_compressed_pair &&) = default;
        MDSPAN_INLINE_FUNCTION_DEFAULTED
        MDSPAN_IMPL_CONSTEXPR_14_DEFAULTED impl_compressed_pair &operator=(impl_compressed_pair const &) = default;
        MDSPAN_INLINE_FUNCTION_DEFAULTED
        MDSPAN_IMPL_CONSTEXPR_14_DEFAULTED impl_compressed_pair &operator=(impl_compressed_pair &&) = default;
        MDSPAN_INLINE_FUNCTION_DEFAULTED
        ~impl_compressed_pair() = default;
        template <class T1Like, class T2Like>
        MDSPAN_INLINE_FUNCTION constexpr impl_compressed_pair(T1Like &&t1, T2Like &&t2) noexcept
            : first_base_t(T1((T1Like &&)t1)), second_base_t(T2((T2Like &&)t2)) {}
    };

#endif // !defined(MDSPAN_IMPL_USE_ATTRIBUTE_NO_UNIQUE_ADDRESS)

} // end namespace detail
} // end namespace MDSPAN_IMPL_STANDARD_NAMESPACE
// END_FILE_INCLUDE: mdspan/include/experimental/__p0009_bits/compressed_pair.hpp

#if !defined(MDSPAN_IMPL_USE_ATTRIBUTE_NO_UNIQUE_ADDRESS)
#endif

#include <array>
#include <type_traits>
#include <utility>

#ifdef __cpp_lib_span
#include <span>
#endif
#if defined(MDSPAN_IMPL_USE_CONCEPTS) && MDSPAN_HAS_CXX_20 && defined(__cpp_lib_concepts)
#include <concepts>
#endif

namespace MDSPAN_IMPL_STANDARD_NAMESPACE {

struct layout_left {
    template <class Extents>
    class mapping;
};
struct layout_right {
    template <class Extents>
    class mapping;
};

namespace detail {
#if MDSPAN_HAS_CXX_17
    using std::void_t;
#else
    template <class...>
    using void_t = void;
#endif
    // FIXME GCC <= 12: workaround gcc-12 bug that shows up in Kokkos; compilation fails when Mapping doesn't have
    // extents_type. Normally this should just be a substitution failure, but causes an error with GCC <= 12
    // FIXME MSVC: I guess MSVC has a similar issue when it hits Layout::template mapping
    template <class, class, class = void, class = void>
    struct is_mapping_of_impl : std::false_type {};

    // FIXME GCC <= 12: We can't just do a conjunction of the two conditions, because the affected GCC versions seem to
    // not short-circuit when resolving the substitution of Mapping
    template <class Mapping, class Layout>
    struct is_mapping_of_impl<
        Mapping, Layout, void_t<typename Mapping::extents_type>,
        void_t<typename Layout::template mapping<typename Mapping::extents_type>>>
        : std::is_same<typename Layout::template mapping<typename Mapping::extents_type>, Mapping> {};

    template <class Layout, class Mapping>
    constexpr bool is_mapping_of = is_mapping_of_impl<Mapping, Layout>::value;

#if defined(MDSPAN_IMPL_USE_CONCEPTS) && MDSPAN_HAS_CXX_20
#if !defined(__cpp_lib_concepts)
    namespace internal {
        namespace detail {
            template <typename Tp, typename _Up>
            concept same_as = std::is_same_v<Tp, _Up>;
        } // namespace detail
        template <class T, class U>
        concept same_as = detail::same_as<T, U> && detail::same_as<U, T>;
    } // namespace internal
#endif

    template <class M>
    concept layout_mapping_alike = requires {
        requires impl_is_extents<typename M::extents_type>::value;
#if defined(__cpp_lib_concepts)
        {M::is_always_strided()}->std::same_as<bool>;
        {M::is_always_exhaustive()}->std::same_as<bool>;
        {M::is_always_unique()}->std::same_as<bool>;
#else
        {M::is_always_strided()}->internal::same_as<bool>;
        {M::is_always_exhaustive()}->internal::same_as<bool>;
        {M::is_always_unique()}->internal::same_as<bool>;
#endif
        std::bool_constant<M::is_always_strided()>::value;
        std::bool_constant<M::is_always_exhaustive()>::value;
        std::bool_constant<M::is_always_unique()>::value;
    };
#endif

} // namespace detail

struct layout_stride {
    template <class Extents>
    class mapping
#if !defined(MDSPAN_IMPL_USE_ATTRIBUTE_NO_UNIQUE_ADDRESS)
        : private detail::no_unique_address_emulation<detail::impl_compressed_pair<
              Extents, detail::possibly_empty_array<typename Extents::index_type, Extents::rank()>>>
#endif
    {
      public:
        using extents_type = Extents;
        using index_type = typename extents_type::index_type;
        using size_type = typename extents_type::size_type;
        using rank_type = typename extents_type::rank_type;
        using layout_type = layout_stride;

        // This could be a `requires`, but I think it's better and clearer as a `static_assert`.
        static_assert(
            detail::impl_is_extents_v<Extents>,
            MDSPAN_IMPL_STANDARD_NAMESPACE_STRING "::layout_stride::mapping must be instantiated with a specialization "
                                                  "of " MDSPAN_IMPL_STANDARD_NAMESPACE_STRING "::extents."
        );

      private:
        //----------------------------------------------------------------------------

        using strides_storage_t = detail::possibly_empty_array<index_type, extents_type::rank()>;
        using member_pair_t = detail::impl_compressed_pair<extents_type, strides_storage_t>;

#if defined(MDSPAN_IMPL_USE_ATTRIBUTE_NO_UNIQUE_ADDRESS)
        MDSPAN_IMPL_NO_UNIQUE_ADDRESS member_pair_t m_members;
#else
        using base_t = detail::no_unique_address_emulation<member_pair_t>;
#endif

        MDSPAN_FORCE_INLINE_FUNCTION constexpr strides_storage_t const &strides_storage() const noexcept {
#if defined(MDSPAN_IMPL_USE_ATTRIBUTE_NO_UNIQUE_ADDRESS)
            return m_members.second();
#else
            return this->base_t::ref().second();
#endif
        }
        MDSPAN_FORCE_INLINE_FUNCTION MDSPAN_IMPL_CONSTEXPR_14 strides_storage_t &strides_storage() noexcept {
#if defined(MDSPAN_IMPL_USE_ATTRIBUTE_NO_UNIQUE_ADDRESS)
            return m_members.second();
#else
            return this->base_t::ref().second();
#endif
        }

        template <class SizeType, size_t... Ep, size_t... Idx>
        MDSPAN_IMPL_HOST_DEVICE constexpr index_type get_size(
            ::MDSPAN_IMPL_STANDARD_NAMESPACE::extents<SizeType, Ep...>, std::integer_sequence<size_t, Idx...>
        ) const {
            return MDSPAN_IMPL_FOLD_TIMES_RIGHT(static_cast<index_type>(extents().extent(Idx)), 1);
        }

        //----------------------------------------------------------------------------

        template <class>
        friend class mapping;

        //----------------------------------------------------------------------------

        // Workaround for non-deducibility of the index sequence template parameter if it's given at the top level
        template <class>
        struct deduction_workaround;

        template <size_t... Idxs>
        struct deduction_workaround<std::index_sequence<Idxs...>> {
            template <class OtherExtents>
            MDSPAN_INLINE_FUNCTION static constexpr bool
            _eq_impl(mapping const &self, mapping<OtherExtents> const &other) noexcept {
                using common_t = std::common_type_t<index_type, typename OtherExtents::index_type>;
                return MDSPAN_IMPL_FOLD_AND((
                           static_cast<common_t>(self.stride(Idxs)) == static_cast<common_t>(other.stride(Idxs))
                       ) /* && ... */) &&
                       MDSPAN_IMPL_FOLD_AND((
                           static_cast<common_t>(self.extents().extent(Idxs)) ==
                           static_cast<common_t>(other.extents().extent(Idxs))
                       ) /* || ... */);
            }
            template <class OtherExtents>
            MDSPAN_INLINE_FUNCTION static constexpr bool
            _not_eq_impl(mapping const &self, mapping<OtherExtents> const &other) noexcept {
                using common_t = std::common_type_t<index_type, typename OtherExtents::index_type>;
                return MDSPAN_IMPL_FOLD_OR((
                           static_cast<common_t>(self.stride(Idxs)) != static_cast<common_t>(other.stride(Idxs))
                       ) /* || ... */) ||
                       MDSPAN_IMPL_FOLD_OR((
                           static_cast<common_t>(self.extents().extent(Idxs)) !=
                           static_cast<common_t>(other.extents().extent(Idxs))
                       ) /* || ... */);
            }

            template <class... Integral>
            MDSPAN_FORCE_INLINE_FUNCTION static constexpr size_t
            _call_op_impl(mapping const &self, Integral... idxs) noexcept {
                return MDSPAN_IMPL_FOLD_PLUS_RIGHT((idxs * self.stride(Idxs)), /* + ... + */ 0);
            }

            MDSPAN_INLINE_FUNCTION
            static constexpr size_t _req_span_size_impl(mapping const &self) noexcept {
                // assumes no negative strides; not sure if I'm allowed to assume that or not
                return deduction_workaround_impl::_call_op_impl(self, (self.extents().template extent<Idxs>() - 1)...) +
                       1;
            }

            template <class OtherMapping>
            MDSPAN_INLINE_FUNCTION static constexpr const strides_storage_t fill_strides(const OtherMapping &map) {
                return strides_storage_t{static_cast<index_type>(map.stride(Idxs))...};
            }

            MDSPAN_INLINE_FUNCTION
            static constexpr const strides_storage_t &fill_strides(const strides_storage_t &s) { return s; }

            template <class IntegralType>
            static constexpr const strides_storage_t
            fill_strides(const std::array<IntegralType, extents_type::rank()> &s) {
                return strides_storage_t{static_cast<index_type>(s[Idxs])...};
            }

            MDSPAN_TEMPLATE_REQUIRES(
                class IntegralType, (std::is_convertible<IntegralType, typename extents_type::index_type>::value)
            )
            MDSPAN_INLINE_FUNCTION
            // Need to avoid zero length c-array
            static constexpr const strides_storage_t fill_strides(
                mdspan_non_standard_tag, const IntegralType (&s)[extents_type::rank() > 0 ? extents_type::rank() : 1]
            ) {
                return strides_storage_t{static_cast<index_type>(s[Idxs])...};
            }

#ifdef __cpp_lib_span
            template <class IntegralType>
            static constexpr const strides_storage_t
            fill_strides(const std::span<IntegralType, extents_type::rank()> &s) {
                return strides_storage_t{static_cast<index_type>(s[Idxs])...};
            }
#endif

            MDSPAN_INLINE_FUNCTION
            static constexpr std::array<index_type, extents_type::rank()> return_strides(const strides_storage_t &s) {
                return std::array<index_type, extents_type::rank()>{s[Idxs]...};
            }

            template <size_t K>
            MDSPAN_INLINE_FUNCTION static constexpr size_t return_zero() {
                return 0;
            }

            template <class Mapping>
            MDSPAN_INLINE_FUNCTION static constexpr typename Mapping::index_type offset(const Mapping &m) {
                return m(return_zero<Idxs>()...);
            }
        };

        // Can't use defaulted parameter in the deduction_workaround template because of a bug in MSVC warning C4348.
        using deduction_workaround_impl = deduction_workaround<std::make_index_sequence<Extents::rank()>>;

        MDSPAN_FUNCTION
        static constexpr strides_storage_t strides_storage(detail::with_rank<0>) { return {}; }

        template <std::size_t N>
        MDSPAN_FUNCTION static constexpr strides_storage_t strides_storage(detail::with_rank<N>) {
            strides_storage_t s{};

            extents_type e;
            index_type stride = 1;
            for (int r = static_cast<int>(extents_type::rank() - 1); r >= 0; r--) {
                s[r] = stride;
                stride *= e.extent(r);
            }

            return s;
        }

        //----------------------------------------------------------------------------

#if defined(MDSPAN_IMPL_USE_ATTRIBUTE_NO_UNIQUE_ADDRESS)
        MDSPAN_INLINE_FUNCTION constexpr explicit mapping(member_pair_t &&m) : m_members(::std::move(m)) {}
#else
        MDSPAN_INLINE_FUNCTION constexpr explicit mapping(base_t &&__b) : base_t(::std::move(__b)) {}
#endif

      public:
        //--------------------------------------------------------------------------------

        MDSPAN_INLINE_FUNCTION constexpr mapping() noexcept
#if defined(MDSPAN_IMPL_USE_ATTRIBUTE_NO_UNIQUE_ADDRESS)
      : m_members{
#else
            : base_t(
                  base_t{member_pair_t(
#endif
          extents_type(),
          strides_storage_t(strides_storage(detail::with_rank<extents_type::rank()>{}))
#if defined(MDSPAN_IMPL_USE_ATTRIBUTE_NO_UNIQUE_ADDRESS)
        }
#else
                  )}
              )
#endif
    {
        }

        MDSPAN_INLINE_FUNCTION_DEFAULTED constexpr mapping(mapping const &) noexcept = default;

        MDSPAN_TEMPLATE_REQUIRES(
            class IntegralTypes,
            /* requires */ (
                // MSVC 19.32 does not like using index_type here, requires the typename Extents::index_type
                // error C2641: cannot deduce template arguments for
                // 'MDSPAN_IMPL_STANDARD_NAMESPACE::layout_stride::mapping'
                MDSPAN_IMPL_TRAIT(
                    std::is_convertible, const std::remove_const_t<IntegralTypes> &, typename Extents::index_type
                ) &&
                MDSPAN_IMPL_TRAIT(
                    std::is_nothrow_constructible, typename Extents::index_type,
                    const std::remove_const_t<IntegralTypes> &
                )
            )
        )
        constexpr
    mapping(
      extents_type const& e,
      std::array<IntegralTypes, extents_type::rank()> const& s
    ) noexcept
#if defined(MDSPAN_IMPL_USE_ATTRIBUTE_NO_UNIQUE_ADDRESS)
      : m_members{
#else
            : base_t(
                  base_t{member_pair_t(
#endif
          e, strides_storage_t(deduction_workaround_impl::fill_strides(s))
#if defined(MDSPAN_IMPL_USE_ATTRIBUTE_NO_UNIQUE_ADDRESS)
        }
#else
                  )}
              )
#endif
    {
            /*
             * TODO: check preconditions
             * - s[i] > 0 is true for all i in the range [0, rank_ ).
             * - REQUIRED-SPAN-SIZE(e, s) is a representable value of type index_type ([basic.fundamental]).
             * - If rank_ is greater than 0, then there exists a permutation P of the integers in the
             *   range [0, rank_), such that s[ pi ] >= s[ pi − 1 ] * e.extent( pi − 1 ) is true for
             *   all i in the range [1, rank_ ), where pi is the ith element of P.
             */
        }

        MDSPAN_TEMPLATE_REQUIRES(
            class IntegralTypes,
            /* requires */ (
                // MSVC 19.32 does not like using index_type here, requires the typename Extents::index_type
                // error C2641: cannot deduce template arguments for
                // 'MDSPAN_IMPL_STANDARD_NAMESPACE::layout_stride::mapping'
                MDSPAN_IMPL_TRAIT(
                    std::is_convertible, const std::remove_const_t<IntegralTypes> &, typename Extents::index_type
                ) &&
                MDSPAN_IMPL_TRAIT(
                    std::is_nothrow_constructible, typename Extents::index_type,
                    const std::remove_const_t<IntegralTypes> &
                )
            )
        )
        MDSPAN_INLINE_FUNCTION
        constexpr
    mapping(
      mdspan_non_standard_tag,
      extents_type const& e,
      // Need to avoid zero-length c-array
      const IntegralTypes (&s)[extents_type::rank()>0?extents_type::rank():1]
    ) noexcept
#if defined(MDSPAN_IMPL_USE_ATTRIBUTE_NO_UNIQUE_ADDRESS)
      : m_members{
#else
            : base_t(
                  base_t{member_pair_t(
#endif
          e, strides_storage_t(deduction_workaround_impl::fill_strides(mdspan_non_standard, s))
#if defined(MDSPAN_IMPL_USE_ATTRIBUTE_NO_UNIQUE_ADDRESS)
        }
#else
                  )}
              )
#endif
    {
            /*
             * TODO: check preconditions
             * - s[i] > 0 is true for all i in the range [0, rank_ ).
             * - REQUIRED-SPAN-SIZE(e, s) is a representable value of type index_type ([basic.fundamental]).
             * - If rank_ is greater than 0, then there exists a permutation P of the integers in the
             *   range [0, rank_), such that s[ pi ] >= s[ pi − 1 ] * e.extent( pi − 1 ) is true for
             *   all i in the range [1, rank_ ), where pi is the ith element of P.
             */
        }

#ifdef __cpp_lib_span
        MDSPAN_TEMPLATE_REQUIRES(
            class IntegralTypes,
            /* requires */ (
                // MSVC 19.32 does not like using index_type here, requires the typename Extents::index_type
                // error C2641: cannot deduce template arguments for
                // 'MDSPAN_IMPL_STANDARD_NAMESPACE::layout_stride::mapping'
                MDSPAN_IMPL_TRAIT(
                    std::is_convertible, const std::remove_const_t<IntegralTypes> &, typename Extents::index_type
                ) &&
                MDSPAN_IMPL_TRAIT(
                    std::is_nothrow_constructible, typename Extents::index_type,
                    const std::remove_const_t<IntegralTypes> &
                )
            )
        )
        constexpr
    mapping(
      extents_type const& e,
      std::span<IntegralTypes, extents_type::rank()> const& s
    ) noexcept
#if defined(MDSPAN_IMPL_USE_ATTRIBUTE_NO_UNIQUE_ADDRESS)
      : m_members{
#else
            : base_t(
                  base_t{member_pair_t(
#endif
          e, strides_storage_t(deduction_workaround_impl::fill_strides(s))
#if defined(MDSPAN_IMPL_USE_ATTRIBUTE_NO_UNIQUE_ADDRESS)
        }
#else
                  )}
              )
#endif
    {
            /*
             * TODO: check preconditions
             * - s[i] > 0 is true for all i in the range [0, rank_ ).
             * - REQUIRED-SPAN-SIZE(e, s) is a representable value of type index_type ([basic.fundamental]).
             * - If rank_ is greater than 0, then there exists a permutation P of the integers in the
             *   range [0, rank_), such that s[ pi ] >= s[ pi − 1 ] * e.extent( pi − 1 ) is true for
             *   all i in the range [1, rank_ ), where pi is the ith element of P.
             */
        }
#endif // __cpp_lib_span

#if !(defined(MDSPAN_IMPL_USE_CONCEPTS) && MDSPAN_HAS_CXX_20)
        MDSPAN_TEMPLATE_REQUIRES(
            class StridedLayoutMapping,
            /* requires */ (
                MDSPAN_IMPL_TRAIT(std::is_constructible, extents_type, typename StridedLayoutMapping::extents_type) &&
                detail::is_mapping_of<typename StridedLayoutMapping::layout_type, StridedLayoutMapping> &&
                StridedLayoutMapping::is_always_unique() && StridedLayoutMapping::is_always_strided()
            )
        )
#else
        template <class StridedLayoutMapping>
        requires(
            detail::layout_mapping_alike<StridedLayoutMapping>
                &&MDSPAN_IMPL_TRAIT(std::is_constructible, extents_type, typename StridedLayoutMapping::extents_type) &&
            StridedLayoutMapping::is_always_unique() && StridedLayoutMapping::is_always_strided()
        )
#endif
        MDSPAN_CONDITIONAL_EXPLICIT(
            !(std::is_convertible<typename StridedLayoutMapping::extents_type, extents_type>::value &&
              (detail::is_mapping_of<layout_left, StridedLayoutMapping> ||
               detail::is_mapping_of<layout_right, StridedLayoutMapping> ||
               detail::is_mapping_of<layout_stride, StridedLayoutMapping>))
        ) // needs two () due to comma
        MDSPAN_INLINE_FUNCTION MDSPAN_IMPL_CONSTEXPR_14
    mapping(StridedLayoutMapping const& other) noexcept // NOLINT(google-explicit-constructor)
#if defined(MDSPAN_IMPL_USE_ATTRIBUTE_NO_UNIQUE_ADDRESS)
      : m_members{
#else
            : base_t(
                  base_t{member_pair_t(
#endif
          other.extents(), strides_storage_t(deduction_workaround_impl::fill_strides(other))
#if defined(MDSPAN_IMPL_USE_ATTRIBUTE_NO_UNIQUE_ADDRESS)
        }
#else
                  )}
              )
#endif
    {
            /*
             * TODO: check preconditions
             * - other.stride(i) > 0 is true for all i in the range [0, rank_ ).
             * - other.required_span_size() is a representable value of type index_type ([basic.fundamental]).
             * - OFFSET(other) == 0
             */
        }

        //--------------------------------------------------------------------------------

        MDSPAN_INLINE_FUNCTION_DEFAULTED MDSPAN_IMPL_CONSTEXPR_14_DEFAULTED mapping &
        operator=(mapping const &) noexcept = default;

        MDSPAN_INLINE_FUNCTION constexpr const extents_type &extents() const noexcept {
#if defined(MDSPAN_IMPL_USE_ATTRIBUTE_NO_UNIQUE_ADDRESS)
            return m_members.first();
#else
            return this->base_t::ref().first();
#endif
        }

        MDSPAN_INLINE_FUNCTION
        constexpr std::array<index_type, extents_type::rank()> strides() const noexcept {
            return deduction_workaround_impl::return_strides(strides_storage());
        }

        MDSPAN_INLINE_FUNCTION
        constexpr index_type required_span_size() const noexcept {
            index_type span_size = 1;
            // using int here to avoid warning about pointless comparison to 0
            for (int r = 0; r < static_cast<int>(extents_type::rank()); r++) {
                // Return early if any of the extents are zero
                if (extents().extent(r) == 0)
                    return 0;
                span_size += (static_cast<index_type>(extents().extent(r) - 1) * strides_storage()[r]);
            }
            return span_size;
        }

        MDSPAN_TEMPLATE_REQUIRES(
            class... Indices,
            /* requires */ (
                sizeof...(Indices) == Extents::rank() && (detail::are_valid_indices<index_type, Indices...>())
            )
        )
        MDSPAN_FORCE_INLINE_FUNCTION
        constexpr index_type operator()(Indices... idxs) const noexcept {
#if !defined(NDEBUG)
            detail::check_all_indices(this->extents(), idxs...);
#endif // ! NDEBUG
            return static_cast<index_type>(
                deduction_workaround_impl::_call_op_impl(*this, static_cast<index_type>(idxs)...)
            );
        }

        MDSPAN_INLINE_FUNCTION static constexpr bool is_always_unique() noexcept { return true; }
        MDSPAN_INLINE_FUNCTION static constexpr bool is_always_exhaustive() noexcept { return false; }
        MDSPAN_INLINE_FUNCTION static constexpr bool is_always_strided() noexcept { return true; }

        MDSPAN_INLINE_FUNCTION static constexpr bool is_unique() noexcept { return true; }

      private:
        MDSPAN_INLINE_FUNCTION
        constexpr bool exhaustive_for_nonzero_span_size() const {
            return required_span_size() == get_size(extents(), std::make_index_sequence<extents_type::rank()>());
        }

        MDSPAN_INLINE_FUNCTION
        constexpr bool is_exhaustive_impl(detail::with_rank<0>) const { return true; }
        MDSPAN_INLINE_FUNCTION
        constexpr bool is_exhaustive_impl(detail::with_rank<1>) const {
            if (required_span_size() != static_cast<index_type>(0)) {
                return exhaustive_for_nonzero_span_size();
            }
            return stride(0) == 1;
        }
        template <std::size_t N>
        MDSPAN_INLINE_FUNCTION constexpr bool is_exhaustive_impl(detail::with_rank<N>) const {
            if (required_span_size() != static_cast<index_type>(0)) {
                return exhaustive_for_nonzero_span_size();
            }

            rank_type r_largest = 0;
            for (rank_type r = 1; r < extents_type::rank(); r++) {
                if (stride(r) > stride(r_largest)) {
                    r_largest = r;
                }
            }
            for (rank_type r = 0; r < extents_type::rank(); r++) {
                if (extents().extent(r) == 0 && r != r_largest) {
                    return false;
                }
            }
            return true;
        }

      public:
        MDSPAN_INLINE_FUNCTION MDSPAN_IMPL_CONSTEXPR_14 bool is_exhaustive() const noexcept {
            return is_exhaustive_impl(detail::with_rank<extents_type::rank()>{});
        }
        MDSPAN_INLINE_FUNCTION static constexpr bool is_strided() noexcept { return true; }

        MDSPAN_INLINE_FUNCTION
        constexpr index_type stride(rank_type r) const noexcept { return strides_storage()[r]; }

#if !(defined(MDSPAN_IMPL_USE_CONCEPTS) && MDSPAN_HAS_CXX_20)
        MDSPAN_TEMPLATE_REQUIRES(
            class StridedLayoutMapping,
            /* requires */ (
                detail::is_mapping_of<typename StridedLayoutMapping::layout_type, StridedLayoutMapping> &&
                (extents_type::rank() == StridedLayoutMapping::extents_type::rank()) &&
                StridedLayoutMapping::is_always_strided()
            )
        )
#else
        template<class StridedLayoutMapping>
    requires(
         detail::layout_mapping_alike<StridedLayoutMapping> &&
         (extents_type::rank() == StridedLayoutMapping::extents_type::rank()) &&
         StridedLayoutMapping::is_always_strided()
    )
#endif
        MDSPAN_INLINE_FUNCTION
        friend constexpr bool operator==(const mapping &x, const StridedLayoutMapping &y) noexcept {
            return (x.extents() == y.extents()) &&
                   (deduction_workaround_impl::offset(y) ==
                    static_cast<typename StridedLayoutMapping::index_type>(0)) &&
                   detail::rankwise_equal(detail::with_rank<extents_type::rank()>{}, x, y, detail::stride);
        }

        // This one is not technically part of the proposal. Just here to make implementation a bit more optimal
        // hopefully
        MDSPAN_TEMPLATE_REQUIRES(
            class OtherExtents,
            /* requires */ ((extents_type::rank() == OtherExtents::rank()))
        )
        MDSPAN_INLINE_FUNCTION
        friend constexpr bool operator==(mapping const &lhs, mapping<OtherExtents> const &rhs) noexcept {
            return deduction_workaround_impl::_eq_impl(lhs, rhs);
        }

#if !MDSPAN_HAS_CXX_20
        MDSPAN_TEMPLATE_REQUIRES(
            class StridedLayoutMapping,
            /* requires */ (
                detail::is_mapping_of<typename StridedLayoutMapping::layout_type, StridedLayoutMapping> &&
                (extents_type::rank() == StridedLayoutMapping::extents_type::rank()) &&
                StridedLayoutMapping::is_always_strided()
            )
        )
        MDSPAN_INLINE_FUNCTION
        friend constexpr bool operator!=(const mapping &x, const StridedLayoutMapping &y) noexcept { return !(x == y); }

        MDSPAN_TEMPLATE_REQUIRES(
            class OtherExtents,
            /* requires */ ((extents_type::rank() == OtherExtents::rank()))
        )
        MDSPAN_INLINE_FUNCTION
        friend constexpr bool operator!=(mapping const &lhs, mapping<OtherExtents> const &rhs) noexcept {
            return deduction_workaround_impl::_not_eq_impl(lhs, rhs);
        }
#endif

        // [mdspan.submdspan.mapping], submdspan mapping specialization
        template <class... SliceSpecifiers>
        MDSPAN_INLINE_FUNCTION constexpr auto submdspan_mapping_impl(SliceSpecifiers... slices) const;

        template <class... SliceSpecifiers>
        MDSPAN_INLINE_FUNCTION friend constexpr auto submdspan_mapping(const mapping &src, SliceSpecifiers... slices) {
            return src.submdspan_mapping_impl(slices...);
        }
    };
};

namespace detail {

    template <class Layout, class Extents, class Mapping>
    MDSPAN_INLINE_FUNCTION constexpr void validate_strides(with_rank<0>, Layout, const Extents &, const Mapping &) {}

    template <std::size_t N, class Layout, class Extents, class Mapping>
    MDSPAN_INLINE_FUNCTION constexpr void
    validate_strides(with_rank<N>, Layout, const Extents &ext, const Mapping &other) {
        static_assert(
            std::is_same<typename Mapping::layout_type, layout_stride>::value &&
                (std::is_same<Layout, layout_left>::value || std::is_same<Layout, layout_right>::value),
            "This function is only intended to validate construction of "
            "a layout_left or layout_right mapping from a layout_stride mapping."
        );

        constexpr auto is_left = std::is_same<Layout, layout_left>::value;

        typename Extents::index_type expected_stride = 1;

        for (std::size_t r = 0; r < N; r++) {
            const std::size_t s = is_left ? r : N - 1 - r;

            MDSPAN_IMPL_PRECONDITION(
                common_integral_compare(expected_stride, other.stride(s)) && "invalid strides for layout_{left,right}"
            );

            expected_stride *= ext.extent(s);
        }
    }

} // namespace detail
} // end namespace MDSPAN_IMPL_STANDARD_NAMESPACE
// END_FILE_INCLUDE: mdspan/include/experimental/__p0009_bits/layout_stride.hpp
#if MDSPAN_HAS_CXX_17
// BEGIN_FILE_INCLUDE: mdspan/include/experimental/__p2642_bits/layout_padded_fwd.hpp
//@HEADER
//  ************************************************************************
//
//                         Kokkos v. 4.0
//        Copyright (2022) National Technology & Engineering
//                Solutions of Sandia, LLC (NTESS).
//
//  Under the terms of Contract DE-NA0003525 with NTESS,
//  the U.S. Government retains certain rights in this software.
//
//  Part of Kokkos, under the Apache License v2.0 with LLVM Exceptions.
//
//  SPDX-License-Identifier: Apache-2.0 WITH LLVM-exception
//
//@HEADER

#include <cassert>

namespace MDSPAN_IMPL_STANDARD_NAMESPACE {
namespace MDSPAN_IMPL_PROPOSED_NAMESPACE {

    template <size_t padding_value = dynamic_extent>
    struct layout_left_padded {
        template <class Extents>
        class mapping;
    };

    template <size_t padding_value = dynamic_extent>
    struct layout_right_padded {
        template <class Extents>
        class mapping;
    };

    namespace detail {
        // The layout_padded_constants structs are only useful if rank > 1, otherwise they may wrap
        template <class Layout, class ExtentsType>
        struct layout_padded_constants;

        template <class ExtentsType, size_t PaddingStride>
        struct layout_padded_constants<layout_left_padded<PaddingStride>, ExtentsType> {
            using rank_type = typename ExtentsType::rank_type;
            static constexpr rank_type padded_stride_idx = 1;
            static constexpr rank_type extent_to_pad_idx = 0;
        };

        template <class ExtentsType, size_t PaddingStride>
        struct layout_padded_constants<layout_right_padded<PaddingStride>, ExtentsType> {
            using rank_type = typename ExtentsType::rank_type;
            static constexpr rank_type padded_stride_idx = ExtentsType::rank() - 2;
            static constexpr rank_type extent_to_pad_idx = ExtentsType::rank() - 1;
        };

        template <class Layout>
        struct is_layout_left_padded : std::false_type {};

        template <size_t PaddingStride>
        struct is_layout_left_padded<layout_left_padded<PaddingStride>> : std::true_type {};

        template <class Mapping, class Enabled = void>
        struct is_layout_left_padded_mapping : std::false_type {};

        template <class Mapping>
        struct is_layout_left_padded_mapping<
            Mapping, std::enable_if_t<std::is_same<
                         Mapping, typename layout_left_padded<Mapping::padding_value>::template mapping<
                                      typename Mapping::extents_type>>::value>> : std::true_type {};

        template <class Layout>
        struct is_layout_right_padded : std::false_type {};

        template <size_t PaddingStride>
        struct is_layout_right_padded<layout_right_padded<PaddingStride>> : std::true_type {};

        template <class Mapping, class Enabled = void>
        struct is_layout_right_padded_mapping : std::false_type {};

        template <class Mapping>
        struct is_layout_right_padded_mapping<
            Mapping, std::enable_if_t<std::is_same<
                         Mapping, typename layout_right_padded<Mapping::padding_value>::template mapping<
                                      typename Mapping::extents_type>>::value>> : std::true_type {};

        template <class LayoutExtentsType, class PaddedLayoutMappingType>
        MDSPAN_INLINE_FUNCTION constexpr void
        check_padded_layout_converting_constructor_mandates(MDSPAN_IMPL_STANDARD_NAMESPACE::detail::with_rank<0>) {}

        template <class LayoutExtentsType, class PaddedLayoutMappingType>
        MDSPAN_INLINE_FUNCTION constexpr void
        check_padded_layout_converting_constructor_mandates(MDSPAN_IMPL_STANDARD_NAMESPACE::detail::with_rank<1>) {}

        template <class LayoutExtentsType, class PaddedLayoutMappingType, std::size_t N>
        MDSPAN_INLINE_FUNCTION constexpr void
        check_padded_layout_converting_constructor_mandates(MDSPAN_IMPL_STANDARD_NAMESPACE::detail::with_rank<N>) {
            using extents_type = typename PaddedLayoutMappingType::extents_type;
            constexpr auto padding_value = PaddedLayoutMappingType::padding_value;
            constexpr auto idx = layout_padded_constants<
                typename PaddedLayoutMappingType::layout_type, LayoutExtentsType>::extent_to_pad_idx;

            constexpr auto statically_determinable = (LayoutExtentsType::static_extent(idx) != dynamic_extent) &&
                                                     (extents_type::static_extent(idx) != dynamic_extent) &&
                                                     (padding_value != dynamic_extent);

            static_assert(
                !statically_determinable ||
                    (padding_value == 0 ? LayoutExtentsType::static_extent(idx) == 0
                                        : LayoutExtentsType::static_extent(idx) % padding_value == 0),
                ""
            );
        }

        template <typename ExtentsType, typename OtherMapping>
        MDSPAN_INLINE_FUNCTION constexpr void check_padded_layout_converting_constructor_preconditions(
            MDSPAN_IMPL_STANDARD_NAMESPACE::detail::with_rank<0>, const OtherMapping &
        ) {}
        template <typename ExtentsType, typename OtherMapping>
        MDSPAN_INLINE_FUNCTION constexpr void check_padded_layout_converting_constructor_preconditions(
            MDSPAN_IMPL_STANDARD_NAMESPACE::detail::with_rank<1>, const OtherMapping &
        ) {}
        template <typename ExtentsType, typename OtherMapping, std::size_t N>
        MDSPAN_INLINE_FUNCTION constexpr void check_padded_layout_converting_constructor_preconditions(
            MDSPAN_IMPL_STANDARD_NAMESPACE::detail::with_rank<N>, const OtherMapping &other_mapping
        ) {
            constexpr auto padded_stride_idx =
                layout_padded_constants<typename OtherMapping::layout_type, ExtentsType>::padded_stride_idx;
            constexpr auto extent_to_pad_idx =
                layout_padded_constants<typename OtherMapping::layout_type, ExtentsType>::extent_to_pad_idx;
            MDSPAN_IMPL_PRECONDITION(
                other_mapping.stride(padded_stride_idx) == other_mapping.extents().extent(extent_to_pad_idx)
            );
        }

    } // namespace detail
} // namespace MDSPAN_IMPL_PROPOSED_NAMESPACE
} // namespace MDSPAN_IMPL_STANDARD_NAMESPACE
// END_FILE_INCLUDE: mdspan/include/experimental/__p2642_bits/layout_padded_fwd.hpp
#endif

namespace MDSPAN_IMPL_STANDARD_NAMESPACE {

//==============================================================================
template <class Extents>
class layout_right::mapping {
  public:
    using extents_type = Extents;
    using index_type = typename extents_type::index_type;
    using size_type = typename extents_type::size_type;
    using rank_type = typename extents_type::rank_type;
    using layout_type = layout_right;

  private:
    static_assert(
        detail::impl_is_extents_v<extents_type>, MDSPAN_IMPL_STANDARD_NAMESPACE_STRING
        "::layout_right::mapping must be instantiated with a specialization of " MDSPAN_IMPL_STANDARD_NAMESPACE_STRING
        "::extents."
    );

    template <class>
    friend class mapping;

    // i0+(i1 + E(1)*(i2 + E(2)*i3))
    template <size_t r, size_t Rank>
    struct rank_count {};

    template <size_t r, size_t Rank, class I, class... Indices>
    MDSPAN_IMPL_HOST_DEVICE constexpr index_type
    compute_offset(index_type offset, rank_count<r, Rank>, const I &i, Indices... idx) const {
        return compute_offset(offset * m_extents.extent(r) + i, rank_count<r + 1, Rank>(), idx...);
    }

    template <class I, class... Indices>
    MDSPAN_IMPL_HOST_DEVICE constexpr index_type
    compute_offset(rank_count<0, extents_type::rank()>, const I &i, Indices... idx) const {
        return compute_offset(i, rank_count<1, extents_type::rank()>(), idx...);
    }

    MDSPAN_IMPL_HOST_DEVICE
    constexpr index_type compute_offset(size_t offset, rank_count<extents_type::rank(), extents_type::rank()>) const {
        return static_cast<index_type>(offset);
    }

    MDSPAN_IMPL_HOST_DEVICE
    constexpr index_type compute_offset(rank_count<0, 0>) const { return 0; }

  public:
    //--------------------------------------------------------------------------------

    MDSPAN_INLINE_FUNCTION_DEFAULTED constexpr mapping() noexcept = default;
    MDSPAN_INLINE_FUNCTION_DEFAULTED constexpr mapping(mapping const &) noexcept = default;

    MDSPAN_IMPL_HOST_DEVICE
    constexpr mapping(extents_type const &exts) noexcept : m_extents(exts) {}

    MDSPAN_TEMPLATE_REQUIRES(
        class OtherExtents,
        /* requires */ (MDSPAN_IMPL_TRAIT(std::is_constructible, extents_type, OtherExtents))
    )
    MDSPAN_CONDITIONAL_EXPLICIT((!std::is_convertible<OtherExtents, extents_type>::value)) // needs two () due to comma
    MDSPAN_INLINE_FUNCTION MDSPAN_IMPL_CONSTEXPR_14
    mapping(mapping<OtherExtents> const &other) noexcept // NOLINT(google-explicit-constructor)
        : m_extents(other.extents()) {
        /*
         * TODO: check precondition
         * other.required_span_size() is a representable value of type index_type
         */
    }

    MDSPAN_TEMPLATE_REQUIRES(
        class OtherExtents,
        /* requires */ (
            MDSPAN_IMPL_TRAIT(std::is_constructible, extents_type, OtherExtents) && (extents_type::rank() <= 1)
        )
    )
    MDSPAN_CONDITIONAL_EXPLICIT((!std::is_convertible<OtherExtents, extents_type>::value)) // needs two () due to comma
    MDSPAN_INLINE_FUNCTION MDSPAN_IMPL_CONSTEXPR_14
    mapping(layout_left::mapping<OtherExtents> const &other) noexcept // NOLINT(google-explicit-constructor)
        : m_extents(other.extents()) {
        /*
         * TODO: check precondition
         * other.required_span_size() is a representable value of type index_type
         */
    }

    /**
     * Converting constructor from `layout_right_padded::mapping`.
     *
     * This overload participates in overload resolution only if Mapping is a layout_right_padded mapping and
     * extents_type is constructible from Mapping::extents_type.
     *
     * \note There is currently a difference from p2642r2, where this function is specified as taking
     * `layout_right_padded< padding_value >::mapping< Extents>`. However, this makes `padding_value` non-deducible.
     */
#if MDSPAN_HAS_CXX_17
    MDSPAN_TEMPLATE_REQUIRES(
        class Mapping,
        /* requires */ (MDSPAN_IMPL_PROPOSED_NAMESPACE::detail::is_layout_right_padded_mapping<Mapping>::value
                            &&std::is_constructible_v<extents_type, typename Mapping::extents_type>)
    )
    MDSPAN_CONDITIONAL_EXPLICIT((!std::is_convertible_v<typename Mapping::extents_type, extents_type>))
    MDSPAN_INLINE_FUNCTION constexpr mapping(const Mapping &other) noexcept : m_extents(other.extents()) {
        MDSPAN_IMPL_PROPOSED_NAMESPACE::detail::check_padded_layout_converting_constructor_mandates<
            extents_type, Mapping>(detail::with_rank<extents_type::rank()>{});
        MDSPAN_IMPL_PROPOSED_NAMESPACE::detail::check_padded_layout_converting_constructor_preconditions<extents_type>(
            detail::with_rank<extents_type::rank()>{}, other
        );
    }
#endif

    MDSPAN_TEMPLATE_REQUIRES(
        class OtherExtents,
        /* requires */ (MDSPAN_IMPL_TRAIT(std::is_constructible, extents_type, OtherExtents))
    )
    MDSPAN_CONDITIONAL_EXPLICIT((extents_type::rank() > 0))
    MDSPAN_INLINE_FUNCTION MDSPAN_IMPL_CONSTEXPR_14
    mapping(layout_stride::mapping<OtherExtents> const &other) noexcept // NOLINT(google-explicit-constructor)
        : m_extents(other.extents()) {
        /*
         * TODO: check precondition
         * other.required_span_size() is a representable value of type index_type
         */
        detail::validate_strides(detail::with_rank<extents_type::rank()>{}, layout_right{}, m_extents, other);
    }

    MDSPAN_INLINE_FUNCTION_DEFAULTED MDSPAN_IMPL_CONSTEXPR_14_DEFAULTED mapping &
    operator=(mapping const &) noexcept = default;

    MDSPAN_INLINE_FUNCTION
    constexpr const extents_type &extents() const noexcept { return m_extents; }

    MDSPAN_INLINE_FUNCTION
    constexpr index_type required_span_size() const noexcept {
        index_type value = 1;
        for (rank_type r = 0; r != extents_type::rank(); ++r)
            value *= m_extents.extent(r);
        return value;
    }

    //--------------------------------------------------------------------------------

    MDSPAN_TEMPLATE_REQUIRES(
        class... Indices,
        /* requires */ (
            (sizeof...(Indices) == extents_type::rank()) && (detail::are_valid_indices<index_type, Indices...>())
        )
    )
    MDSPAN_IMPL_HOST_DEVICE
    constexpr index_type operator()(Indices... idxs) const noexcept {
#if !defined(NDEBUG)
        detail::check_all_indices(this->extents(), idxs...);
#endif // ! NDEBUG
        return compute_offset(rank_count<0, extents_type::rank()>(), static_cast<index_type>(idxs)...);
    }

    MDSPAN_INLINE_FUNCTION static constexpr bool is_always_unique() noexcept { return true; }
    MDSPAN_INLINE_FUNCTION static constexpr bool is_always_exhaustive() noexcept { return true; }
    MDSPAN_INLINE_FUNCTION static constexpr bool is_always_strided() noexcept { return true; }
    MDSPAN_INLINE_FUNCTION static constexpr bool is_unique() noexcept { return true; }
    MDSPAN_INLINE_FUNCTION static constexpr bool is_exhaustive() noexcept { return true; }
    MDSPAN_INLINE_FUNCTION static constexpr bool is_strided() noexcept { return true; }

    MDSPAN_INLINE_FUNCTION
    constexpr index_type stride(rank_type i) const noexcept
#if MDSPAN_HAS_CXX_20
        requires(Extents::rank() > 0)
#endif
    {
        index_type value = 1;
        for (rank_type r = extents_type::rank() - 1; r > i; r--)
            value *= m_extents.extent(r);
        return value;
    }

    MDSPAN_TEMPLATE_REQUIRES(
        class OtherExtents,
        /* requires */ (Extents::rank() == OtherExtents::rank())
    )
    MDSPAN_INLINE_FUNCTION
    friend constexpr bool operator==(mapping const &lhs, mapping<OtherExtents> const &rhs) noexcept {
        return lhs.extents() == rhs.extents();
    }

    // In C++ 20 the not equal exists if equal is found
#if !(MDSPAN_HAS_CXX_20)
    MDSPAN_TEMPLATE_REQUIRES(
        class OtherExtents,
        /* requires */ (Extents::rank() == OtherExtents::rank())
    )
    MDSPAN_INLINE_FUNCTION
    friend constexpr bool operator!=(mapping const &lhs, mapping<OtherExtents> const &rhs) noexcept {
        return lhs.extents() != rhs.extents();
    }
#endif

    // Not really public, but currently needed to implement fully constexpr useable submdspan:
    template <size_t N, class SizeType, size_t... E, size_t... Idx>
    MDSPAN_INLINE_FUNCTION constexpr index_type impl_get_stride(
        MDSPAN_IMPL_STANDARD_NAMESPACE::extents<SizeType, E...>, std::integer_sequence<size_t, Idx...>
    ) const {
        return MDSPAN_IMPL_FOLD_TIMES_RIGHT((Idx > N ? m_extents.template extent<Idx>() : 1), 1);
    }
    template <size_t N>
    MDSPAN_INLINE_FUNCTION constexpr index_type impl_stide() const noexcept {
        return impl_get_stride<N>(m_extents, std::make_index_sequence<extents_type::rank()>());
    }

  private:
    MDSPAN_IMPL_NO_UNIQUE_ADDRESS extents_type m_extents{};

    // [mdspan.submdspan.mapping], submdspan mapping specialization
    template <class... SliceSpecifiers>
    MDSPAN_INLINE_FUNCTION constexpr auto submdspan_mapping_impl(SliceSpecifiers... slices) const;

    template <class... SliceSpecifiers>
    MDSPAN_INLINE_FUNCTION friend constexpr auto submdspan_mapping(const mapping &src, SliceSpecifiers... slices) {
        return src.submdspan_mapping_impl(slices...);
    }
};

} // end namespace MDSPAN_IMPL_STANDARD_NAMESPACE
// END_FILE_INCLUDE: mdspan/include/experimental/__p0009_bits/layout_right.hpp

#include <stdexcept>
#include <string>
#include <type_traits>

namespace MDSPAN_IMPL_STANDARD_NAMESPACE {
template <
    class ElementType, class Extents, class LayoutPolicy = layout_right,
    class AccessorPolicy = default_accessor<ElementType>>
class mdspan {
  private:
    static_assert(
        detail::impl_is_extents_v<Extents>, MDSPAN_IMPL_STANDARD_NAMESPACE_STRING
        "::mdspan's Extents template parameter must be a specialization of " MDSPAN_IMPL_STANDARD_NAMESPACE_STRING
        "::extents."
    );
    static_assert(
        std::is_same<ElementType, typename AccessorPolicy::element_type>::value, MDSPAN_IMPL_STANDARD_NAMESPACE_STRING
        "::mdspan's ElementType template parameter must be the same as its AccessorPolicy::element_type."
    );

    // Workaround for non-deducibility of the index sequence template parameter if it's given at the top level
    template <class>
    struct deduction_workaround;

    template <size_t... Idxs>
    struct deduction_workaround<std::index_sequence<Idxs...>> {
        MDSPAN_FORCE_INLINE_FUNCTION static constexpr size_t size(mdspan const &self) noexcept {
            return MDSPAN_IMPL_FOLD_TIMES_RIGHT((self.mapping_ref().extents().extent(Idxs)), /* * ... * */ size_t(1));
        }
        MDSPAN_FORCE_INLINE_FUNCTION static constexpr bool empty(mdspan const &self) noexcept {
            return (self.rank() > 0) &&
                   MDSPAN_IMPL_FOLD_OR((self.mapping_ref().extents().extent(Idxs) == index_type(0)));
        }
        template <class ReferenceType, class SizeType, size_t N>
        MDSPAN_FORCE_INLINE_FUNCTION static constexpr ReferenceType
        callop(mdspan const &self, const std::array<SizeType, N> &indices) noexcept {
            return self.accessor_ref().access(self.ptr_ref(), self.mapping_ref()(indices[Idxs]...));
        }
#ifdef __cpp_lib_span
        template <class ReferenceType, class SizeType, size_t N>
        MDSPAN_FORCE_INLINE_FUNCTION static constexpr ReferenceType
        callop(mdspan const &self, const std::span<SizeType, N> &indices) noexcept {
            return self.accessor_ref().access(self.ptr_ref(), self.mapping_ref()(indices[Idxs]...));
        }
#endif
    };

  public:
    //--------------------------------------------------------------------------------
    // Domain and codomain types

    using extents_type = Extents;
    using layout_type = LayoutPolicy;
    using accessor_type = AccessorPolicy;
    using mapping_type = typename layout_type::template mapping<extents_type>;
    using element_type = ElementType;
    using value_type = std::remove_cv_t<element_type>;
    using index_type = typename extents_type::index_type;
    using size_type = typename extents_type::size_type;
    using rank_type = typename extents_type::rank_type;
    using data_handle_type = typename accessor_type::data_handle_type;
    using reference = typename accessor_type::reference;

    MDSPAN_INLINE_FUNCTION static constexpr size_t rank() noexcept { return extents_type::rank(); }
    MDSPAN_INLINE_FUNCTION static constexpr size_t rank_dynamic() noexcept { return extents_type::rank_dynamic(); }
    MDSPAN_INLINE_FUNCTION static constexpr size_t static_extent(size_t r) noexcept {
        return extents_type::static_extent(r);
    }
    MDSPAN_INLINE_FUNCTION constexpr index_type extent(size_t r) const noexcept {
        return mapping_ref().extents().extent(r);
    }

  private:
    // Can't use defaulted parameter in the deduction_workaround template because of a bug in MSVC warning C4348.
    using deduction_workaround_impl = deduction_workaround<std::make_index_sequence<extents_type::rank()>>;

    using map_acc_pair_t = detail::impl_compressed_pair<mapping_type, accessor_type>;

  public:
    //--------------------------------------------------------------------------------
    // [mdspan.basic.cons], mdspan constructors, assignment, and destructor

#if !MDSPAN_HAS_CXX_20
    MDSPAN_INLINE_FUNCTION_DEFAULTED constexpr mdspan() = default;
#else
    MDSPAN_INLINE_FUNCTION_DEFAULTED constexpr mdspan() requires(
        // nvhpc has a bug where using just rank_dynamic() here doesn't work ...
        (extents_type::rank_dynamic() > 0) && MDSPAN_IMPL_TRAIT(std::is_default_constructible, data_handle_type) &&
        MDSPAN_IMPL_TRAIT(std::is_default_constructible, mapping_type) &&
        MDSPAN_IMPL_TRAIT(std::is_default_constructible, accessor_type)
    ) = default;
#endif
    MDSPAN_INLINE_FUNCTION_DEFAULTED constexpr mdspan(const mdspan &) = default;
    MDSPAN_INLINE_FUNCTION_DEFAULTED constexpr mdspan(mdspan &&) = default;

    MDSPAN_TEMPLATE_REQUIRES(
        class... SizeTypes,
        /* requires */ (
            ((sizeof...(SizeTypes) == rank()) || (sizeof...(SizeTypes) == rank_dynamic())) &&
            (detail::are_valid_indices<index_type, SizeTypes...>()) &&
            MDSPAN_IMPL_TRAIT(std::is_constructible, mapping_type, extents_type) &&
            MDSPAN_IMPL_TRAIT(std::is_default_constructible, accessor_type)
        )
    )
    MDSPAN_INLINE_FUNCTION
    explicit constexpr mdspan(data_handle_type p, SizeTypes... dynamic_extents)
        // TODO @proposal-bug shouldn't I be allowed to do `move(p)` here?
        : m_members(
              std::move(p),
              map_acc_pair_t(
                  mapping_type(extents_type(static_cast<index_type>(std::move(dynamic_extents))...)), accessor_type()
              )
          ) {}

    MDSPAN_TEMPLATE_REQUIRES(
        class SizeType, size_t N,
        /* requires */
        (MDSPAN_IMPL_TRAIT(std::is_convertible, const SizeType &, index_type) &&
         MDSPAN_IMPL_TRAIT(std::is_nothrow_constructible, index_type, const SizeType &) &&
         ((N == rank()) || (N == rank_dynamic())) &&
         MDSPAN_IMPL_TRAIT(std::is_constructible, mapping_type, extents_type) &&
         MDSPAN_IMPL_TRAIT(std::is_default_constructible, accessor_type))
    )
    MDSPAN_CONDITIONAL_EXPLICIT(N != rank_dynamic())
    MDSPAN_INLINE_FUNCTION
    constexpr mdspan(data_handle_type p, const std::array<SizeType, N> &dynamic_extents)
        : m_members(std::move(p), map_acc_pair_t(mapping_type(extents_type(dynamic_extents)), accessor_type())) {}

#ifdef __cpp_lib_span
    MDSPAN_TEMPLATE_REQUIRES(
        class SizeType, size_t N,
        /* requires */
        (MDSPAN_IMPL_TRAIT(std::is_convertible, const SizeType &, index_type) &&
         MDSPAN_IMPL_TRAIT(std::is_nothrow_constructible, index_type, const SizeType &) &&
         ((N == rank()) || (N == rank_dynamic())) &&
         MDSPAN_IMPL_TRAIT(std::is_constructible, mapping_type, extents_type) &&
         MDSPAN_IMPL_TRAIT(std::is_default_constructible, accessor_type))
    )
    MDSPAN_CONDITIONAL_EXPLICIT(N != rank_dynamic())
    MDSPAN_INLINE_FUNCTION
    constexpr mdspan(data_handle_type p, std::span<SizeType, N> dynamic_extents)
        : m_members(
              std::move(p), map_acc_pair_t(mapping_type(extents_type(as_const(dynamic_extents))), accessor_type())
          ) {}
#endif

    MDSPAN_FUNCTION_REQUIRES(
        (MDSPAN_INLINE_FUNCTION constexpr), mdspan, (data_handle_type p, const extents_type &exts), ,
        /* requires */
        (MDSPAN_IMPL_TRAIT(std::is_default_constructible, accessor_type) &&
         MDSPAN_IMPL_TRAIT(std::is_constructible, mapping_type, const extents_type &))
    )
        : m_members(std::move(p), map_acc_pair_t(mapping_type(exts), accessor_type())) {}

    MDSPAN_FUNCTION_REQUIRES(
        (MDSPAN_INLINE_FUNCTION constexpr), mdspan, (data_handle_type p, const mapping_type &m), ,
        /* requires */ (MDSPAN_IMPL_TRAIT(std::is_default_constructible, accessor_type))
    )
        : m_members(std::move(p), map_acc_pair_t(m, accessor_type())) {}

    MDSPAN_INLINE_FUNCTION
    constexpr mdspan(data_handle_type p, const mapping_type &m, const accessor_type &a)
        : m_members(std::move(p), map_acc_pair_t(m, a)) {}

    MDSPAN_TEMPLATE_REQUIRES(
        class OtherElementType, class OtherExtents, class OtherLayoutPolicy, class OtherAccessor,
        /* requires */
        (MDSPAN_IMPL_TRAIT(
             std::is_constructible, mapping_type, const typename OtherLayoutPolicy::template mapping<OtherExtents> &
         ) &&
         MDSPAN_IMPL_TRAIT(std::is_constructible, accessor_type, const OtherAccessor &))
    )
    MDSPAN_CONDITIONAL_EXPLICIT(
        !MDSPAN_IMPL_TRAIT(
            std::is_convertible, const typename OtherLayoutPolicy::template mapping<OtherExtents> &, mapping_type
        ) ||
        !MDSPAN_IMPL_TRAIT(std::is_convertible, const OtherAccessor &, accessor_type)
    )
    MDSPAN_INLINE_FUNCTION
    constexpr mdspan(const mdspan<OtherElementType, OtherExtents, OtherLayoutPolicy, OtherAccessor> &other)
        : m_members(other.ptr_ref(), map_acc_pair_t(other.mapping_ref(), other.accessor_ref())) {
        static_assert(
            MDSPAN_IMPL_TRAIT(std::is_constructible, data_handle_type, typename OtherAccessor::data_handle_type),
            "Incompatible data_handle_type for mdspan construction"
        );
        static_assert(
            MDSPAN_IMPL_TRAIT(std::is_constructible, extents_type, OtherExtents),
            "Incompatible extents for mdspan construction"
        );
        /*
         * TODO: Check precondition
         * For each rank index r of extents_type, static_extent(r) == dynamic_extent || static_extent(r) ==
         * other.extent(r) is true.
         */
    }

    /* Might need this on NVIDIA?
    MDSPAN_INLINE_FUNCTION_DEFAULTED
    ~mdspan() = default;
    */

    MDSPAN_INLINE_FUNCTION_DEFAULTED MDSPAN_IMPL_CONSTEXPR_14_DEFAULTED mdspan &operator=(const mdspan &) = default;
    MDSPAN_INLINE_FUNCTION_DEFAULTED MDSPAN_IMPL_CONSTEXPR_14_DEFAULTED mdspan &operator=(mdspan &&) = default;

    //--------------------------------------------------------------------------------
    // [mdspan.basic.mapping], mdspan mapping domain multidimensional index to access codomain element

    MDSPAN_TEMPLATE_REQUIRES(
        class... SizeTypes,
        /* requires */ (
            extents_type::rank() == sizeof...(SizeTypes) && (detail::are_valid_indices<index_type, SizeTypes...>())
        )
    )
    constexpr reference at(SizeTypes... indices) const {
        size_t r = 0;
        for (const auto &index : {indices...}) {
            if (is_index_oor(index, mapping_ref().extents().extent(r))) {
                throw std::out_of_range(
                    "mdspan::at(...," + std::to_string(index) + ",...) out-of-range at rank index " +
                    std::to_string(r) + " for mdspan with extent {...," +
                    std::to_string(mapping_ref().extents().extent(r)) + ",...}"
                );
            }
            ++r;
        }
        return accessor_ref().access(ptr_ref(), mapping_ref()(static_cast<index_type>(std::move(indices))...));
    }

    MDSPAN_TEMPLATE_REQUIRES(
        class SizeType,
        /* requires */ (
            MDSPAN_IMPL_TRAIT(std::is_convertible, const SizeType &, index_type) &&
            MDSPAN_IMPL_TRAIT(std::is_nothrow_constructible, index_type, const SizeType &)
        )
    )
    constexpr reference at(const std::array<SizeType, rank()> &indices) const {
        for (size_t r = 0; r < indices.size(); ++r) {
            if (is_index_oor(indices[r], mapping_ref().extents().extent(r))) {
                throw std::out_of_range(
                    "mdspan::at({...," + std::to_string(indices[r]) + ",...}) out-of-range at rank index " +
                    std::to_string(r) + " for mdspan with extent {...," +
                    std::to_string(mapping_ref().extents().extent(r)) + ",...}"
                );
            }
        }
        return deduction_workaround_impl::template callop<reference>(*this, indices);
    }

#ifdef __cpp_lib_span
    MDSPAN_TEMPLATE_REQUIRES(
        class SizeType,
        /* requires */ (
            MDSPAN_IMPL_TRAIT(std::is_convertible, const SizeType &, index_type) &&
            MDSPAN_IMPL_TRAIT(std::is_nothrow_constructible, index_type, const SizeType &)
        )
    )
    constexpr reference at(std::span<SizeType, rank()> indices) const {
        for (size_t r = 0; r < indices.size(); ++r) {
            if (is_index_oor(indices[r], mapping_ref().extents().extent(r))) {
                throw std::out_of_range(
                    "mdspan::at({...," + std::to_string(indices[r]) + ",...}) out-of-range at rank index " +
                    std::to_string(r) + " for mdspan with extent {...," +
                    std::to_string(mapping_ref().extents().extent(r)) + ",...}"
                );
            }
        }
        return deduction_workaround_impl::template callop<reference>(*this, indices);
    }
#endif // __cpp_lib_span

#if MDSPAN_USE_BRACKET_OPERATOR
    MDSPAN_TEMPLATE_REQUIRES(
        class... SizeTypes,
        /* requires */ (
            extents_type::rank() == sizeof...(SizeTypes) && (detail::are_valid_indices<index_type, SizeTypes...>())
        )
    )
    MDSPAN_FORCE_INLINE_FUNCTION
    constexpr reference operator[](SizeTypes... indices) const {
        return accessor_ref().access(ptr_ref(), mapping_ref()(static_cast<index_type>(std::move(indices))...));
    }
#endif

    MDSPAN_TEMPLATE_REQUIRES(
        class SizeType,
        /* requires */ (
            MDSPAN_IMPL_TRAIT(std::is_convertible, const SizeType &, index_type) &&
            MDSPAN_IMPL_TRAIT(std::is_nothrow_constructible, index_type, const SizeType &)
        )
    )
    MDSPAN_FORCE_INLINE_FUNCTION
    constexpr reference operator[](const std::array<SizeType, rank()> &indices) const {
        return deduction_workaround_impl::template callop<reference>(*this, indices);
    }

#ifdef __cpp_lib_span
    MDSPAN_TEMPLATE_REQUIRES(
        class SizeType,
        /* requires */ (
            MDSPAN_IMPL_TRAIT(std::is_convertible, const SizeType &, index_type) &&
            MDSPAN_IMPL_TRAIT(std::is_nothrow_constructible, index_type, const SizeType &)
        )
    )
    MDSPAN_FORCE_INLINE_FUNCTION
    constexpr reference operator[](std::span<SizeType, rank()> indices) const {
        return deduction_workaround_impl::template callop<reference>(*this, indices);
    }
#endif // __cpp_lib_span

#if !MDSPAN_USE_BRACKET_OPERATOR
    MDSPAN_TEMPLATE_REQUIRES(
        class Index,
        /* requires */ (
            MDSPAN_IMPL_TRAIT(std::is_convertible, Index, index_type) &&
            MDSPAN_IMPL_TRAIT(std::is_nothrow_constructible, index_type, Index) && extents_type::rank() == 1
        )
    )
    MDSPAN_FORCE_INLINE_FUNCTION
    constexpr reference operator[](Index idx) const {
        return accessor_ref().access(ptr_ref(), mapping_ref()(static_cast<index_type>(std::move(idx))));
    }
#endif

#if MDSPAN_USE_PAREN_OPERATOR
    MDSPAN_TEMPLATE_REQUIRES(
        class... SizeTypes,
        /* requires */ (
            extents_type::rank() == sizeof...(SizeTypes) && (detail::are_valid_indices<index_type, SizeTypes...>())
        )
    )
    MDSPAN_FORCE_INLINE_FUNCTION
    constexpr reference operator()(SizeTypes... indices) const {
        return accessor_ref().access(ptr_ref(), mapping_ref()(static_cast<index_type>(std::move(indices))...));
    }

    MDSPAN_TEMPLATE_REQUIRES(
        class SizeType,
        /* requires */ (
            MDSPAN_IMPL_TRAIT(std::is_convertible, const SizeType &, index_type) &&
            MDSPAN_IMPL_TRAIT(std::is_nothrow_constructible, index_type, const SizeType &)
        )
    )
    MDSPAN_FORCE_INLINE_FUNCTION
    constexpr reference operator()(const std::array<SizeType, rank()> &indices) const {
        return deduction_workaround_impl::template callop<reference>(*this, indices);
    }

#ifdef __cpp_lib_span
    MDSPAN_TEMPLATE_REQUIRES(
        class SizeType,
        /* requires */ (
            MDSPAN_IMPL_TRAIT(std::is_convertible, const SizeType &, index_type) &&
            MDSPAN_IMPL_TRAIT(std::is_nothrow_constructible, index_type, const SizeType &)
        )
    )
    MDSPAN_FORCE_INLINE_FUNCTION
    constexpr reference operator()(std::span<SizeType, rank()> indices) const {
        return deduction_workaround_impl::template callop<reference>(*this, indices);
    }
#endif // __cpp_lib_span
#endif // MDSPAN_USE_PAREN_OPERATOR

    MDSPAN_INLINE_FUNCTION constexpr size_type size() const noexcept {
        return static_cast<size_type>(deduction_workaround_impl::size(*this));
    }

    MDSPAN_INLINE_FUNCTION constexpr bool empty() const noexcept { return deduction_workaround_impl::empty(*this); }

    MDSPAN_INLINE_FUNCTION
    friend constexpr void swap(mdspan &x, mdspan &y) noexcept {
// can't call the std::swap inside on HIP
#if !defined(MDSPAN_IMPL_HAS_HIP) && !defined(MDSPAN_IMPL_HAS_CUDA)
        using std::swap;
        swap(x.ptr_ref(), y.ptr_ref());
        swap(x.mapping_ref(), y.mapping_ref());
        swap(x.accessor_ref(), y.accessor_ref());
#else
        mdspan tmp = y;
        y = x;
        x = tmp;
#endif
    }

    //--------------------------------------------------------------------------------
    // [mdspan.basic.domobs], mdspan observers of the domain multidimensional index space

    MDSPAN_INLINE_FUNCTION constexpr const extents_type &extents() const noexcept { return mapping_ref().extents(); }
    MDSPAN_INLINE_FUNCTION constexpr const data_handle_type &data_handle() const noexcept { return ptr_ref(); }
    MDSPAN_INLINE_FUNCTION constexpr const mapping_type &mapping() const noexcept { return mapping_ref(); }
    MDSPAN_INLINE_FUNCTION constexpr const accessor_type &accessor() const noexcept { return accessor_ref(); }

    //--------------------------------------------------------------------------------
    // [mdspan.basic.obs], mdspan observers of the mapping

    MDSPAN_INLINE_FUNCTION static constexpr bool is_always_unique() { return mapping_type::is_always_unique(); }
    MDSPAN_INLINE_FUNCTION static constexpr bool is_always_exhaustive() { return mapping_type::is_always_exhaustive(); }
    MDSPAN_INLINE_FUNCTION static constexpr bool is_always_strided() { return mapping_type::is_always_strided(); }

    MDSPAN_INLINE_FUNCTION constexpr bool is_unique() const { return mapping_ref().is_unique(); }
    MDSPAN_INLINE_FUNCTION constexpr bool is_exhaustive() const { return mapping_ref().is_exhaustive(); }
    MDSPAN_INLINE_FUNCTION constexpr bool is_strided() const { return mapping_ref().is_strided(); }
    MDSPAN_INLINE_FUNCTION constexpr index_type stride(size_t r) const { return mapping_ref().stride(r); }

  private:
    detail::impl_compressed_pair<data_handle_type, map_acc_pair_t> m_members{};

    MDSPAN_FORCE_INLINE_FUNCTION MDSPAN_IMPL_CONSTEXPR_14 data_handle_type &ptr_ref() noexcept {
        return m_members.first();
    }
    MDSPAN_FORCE_INLINE_FUNCTION constexpr data_handle_type const &ptr_ref() const noexcept {
        return m_members.first();
    }
    MDSPAN_FORCE_INLINE_FUNCTION MDSPAN_IMPL_CONSTEXPR_14 mapping_type &mapping_ref() noexcept {
        return m_members.second().first();
    }
    MDSPAN_FORCE_INLINE_FUNCTION constexpr mapping_type const &mapping_ref() const noexcept {
        return m_members.second().first();
    }
    MDSPAN_FORCE_INLINE_FUNCTION MDSPAN_IMPL_CONSTEXPR_14 accessor_type &accessor_ref() noexcept {
        return m_members.second().second();
    }
    MDSPAN_FORCE_INLINE_FUNCTION constexpr accessor_type const &accessor_ref() const noexcept {
        return m_members.second().second();
    }

    MDSPAN_TEMPLATE_REQUIRES(
        class SizeType,
        /* requires */ (
            MDSPAN_IMPL_TRAIT(std::is_convertible, const SizeType &, index_type) &&
            MDSPAN_IMPL_TRAIT(std::is_nothrow_constructible, index_type, const SizeType &)
        )
    )
    MDSPAN_FORCE_INLINE_FUNCTION constexpr bool is_index_oor(SizeType index, index_type extent) const noexcept {
        // Check for negative indices
        if MDSPAN_IMPL_IF_CONSTEXPR_17 (MDSPAN_IMPL_TRAIT(std::is_signed, SizeType)) {
            if (index < 0) {
                return true;
            }
        }
        return static_cast<index_type>(index) >= extent;
    }

    template <class, class, class, class>
    friend class mdspan;
};

#if defined(MDSPAN_IMPL_USE_CLASS_TEMPLATE_ARGUMENT_DEDUCTION)
MDSPAN_TEMPLATE_REQUIRES(
    class ElementType, class... SizeTypes,
    /* requires */ MDSPAN_IMPL_FOLD_AND(MDSPAN_IMPL_TRAIT(std::is_convertible, SizeTypes, size_t) /* && ... */) &&
        (sizeof...(SizeTypes) > 0)
)
MDSPAN_DEDUCTION_GUIDE explicit mdspan(ElementType *, SizeTypes...)
    -> mdspan<ElementType, ::MDSPAN_IMPL_STANDARD_NAMESPACE::dextents<size_t, sizeof...(SizeTypes)>>;

MDSPAN_TEMPLATE_REQUIRES(class Pointer, (MDSPAN_IMPL_TRAIT(std::is_pointer, std::remove_reference_t<Pointer>)))
MDSPAN_DEDUCTION_GUIDE mdspan(Pointer &&)
    -> mdspan<std::remove_pointer_t<std::remove_reference_t<Pointer>>, extents<size_t>>;

MDSPAN_TEMPLATE_REQUIRES(class CArray, (MDSPAN_IMPL_TRAIT(std::is_array, CArray) && (std::rank_v<CArray> == 1)))
MDSPAN_DEDUCTION_GUIDE mdspan(CArray &)
    -> mdspan<std::remove_all_extents_t<CArray>, extents<size_t, ::std::extent_v<CArray, 0>>>;

template <class ElementType, class SizeType, size_t N>
MDSPAN_DEDUCTION_GUIDE mdspan(ElementType *, const ::std::array<SizeType, N> &)
    -> mdspan<ElementType, ::MDSPAN_IMPL_STANDARD_NAMESPACE::dextents<size_t, N>>;

#ifdef __cpp_lib_span
template <class ElementType, class SizeType, size_t N>
MDSPAN_DEDUCTION_GUIDE mdspan(ElementType *, ::std::span<SizeType, N>)
    -> mdspan<ElementType, ::MDSPAN_IMPL_STANDARD_NAMESPACE::dextents<size_t, N>>;
#endif

// This one is necessary because all the constructors take `data_handle_type`s, not
// `ElementType*`s, and `data_handle_type` is taken from `accessor_type::data_handle_type`, which
// seems to throw off automatic deduction guides.
template <class ElementType, class SizeType, size_t... ExtentsPack>
MDSPAN_DEDUCTION_GUIDE mdspan(ElementType *, const extents<SizeType, ExtentsPack...> &)
    -> mdspan<ElementType, ::MDSPAN_IMPL_STANDARD_NAMESPACE::extents<SizeType, ExtentsPack...>>;

template <class ElementType, class MappingType>
MDSPAN_DEDUCTION_GUIDE mdspan(ElementType *, const MappingType &)
    -> mdspan<ElementType, typename MappingType::extents_type, typename MappingType::layout_type>;

template <class MappingType, class AccessorType>
MDSPAN_DEDUCTION_GUIDE mdspan(const typename AccessorType::data_handle_type, const MappingType &, const AccessorType &)
    -> mdspan<
        typename AccessorType::element_type, typename MappingType::extents_type, typename MappingType::layout_type,
        AccessorType>;
#endif

} // end namespace MDSPAN_IMPL_STANDARD_NAMESPACE
// END_FILE_INCLUDE: mdspan/include/experimental/__p0009_bits/mdspan.hpp
// BEGIN_FILE_INCLUDE: mdspan/include/experimental/__p0009_bits/layout_left.hpp
//@HEADER
//  ************************************************************************
//
//                         Kokkos v. 4.0
//        Copyright (2022) National Technology & Engineering
//                Solutions of Sandia, LLC (NTESS).
//
//  Under the terms of Contract DE-NA0003525 with NTESS,
//  the U.S. Government retains certain rights in this software.
//
//  Part of Kokkos, under the Apache License v2.0 with LLVM Exceptions.
//  See https://kokkos.org/LICENSE for license information.
//  SPDX-License-Identifier: Apache-2.0 WITH LLVM-exception
//
//@HEADER

#if MDSPAN_HAS_CXX_17
#endif
#include <type_traits>

namespace MDSPAN_IMPL_STANDARD_NAMESPACE {

//==============================================================================

template <class Extents>
class layout_left::mapping {
  public:
    using extents_type = Extents;
    using index_type = typename extents_type::index_type;
    using size_type = typename extents_type::size_type;
    using rank_type = typename extents_type::rank_type;
    using layout_type = layout_left;

  private:
    static_assert(
        detail::impl_is_extents_v<extents_type>, MDSPAN_IMPL_STANDARD_NAMESPACE_STRING
        "::layout_left::mapping must be instantiated with a specialization of " MDSPAN_IMPL_STANDARD_NAMESPACE_STRING
        "::extents."
    );

    template <class>
    friend class mapping;

    // i0+(i1 + E(1)*(i2 + E(2)*i3))
    template <size_t r, size_t Rank>
    struct rank_count {};

    template <size_t r, size_t Rank, class I, class... Indices>
    MDSPAN_IMPL_HOST_DEVICE constexpr index_type compute_offset(rank_count<r, Rank>, const I &i, Indices... idx) const {
        return compute_offset(rank_count<r + 1, Rank>(), idx...) * m_extents.extent(r) + i;
    }

    template <class I>
    MDSPAN_IMPL_HOST_DEVICE constexpr index_type
    compute_offset(rank_count<extents_type::rank() - 1, extents_type::rank()>, const I &i) const {
        return i;
    }

    MDSPAN_IMPL_HOST_DEVICE
    constexpr index_type compute_offset(rank_count<0, 0>) const { return 0; }

  public:
    //--------------------------------------------------------------------------------

    MDSPAN_INLINE_FUNCTION_DEFAULTED constexpr mapping() noexcept = default;
    MDSPAN_INLINE_FUNCTION_DEFAULTED constexpr mapping(mapping const &) noexcept = default;

    MDSPAN_IMPL_HOST_DEVICE
    constexpr mapping(extents_type const &exts) noexcept : m_extents(exts) {}

    MDSPAN_TEMPLATE_REQUIRES(
        class OtherExtents,
        /* requires */ (MDSPAN_IMPL_TRAIT(std::is_constructible, extents_type, OtherExtents))
    )
    MDSPAN_CONDITIONAL_EXPLICIT((!std::is_convertible<OtherExtents, extents_type>::value)) // needs two () due to comma
    MDSPAN_INLINE_FUNCTION MDSPAN_IMPL_CONSTEXPR_14
    mapping(mapping<OtherExtents> const &other) noexcept // NOLINT(google-explicit-constructor)
        : m_extents(other.extents()) {
        /*
         * TODO: check precondition
         * other.required_span_size() is a representable value of type index_type
         */
    }

    MDSPAN_TEMPLATE_REQUIRES(
        class OtherExtents,
        /* requires */ (
            MDSPAN_IMPL_TRAIT(std::is_constructible, extents_type, OtherExtents) && (extents_type::rank() <= 1)
        )
    )
    MDSPAN_CONDITIONAL_EXPLICIT((!std::is_convertible<OtherExtents, extents_type>::value)) // needs two () due to comma
    MDSPAN_INLINE_FUNCTION MDSPAN_IMPL_CONSTEXPR_14
    mapping(layout_right::mapping<OtherExtents> const &other) noexcept // NOLINT(google-explicit-constructor)
        : m_extents(other.extents()) {
        /*
         * TODO: check precondition
         * other.required_span_size() is a representable value of type index_type
         */
    }

#if MDSPAN_HAS_CXX_17
    /**
     * Converting constructor from `layout_left_padded::mapping`.
     *
     * This overload participates in overload resolution only if Mapping is a layout_left_padded mapping and
     * extents_type is constructible from Mapping::extents_type.
     *
     * \note There is currently a difference from p2642r2, where this function is specified as taking
     * `layout_left_padded< padding_value >::mapping< Extents>`. However, this makes `padding_value` non-deducible.
     */
    MDSPAN_TEMPLATE_REQUIRES(
        class Mapping,
        /* requires */ (MDSPAN_IMPL_PROPOSED_NAMESPACE::detail::is_layout_left_padded_mapping<Mapping>::value
                            &&std::is_constructible_v<extents_type, typename Mapping::extents_type>)
    )
    MDSPAN_CONDITIONAL_EXPLICIT((!std::is_convertible_v<typename Mapping::extents_type, extents_type>))
    MDSPAN_INLINE_FUNCTION constexpr mapping(const Mapping &other) noexcept : m_extents(other.extents()) {
        MDSPAN_IMPL_PROPOSED_NAMESPACE::detail::check_padded_layout_converting_constructor_mandates<
            extents_type, Mapping>(detail::with_rank<extents_type::rank()>{});
        MDSPAN_IMPL_PROPOSED_NAMESPACE::detail::check_padded_layout_converting_constructor_preconditions<extents_type>(
            detail::with_rank<extents_type::rank()>{}, other
        );
    }
#endif

    MDSPAN_TEMPLATE_REQUIRES(
        class OtherExtents,
        /* requires */ (MDSPAN_IMPL_TRAIT(std::is_constructible, extents_type, OtherExtents))
    )
    MDSPAN_CONDITIONAL_EXPLICIT((extents_type::rank() > 0))
    MDSPAN_INLINE_FUNCTION MDSPAN_IMPL_CONSTEXPR_14
    mapping(layout_stride::mapping<OtherExtents> const &other) noexcept // NOLINT(google-explicit-constructor)
        : m_extents(other.extents()) {
        /*
         * TODO: check precondition
         * other.required_span_size() is a representable value of type index_type
         */
        detail::validate_strides(detail::with_rank<extents_type::rank()>{}, layout_left{}, m_extents, other);
    }

    MDSPAN_INLINE_FUNCTION_DEFAULTED MDSPAN_IMPL_CONSTEXPR_14_DEFAULTED mapping &
    operator=(mapping const &) noexcept = default;

    MDSPAN_INLINE_FUNCTION
    constexpr const extents_type &extents() const noexcept { return m_extents; }

    MDSPAN_INLINE_FUNCTION
    constexpr index_type required_span_size() const noexcept {
        index_type value = 1;
        for (rank_type r = 0; r < extents_type::rank(); r++)
            value *= m_extents.extent(r);
        return value;
    }

    //--------------------------------------------------------------------------------

    MDSPAN_TEMPLATE_REQUIRES(
        class... Indices,
        /* requires */ (
            (sizeof...(Indices) == extents_type::rank()) && (detail::are_valid_indices<index_type, Indices...>())
        )
    )
    MDSPAN_IMPL_HOST_DEVICE
    constexpr index_type operator()(Indices... idxs) const noexcept {
#if !defined(NDEBUG)
        detail::check_all_indices(this->extents(), idxs...);
#endif // ! NDEBUG
        return compute_offset(rank_count<0, extents_type::rank()>(), static_cast<index_type>(idxs)...);
    }

    MDSPAN_INLINE_FUNCTION static constexpr bool is_always_unique() noexcept { return true; }
    MDSPAN_INLINE_FUNCTION static constexpr bool is_always_exhaustive() noexcept { return true; }
    MDSPAN_INLINE_FUNCTION static constexpr bool is_always_strided() noexcept { return true; }

    MDSPAN_INLINE_FUNCTION static constexpr bool is_unique() noexcept { return true; }
    MDSPAN_INLINE_FUNCTION static constexpr bool is_exhaustive() noexcept { return true; }
    MDSPAN_INLINE_FUNCTION static constexpr bool is_strided() noexcept { return true; }

    MDSPAN_INLINE_FUNCTION
    constexpr index_type stride(rank_type i) const noexcept
#if MDSPAN_HAS_CXX_20
        requires(Extents::rank() > 0)
#endif
    {
        index_type value = 1;
        for (rank_type r = 0; r < i; r++)
            value *= m_extents.extent(r);
        return value;
    }

    MDSPAN_TEMPLATE_REQUIRES(
        class OtherExtents,
        /* requires */ (Extents::rank() == OtherExtents::rank())
    )
    MDSPAN_INLINE_FUNCTION
    friend constexpr bool operator==(mapping const &lhs, mapping<OtherExtents> const &rhs) noexcept {
        return lhs.extents() == rhs.extents();
    }

    // In C++ 20 the not equal exists if equal is found
#if !(MDSPAN_HAS_CXX_20)
    MDSPAN_TEMPLATE_REQUIRES(
        class OtherExtents,
        /* requires */ (Extents::rank() == OtherExtents::rank())
    )
    MDSPAN_INLINE_FUNCTION
    friend constexpr bool operator!=(mapping const &lhs, mapping<OtherExtents> const &rhs) noexcept {
        return lhs.extents() != rhs.extents();
    }
#endif

    // Not really public, but currently needed to implement fully constexpr useable submdspan:
    template <size_t N, class SizeType, size_t... E, size_t... Idx>
    MDSPAN_INLINE_FUNCTION constexpr index_type impl_get_stride(
        MDSPAN_IMPL_STANDARD_NAMESPACE::extents<SizeType, E...>, std::integer_sequence<size_t, Idx...>
    ) const {
        return MDSPAN_IMPL_FOLD_TIMES_RIGHT((Idx < N ? m_extents.template extent<Idx>() : 1), 1);
    }
    template <size_t N>
    MDSPAN_INLINE_FUNCTION constexpr index_type impl_stide() const noexcept {
        return impl_get_stride<N>(m_extents, std::make_index_sequence<extents_type::rank()>());
    }

  private:
    MDSPAN_IMPL_NO_UNIQUE_ADDRESS extents_type m_extents{};

    // [mdspan.submdspan.mapping], submdspan mapping specialization
    template <class... SliceSpecifiers>
    MDSPAN_INLINE_FUNCTION constexpr auto submdspan_mapping_impl(SliceSpecifiers... slices) const;

    template <class... SliceSpecifiers>
    MDSPAN_INLINE_FUNCTION friend constexpr auto submdspan_mapping(const mapping &src, SliceSpecifiers... slices) {
        return src.submdspan_mapping_impl(slices...);
    }
};

} // end namespace MDSPAN_IMPL_STANDARD_NAMESPACE
// END_FILE_INCLUDE: mdspan/include/experimental/__p0009_bits/layout_left.hpp
#if MDSPAN_HAS_CXX_17
// BEGIN_FILE_INCLUDE: mdspan/include/experimental/__p2642_bits/layout_padded.hpp
//@HEADER
//  ************************************************************************
//
//                         Kokkos v. 4.0
//        Copyright (2022) National Technology & Engineering
//                Solutions of Sandia, LLC (NTESS).
//
//  Under the terms of Contract DE-NA0003525 with NTESS,
//  the U.S. Government retains certain rights in this software.
//
//  Part of Kokkos, under the Apache License v2.0 with LLVM Exceptions.
//
//  SPDX-License-Identifier: Apache-2.0 WITH LLVM-exception
//
//@HEADER

#include <cassert>

namespace MDSPAN_IMPL_STANDARD_NAMESPACE {
namespace MDSPAN_IMPL_PROPOSED_NAMESPACE {
    namespace detail {
        template <class T, class U>
        MDSPAN_INLINE_FUNCTION constexpr T find_next_multiple(T alignment, U offset) {
            if (alignment == T(0)) {
                return T(0);
            } else {
                return ((offset + alignment - 1) / alignment) * alignment;
            }
        }

        template <class ExtentsType, size_t PaddingValue, size_t ExtentToPadIdx>
        MDSPAN_INLINE_FUNCTION constexpr size_t get_actual_static_padding_value() {
            using MDSPAN_IMPL_STANDARD_NAMESPACE::detail::in_range;
            constexpr auto rank = ExtentsType::rank();

            if constexpr (rank <= typename ExtentsType::rank_type(1)) {
                return 0;
            } else if constexpr (PaddingValue != dynamic_extent &&
                                 ExtentsType::static_extent(ExtentToPadIdx) != dynamic_extent) {
                static_assert(
                    (PaddingValue != 0) || (ExtentsType::static_extent(ExtentToPadIdx) == 0),
                    "padding stride can be 0 only if "
                    "extents_type::static_extent(extent-to-pad) is 0 or dynamic_extent"
                );
                constexpr auto ret = find_next_multiple(PaddingValue, ExtentsType::static_extent(ExtentToPadIdx));

                using index_type = typename ExtentsType::index_type;
                static_assert(
                    in_range<index_type>(ret), "The least multiple of padding_value and first-static-extent "
                                               "must be representable by index_type"
                );

                return ret;
            } else {
                return dynamic_extent;
            }
            // Missing return statement warning from NVCC and ICC
#if (defined(__NVCC__) || defined(__INTEL_COMPILER)) && !defined(__NVCOMPILER)
            return 0;
#endif
        }

        template <size_t PaddingValue, typename Extents, size_t ExtentToPadIdx, size_t Rank, typename Enabled = void>
        struct static_array_type_for_padded_extent {
            static constexpr size_t padding_value = PaddingValue;
            using index_type = typename Extents::index_type;
            using extents_type = Extents;
            using type = ::MDSPAN_IMPL_STANDARD_NAMESPACE::detail::maybe_static_array<
                index_type, size_t, dynamic_extent,
                ::MDSPAN_IMPL_STANDARD_NAMESPACE::MDSPAN_IMPL_PROPOSED_NAMESPACE::detail::
                    get_actual_static_padding_value<extents_type, PaddingValue, ExtentToPadIdx>()>;
        };

        template <size_t PaddingValue, typename Extents, size_t ExtentToPadIdx, size_t Rank>
        struct static_array_type_for_padded_extent<
            PaddingValue, Extents, ExtentToPadIdx, Rank, std::enable_if_t<Rank <= 1>> {
            using index_type = typename Extents::index_type;
            using extents_type = Extents;
            using type =
                ::MDSPAN_IMPL_STANDARD_NAMESPACE::detail::maybe_static_array<index_type, size_t, dynamic_extent, 0>;
        };

        template <size_t PaddingValue, typename Extents, size_t ExtentToPadIdx>
        struct padded_extent {
            static constexpr size_t padding_value = PaddingValue;
            using index_type = typename Extents::index_type;
            using extents_type = Extents;
            using static_array_type = typename static_array_type_for_padded_extent<
                padding_value, Extents, ExtentToPadIdx, Extents::rank()>::type;

            MDSPAN_INLINE_FUNCTION
            static constexpr auto static_value() { return static_array_type::static_value(0); }

            MDSPAN_INLINE_FUNCTION
            static constexpr static_array_type init_padding(const Extents &exts) {
                if constexpr ((Extents::rank() > 1) && (padding_value == dynamic_extent)) {
                    return {exts.extent(ExtentToPadIdx)};
                } else {
                    return init_padding(exts, padding_value);
                }
                // Missing return statement warning from NVCC and ICC
#if (defined(__NVCC__) || defined(__INTEL_COMPILER)) && !defined(__NVCOMPILER)
                return {};
#endif
            }

            MDSPAN_INLINE_FUNCTION static constexpr static_array_type
            init_padding([[maybe_unused]] const Extents &exts, [[maybe_unused]] size_t pv) {
                using MDSPAN_IMPL_STANDARD_NAMESPACE::detail::in_range;
                if constexpr (Extents::rank() > 1) {
                    auto strd = find_next_multiple(pv, exts.extent(ExtentToPadIdx));
                    MDSPAN_IMPL_PRECONDITION(in_range<index_type>(strd));
                    return {strd};
                } else {
                    return {};
                }
                // Missing return statement warning from NVCC and ICC
#if (defined(__NVCC__) || defined(__INTEL_COMPILER)) && !defined(__NVCOMPILER)
                return {};
#endif
            }

            template <typename Mapping, size_t PaddingStrideIdx>
            MDSPAN_INLINE_FUNCTION static constexpr static_array_type init_padding(
                [[maybe_unused]] const Mapping &other_mapping, std::integral_constant<size_t, PaddingStrideIdx>
            ) {
                if constexpr (Extents::rank() > 1) {
                    return {other_mapping.stride(PaddingStrideIdx)};
                } else {
                    return {};
                }
                // Missing return statement warning from NVCC and ICC
#if (defined(__NVCC__) || defined(__INTEL_COMPILER)) && !defined(__NVCOMPILER)
                return {};
#endif
            }
        };

        template <typename Extents>
        MDSPAN_INLINE_FUNCTION constexpr bool check_static_extents_representability() {
            using MDSPAN_IMPL_STANDARD_NAMESPACE::detail::check_mul_result_is_nonnegative_and_representable;
            // We cannot check statically for sure if the extents are representable
            // if we have dynamic values -- this can only be checked by a precondition
            // We can check if the product of only the static extents is representable though...
            using index_type = typename Extents::index_type;

            // get rid of NVCC warning "pointless comparison of unsigned integer with zero"
            if constexpr (Extents::rank() > 0) {
                auto prod = index_type(1);
                for (size_t i = 0; i < Extents::rank(); ++i) {
                    if (Extents::static_extent(i) == dynamic_extent)
                        continue;
                    if (!check_mul_result_is_nonnegative_and_representable(
                            prod, static_cast<index_type>(Extents::static_extent(i))
                        ))
                        return false;
                    prod *= Extents::static_extent(i);
                }
            }

            return true;
        }

        template <typename Extents>
        MDSPAN_INLINE_FUNCTION constexpr bool check_extents_representability(const Extents &exts) {
            using MDSPAN_IMPL_STANDARD_NAMESPACE::detail::check_mul_result_is_nonnegative_and_representable;
            using index_type = typename Extents::index_type;

            // get rid of NVCC warning "pointless comparison of unsigned integer with zero"
            if constexpr (Extents::rank() > 0) {
                auto prod = index_type(1);
                for (size_t i = 0; i < Extents::rank(); ++i) {
                    if (!check_mul_result_is_nonnegative_and_representable(
                            prod, static_cast<index_type>(exts.extent(i))
                        ))
                        return false;
                    prod *= exts.extent(i);
                }
            }

            return true;
        }

        template <typename CheckType, size_t StaticPaddingValue, typename Extents>
        MDSPAN_INLINE_FUNCTION constexpr bool check_static_extents_and_left_padding_representability() {
            using MDSPAN_IMPL_STANDARD_NAMESPACE::detail::check_mul_result_is_nonnegative_and_representable;
            if constexpr (Extents::rank() < 2) {
                return true;
            }

            // We cannot check statically for sure if the product of the extents and padding value
            // are representable if we have dynamic values -- this can only be checked by a precondition
            // We can check if the product of only the static extents and potentially the padding value (if it is
            // static) is representable though... We already checked that StaticPaddingValue is representable by
            // index_type

            // get rid of NVCC warning "pointless comparison of unsigned integer with zero"
            if constexpr (Extents::rank() > 0) {
                auto prod =
                    (StaticPaddingValue != dynamic_extent) ? static_cast<CheckType>(StaticPaddingValue) : CheckType(1);
                for (size_t i = 1; i < Extents::rank(); ++i) {
                    if (Extents::static_extent(i) == dynamic_extent)
                        continue;
                    if (!check_mul_result_is_nonnegative_and_representable(
                            prod, static_cast<CheckType>(Extents::static_extent(i))
                        ))
                        return false;
                    prod *= Extents::static_extent(i);
                }
            }

            return true;
        }

        template <typename CheckType, typename Extents>
        MDSPAN_INLINE_FUNCTION constexpr bool check_extents_and_left_padding_representability(
            const Extents &exts, [[maybe_unused]] size_t dynamic_padding_value
        ) {
            using MDSPAN_IMPL_STANDARD_NAMESPACE::detail::check_mul_result_is_nonnegative_and_representable;

            // get rid of NVCC warning "pointless comparison of unsigned integer with zero"
            // And also a rank 1 layout cannot overflow
            if constexpr (Extents::rank() > 1) {
                auto prod = static_cast<CheckType>(dynamic_padding_value);
                for (size_t i = 1; i < Extents::rank(); ++i) {
                    if (!check_mul_result_is_nonnegative_and_representable(
                            prod, static_cast<CheckType>(exts.extent(i))
                        ))
                        return false;
                    prod *= exts.extent(i);
                }
            }

            return true;
        }

        template <typename CheckType, size_t StaticPaddingValue, typename Extents>
        MDSPAN_INLINE_FUNCTION constexpr bool check_static_extents_and_right_padding_representability() {
            using MDSPAN_IMPL_STANDARD_NAMESPACE::detail::check_mul_result_is_nonnegative_and_representable;

            // We cannot check statically for sure if the product of the extents and padding value
            // are representable if we have dynamic values -- this can only be checked by a precondition
            // We can check if the product of only the static extents and potentially the padding value (if it is
            // static) is representable though... We already checked that StaticPaddingValue is representable by
            // index_type

            // get rid of NVCC warning "pointless comparison of unsigned integer with zero"
            // And also a rank 1 layout cannot overflow
            if constexpr (Extents::rank() > 1) {
                auto prod =
                    (StaticPaddingValue != dynamic_extent) ? static_cast<CheckType>(StaticPaddingValue) : CheckType(1);
                for (size_t i = 0; i < Extents::rank() - 1; ++i) {
                    if (Extents::static_extent(i) == dynamic_extent)
                        continue;
                    if (!check_mul_result_is_nonnegative_and_representable(
                            prod, static_cast<CheckType>(Extents::static_extent(i))
                        ))
                        return false;
                    prod *= Extents::static_extent(i);
                }
            }

            return true;
        }

        template <typename CheckType, typename Extents>
        MDSPAN_INLINE_FUNCTION constexpr bool check_extents_and_right_padding_representability(
            const Extents &exts, [[maybe_unused]] size_t dynamic_padding_value
        ) {
            using MDSPAN_IMPL_STANDARD_NAMESPACE::detail::check_mul_result_is_nonnegative_and_representable;

            // get rid of NVCC warning "pointless comparison of unsigned integer with zero"
            // And also a rank 1 layout cannot overflow
            if constexpr (Extents::rank() > 1) {
                auto prod = static_cast<CheckType>(dynamic_padding_value);
                for (size_t i = 0; i < Extents::rank() - 1; ++i) {
                    if (!check_mul_result_is_nonnegative_and_representable(
                            prod, static_cast<CheckType>(exts.extent(i))
                        ))
                        return false;
                    prod *= exts.extent(i);
                }
            }

            return true;
        }
    } // namespace detail

    template <size_t PaddingValue>
    template <class Extents>
    class layout_left_padded<PaddingValue>::mapping {
      public:
        static constexpr size_t padding_value = PaddingValue;

        using extents_type = Extents;
        using index_type = typename extents_type::index_type;
        using size_type = typename extents_type::size_type;
        using rank_type = typename extents_type::rank_type;
        using layout_type = layout_left_padded<padding_value>;

#ifndef MDSPAN_INTERNAL_TEST
      private:
#endif // MDSPAN_INTERNAL_TEST

        static constexpr rank_type padded_stride_idx =
            detail::layout_padded_constants<layout_type, extents_type>::padded_stride_idx;
        static constexpr rank_type extent_to_pad_idx =
            detail::layout_padded_constants<layout_type, extents_type>::extent_to_pad_idx;

        static_assert(
            (padding_value != 0) || (extents_type::static_extent(extent_to_pad_idx) == 0) ||
                (extents_type::static_extent(extent_to_pad_idx) == dynamic_extent),
            "out of bounds access for rank 0"
        );
        static_assert(
            detail::check_static_extents_representability<extents_type>(),
            "The size of the muiltidimensional index space given by the extents must be representable as a value of "
            "index_type"
        );
        static_assert(
            (padding_value == dynamic_extent) ||
                MDSPAN_IMPL_STANDARD_NAMESPACE::detail::in_range<index_type>(padding_value),
            "padding_value must be representable as a value of type index_type"
        );

        using padded_stride_type = detail::padded_extent<padding_value, extents_type, extent_to_pad_idx>;

        static constexpr size_t static_padding_stride = padded_stride_type::static_value();

        static_assert(
            detail::check_static_extents_and_left_padding_representability<
                index_type, static_padding_stride, extents_type>() &&
                detail::check_static_extents_and_left_padding_representability<
                    size_t, static_padding_stride, extents_type>(),
            "the product of static_padding_stride and static extents 1 through rank must be representable as a value "
            "of type size_t and index_type"
        );

        typename padded_stride_type::static_array_type padded_stride = {};
        extents_type exts = {};

        MDSPAN_INLINE_FUNCTION constexpr index_type compute_offset(std::index_sequence<>) const { return 0; }

        template <size_t Rank, class IndexOffset>
        MDSPAN_INLINE_FUNCTION constexpr index_type
        compute_offset(std::index_sequence<Rank>, IndexOffset index_offset) const {
            return index_offset;
        }

        template <size_t... Ranks, class... IndexOffsets>
        MDSPAN_INLINE_FUNCTION constexpr index_type
        compute_offset(std::index_sequence<Ranks...>, IndexOffsets... index_offsets) const {
            index_type indices[] = {static_cast<index_type>(index_offsets)...};
            // self-recursive fold trick from
            // https://github.com/llvm/llvm-project/blob/96e1914aa2e6d8966acbfbe2f4d184201f1aa318/libcxx/include/mdspan/layout_left.h#L144
            index_type res = 0;
            ((res = indices[extents_type::rank() - 1 - Ranks] + ((extents_type::rank() - 1 - Ranks) == extent_to_pad_idx
                                                                     ? padded_stride.value(0)
                                                                     : exts.extent(extents_type::rank() - 1 - Ranks)) *
                                                                    res),
             ...);
            return res;
        }

      public:
#if !MDSPAN_HAS_CXX_20 || defined(__NVCC__)
        MDSPAN_INLINE_FUNCTION
        constexpr mapping() : mapping(extents_type{}) {}
#else
        MDSPAN_INLINE_FUNCTION_DEFAULTED
        constexpr mapping() requires(static_padding_stride != dynamic_extent) = default;

        MDSPAN_INLINE_FUNCTION
        constexpr mapping() requires(static_padding_stride == dynamic_extent) : mapping(extents_type{}) {}
#endif

        MDSPAN_INLINE_FUNCTION_DEFAULTED constexpr mapping(const mapping &) noexcept = default;
        MDSPAN_INLINE_FUNCTION_DEFAULTED constexpr mapping &operator=(const mapping &) noexcept = default;

        /**
         * Initializes the mapping with the given extents.
         *
         * \param ext the given extents
         */
        MDSPAN_INLINE_FUNCTION
        constexpr mapping(const extents_type &ext) : padded_stride(padded_stride_type::init_padding(ext)), exts(ext) {
            MDSPAN_IMPL_PRECONDITION(detail::check_extents_representability(ext));
            MDSPAN_IMPL_PRECONDITION(
                detail::check_extents_and_left_padding_representability<index_type>(ext, padded_stride.value(0))
            );
        }

        /**
         * Initializes the mapping with the given extents and the specified padding value.
         *
         * This overload participates in overload resolution only if `is_convertible_v<Size, index_type>`
         * is `true` and `is_nothrow_constructible_v<index_type, Size>` is `true`
         *
         * \param ext the given extents
         * \param padding_value the padding value
         */
        MDSPAN_TEMPLATE_REQUIRES(
            class Size,
            /* requires */ (std::is_convertible_v<Size, index_type> &&std::is_nothrow_constructible_v<index_type, Size>)
        )
        MDSPAN_INLINE_FUNCTION
        constexpr mapping(const extents_type &ext, Size dynamic_padding_value)
            : padded_stride(padded_stride_type::init_padding(ext, dynamic_padding_value)), exts(ext) {
            assert(
                (padding_value == dynamic_extent) ||
                (static_cast<index_type>(padding_value) == static_cast<index_type>(dynamic_padding_value))
            );
            MDSPAN_IMPL_PRECONDITION(detail::check_extents_representability(ext));
            MDSPAN_IMPL_PRECONDITION(
                detail::check_extents_and_left_padding_representability<index_type>(ext, dynamic_padding_value)
            );
        }

        /**
         * Converting constructor from `layout_left::mapping`.
         *
         * This overload participates in overload resolution only if
         * `is_constructible_v<extents_type, OtherExtents>` is true. If
         * `OtherExtents::rank() > 1` then one of `padding_value`, `static_extent(0)`,
         * or `OtherExtents::static_extent(0)` must be `dynamic_extent`; otherwise,
         * `OtherExtents::static_extent(0)` must be equal to the least multiple of
         * `padding_value` greater than or equal to `extents_type::static_extent(0)`
         */
        MDSPAN_TEMPLATE_REQUIRES(
            class OtherExtents,
            /* requires */ (std::is_constructible_v<extents_type, OtherExtents>)
        )
        MDSPAN_CONDITIONAL_EXPLICIT((!std::is_convertible_v<OtherExtents, extents_type>))
        MDSPAN_INLINE_FUNCTION
        constexpr mapping(const layout_left::mapping<OtherExtents> &other_mapping)
            : padded_stride(
                  padded_stride_type::init_padding(other_mapping, std::integral_constant<size_t, padded_stride_idx>{})
              ),
              exts(other_mapping.extents()) {
            static_assert(
                (OtherExtents::rank() > 1) || (static_padding_stride != dynamic_extent) ||
                (OtherExtents::static_extent(extent_to_pad_idx) != dynamic_extent) ||
                (static_padding_stride == OtherExtents::static_extent(extent_to_pad_idx))
            );
            MDSPAN_IMPL_PRECONDITION(detail::check_extents_representability(exts));
            MDSPAN_IMPL_PRECONDITION(
                detail::check_extents_and_left_padding_representability<index_type>(exts, padded_stride.value(0))
            );
        }

        /**
         * Converting constructor from `layout_stride::mapping`.
         *
         * This overload participates in overload resolution only if
         * `is_constructible_v<extents_type, OtherExtents>` is true
         */
        MDSPAN_TEMPLATE_REQUIRES(
            class OtherExtents,
            /* requires */ (std::is_constructible_v<extents_type, OtherExtents>)
        )
        MDSPAN_CONDITIONAL_EXPLICIT((extents_type::rank() > 0))
        MDSPAN_INLINE_FUNCTION
        constexpr mapping(const layout_stride::mapping<OtherExtents> &other_mapping)
            : padded_stride(
                  padded_stride_type::init_padding(other_mapping, std::integral_constant<size_t, padded_stride_idx>{})
              ),
              exts(other_mapping.extents()) {
            MDSPAN_IMPL_PRECONDITION(detail::check_extents_representability(exts));
            MDSPAN_IMPL_PRECONDITION(
                detail::check_extents_and_left_padding_representability<index_type>(exts, padded_stride.value(0))
            );
        }

        /**
         * Converting constructor from `layout_left_padded::mapping`.
         *
         * This overload participates in overload resolution only if
         * `is_constructible_v<extents_type, OtherExtents>` is true. Either
         * `padding_value` or `OtherPaddingStride` must be `std::dynamic_extent`, or
         * `padding_value == OtherPaddingStride`.
         */
        MDSPAN_TEMPLATE_REQUIRES(
            class Mapping,
            /* requires */ (detail::is_layout_left_padded_mapping<Mapping>::value
                                &&std::is_constructible_v<extents_type, typename Mapping::extents_type>)
        )
        MDSPAN_CONDITIONAL_EXPLICIT(
            (extents_type::rank() > 1 && (padding_value == dynamic_extent || Mapping::padding_value == dynamic_extent))
        )
        MDSPAN_INLINE_FUNCTION
        constexpr mapping(const Mapping &other_mapping)
            : padded_stride(
                  padded_stride_type::init_padding(other_mapping, std::integral_constant<size_t, padded_stride_idx>{})
              ),
              exts(other_mapping.extents()) {
            static_assert(
                padding_value == dynamic_extent || Mapping::padding_value == dynamic_extent ||
                padding_value == Mapping::padding_value
            );
            MDSPAN_IMPL_PRECONDITION(detail::check_extents_representability(exts));
            MDSPAN_IMPL_PRECONDITION(
                detail::check_extents_and_left_padding_representability<index_type>(exts, padded_stride.value(0))
            );
        }

        /**
         * Converting constructor from `layout_right_padded::mapping`.
         *
         * This overload participates in overload resolution only if
         * `extents_type::rank()` is 0 or 1 and `is_constructible_v<extents_type,
         * OtherExtents>` is `true`.
         */
        MDSPAN_TEMPLATE_REQUIRES(
            class Mapping,
            /* requires */ (
                detail::is_layout_right_padded_mapping<Mapping>::value &&extents_type::rank() <= 1 &&
                std::is_constructible_v<extents_type, typename Mapping::extents_type>
            )
        )
        MDSPAN_CONDITIONAL_EXPLICIT((!std::is_convertible_v<typename Mapping::extents_type, extents_type>))
        MDSPAN_INLINE_FUNCTION
        constexpr mapping(const Mapping &other_mapping) noexcept
            : padded_stride(
                  padded_stride_type::init_padding(
                      static_cast<extents_type>(other_mapping.extents()),
                      other_mapping.extents().extent(extent_to_pad_idx)
                  )
              ),
              exts(other_mapping.extents()) {
            MDSPAN_IMPL_PRECONDITION(detail::check_extents_representability(exts));
            MDSPAN_IMPL_PRECONDITION(
                detail::check_extents_and_left_padding_representability<index_type>(exts, padded_stride.value(0))
            );
        }

        MDSPAN_INLINE_FUNCTION constexpr const extents_type &extents() const noexcept { return exts; }

        constexpr std::array<index_type, extents_type::rank()> strides() const noexcept {
            if constexpr (extents_type::rank() == 0) {
                return {};
            } else if constexpr (extents_type::rank() == 1) {
                return {1};
            } else {
                index_type value = 1;
                std::array<index_type, extents_type::rank()> s{};
                s[extent_to_pad_idx] = value;
                value *= padded_stride.value(0);
                for (rank_type r = extent_to_pad_idx + 1; r < extents_type::rank() - 1; ++r) {
                    s[r] = value;
                    value *= exts.extent(r);
                }
                s[extents_type::rank() - 1] = value;
                return s;
            }
        }

        MDSPAN_INLINE_FUNCTION constexpr index_type required_span_size() const noexcept {
            if constexpr (extents_type::rank() == 0) {
                return 1;
            } else if constexpr (extents_type::rank() == 1) {
                return exts.extent(0);
            } else {
                index_type value = padded_stride.value(0);
                for (rank_type r = 1; r < extents_type::rank(); ++r) {
                    value *= exts.extent(r);
                }
                return value == 0 ? 0 : value + exts.extent(0) - padded_stride.value(0);
            }
        }

        /**
         * Return the mapping given the provided indices per rank.
         *
         * This overload participates in overload resolution only if:
         * - `sizeof...(Indices) == extents_type::rank()`,
         * - `(is_convertible_v<Indices, index_type> && ...) is true`, and
         * - (is_nothrow_constructible_v<index_type, Indices> && ...) is true.
         */
        MDSPAN_TEMPLATE_REQUIRES(
            class... Indices,
            /* requires */ (
                sizeof...(Indices) == extents_type::rank() &&
                (::MDSPAN_IMPL_STANDARD_NAMESPACE::detail::are_valid_indices<index_type, Indices...>())
            )
        )
        MDSPAN_INLINE_FUNCTION constexpr size_t operator()(Indices... idxs) const noexcept {
#if !defined(NDEBUG)
            ::MDSPAN_IMPL_STANDARD_NAMESPACE::detail::check_all_indices(this->extents(), idxs...);
#endif // ! NDEBUG
            return compute_offset(std::index_sequence_for<Indices...>{}, idxs...);
        }

        MDSPAN_INLINE_FUNCTION static constexpr bool is_always_unique() noexcept { return true; }
        MDSPAN_INLINE_FUNCTION static constexpr bool is_always_exhaustive() noexcept {
            return (extents_type::rank() <= rank_type(1)) ||
                   (extents_type::static_extent(extent_to_pad_idx) != dynamic_extent &&
                    extents_type::static_extent(extent_to_pad_idx) == padded_stride_type::static_value());
        }
        MDSPAN_INLINE_FUNCTION static constexpr bool is_always_strided() noexcept { return true; }

        MDSPAN_INLINE_FUNCTION static constexpr bool is_unique() noexcept { return true; }
        MDSPAN_INLINE_FUNCTION constexpr bool is_exhaustive() const noexcept {
            return (extents_type::rank() < 2) || (exts.extent(extent_to_pad_idx) == padded_stride.value(0));
        }
        MDSPAN_INLINE_FUNCTION static constexpr bool is_strided() noexcept { return true; }

        MDSPAN_INLINE_FUNCTION
        constexpr index_type stride(rank_type r) const noexcept {
            assert(r < extents_type::rank());
            if (r == 0)
                return index_type(1);

            index_type value = padded_stride.value(0);
            for (rank_type k = 1; k < r; k++)
                value *= exts.extent(k);

            return value;
        }

        /**
         * Equality operator between `layout_left_padded`s
         *
         * This overload only participates in overload resolution if
         * `OtherExtents::rank() == extents_type::rank()`.
         *
         * \note There is currently a difference from p2642r2, where this function is
         * specified as taking `layout_left_padded< padding_value >::mapping<
         * Extents>`. However, this makes `padding_value` non-deducible.
         */
        MDSPAN_TEMPLATE_REQUIRES(
            class Mapping,
            /* requires */ (
                detail::is_layout_left_padded_mapping<Mapping>::value &&
                (Mapping::extents_type::rank() == extents_type::rank())
            )
        )
        MDSPAN_INLINE_FUNCTION friend constexpr bool operator==(const mapping &left, const Mapping &right) noexcept {
            // Workaround for some compilers not short-circuiting properly with
            // compile-time checks i.e. we can't access stride(_padding_stride_idx) of a
            // rank 0 mapping
            bool strides_equal = true;
            if constexpr (extents_type::rank() > rank_type(1)) {
                strides_equal = left.stride(padded_stride_idx) == right.stride(padded_stride_idx);
            }
            return (left.extents() == right.extents()) && strides_equal;
        }

#if !MDSPAN_HAS_CXX_20
        /**
         * Inequality operator between `layout_left_padded`s
         *
         * This overload only participates in overload resolution if
         * `OtherExtents::rank() == extents_type::rank()`.
         */
        MDSPAN_TEMPLATE_REQUIRES(
            class Mapping,
            /* requires */ (
                detail::is_layout_left_padded_mapping<Mapping>::value &&
                (Mapping::extents_type::rank() == extents_type::rank())
            )
        )
        MDSPAN_INLINE_FUNCTION friend constexpr bool operator!=(const mapping &left, const Mapping &right) noexcept {
            return !(left == right);
        }
#endif

        // [mdspan.submdspan.mapping], submdspan mapping specialization
        template <class... SliceSpecifiers>
        MDSPAN_INLINE_FUNCTION constexpr auto submdspan_mapping_impl(SliceSpecifiers... slices) const;

        template <class... SliceSpecifiers>
        MDSPAN_INLINE_FUNCTION friend constexpr auto submdspan_mapping(const mapping &src, SliceSpecifiers... slices) {
            return src.submdspan_mapping_impl(slices...);
        }
    };

    template <size_t PaddingValue>
    template <class Extents>
    class layout_right_padded<PaddingValue>::mapping {
      public:
        static constexpr size_t padding_value = PaddingValue;

        using extents_type = Extents;
        using index_type = typename extents_type::index_type;
        using size_type = typename extents_type::size_type;
        using rank_type = typename extents_type::rank_type;
        using layout_type = layout_right_padded<padding_value>;

#ifndef MDSPAN_INTERNAL_TEST
      private:
#endif // MDSPAN_INTERNAL_TEST

        static constexpr rank_type padded_stride_idx =
            detail::layout_padded_constants<layout_type, extents_type>::padded_stride_idx;
        static constexpr rank_type extent_to_pad_idx =
            detail::layout_padded_constants<layout_type, extents_type>::extent_to_pad_idx;

        static_assert(
            (padding_value != 0) || (extents_type::static_extent(extent_to_pad_idx) == 0) ||
                (extents_type::static_extent(extent_to_pad_idx) == dynamic_extent),
            "if padding stride is 0, static_extent(extent-to-pad-rank) must also be 0 or dynamic_extent"
        );
        static_assert(
            detail::check_static_extents_representability<extents_type>(),
            "The size of the muiltidimensional index space given by the extents must be representable as a value of "
            "index_type"
        );
        static_assert(
            (padding_value == dynamic_extent) ||
                MDSPAN_IMPL_STANDARD_NAMESPACE::detail::in_range<index_type>(padding_value),
            "padding_value must be representable as a value of type index_type"
        );

        using padded_stride_type = detail::padded_extent<padding_value, extents_type, extent_to_pad_idx>;
        static constexpr size_t static_padding_stride = padded_stride_type::static_value();

        static_assert(
            detail::check_static_extents_and_right_padding_representability<
                index_type, static_padding_stride, extents_type>() &&
                detail::check_static_extents_and_right_padding_representability<
                    size_t, static_padding_stride, extents_type>(),
            "the product of static_padding_stride and static extents 1 through rank must be representable as a value "
            "of type size_t and index_type"
        );

        typename padded_stride_type::static_array_type padded_stride = {};
        extents_type exts = {};

        MDSPAN_INLINE_FUNCTION constexpr index_type compute_offset(std::index_sequence<>) const { return 0; }

        template <size_t Rank, class IndexOffset>
        MDSPAN_INLINE_FUNCTION constexpr index_type
        compute_offset(std::index_sequence<Rank>, IndexOffset index_offset) const {
            return index_offset;
        }

        template <size_t... Ranks, class... IndexOffsets>
        MDSPAN_INLINE_FUNCTION constexpr index_type
        compute_offset(std::index_sequence<Ranks...>, IndexOffsets... index_offsets) const {
            // self-recursive fold trick from
            // https://github.com/llvm/llvm-project/blob/4d9771741d40cc9cfcccb6b033f43689d36b705a/libcxx/include/mdspan/layout_right.h#L141
            index_type res = 0;
            ((res = static_cast<index_type>(index_offsets) +
                    (Ranks == extent_to_pad_idx ? padded_stride.value(0) : exts.extent(Ranks)) * res),
             ...);
            return res;
        }

      public:
#if !MDSPAN_HAS_CXX_20 || defined(__NVCC__)
        MDSPAN_INLINE_FUNCTION
        constexpr mapping() : mapping(extents_type{}) {}
#else
        MDSPAN_INLINE_FUNCTION_DEFAULTED
        constexpr mapping() requires(static_padding_stride != dynamic_extent) = default;

        MDSPAN_INLINE_FUNCTION
        constexpr mapping() requires(static_padding_stride == dynamic_extent) : mapping(extents_type{}) {}
#endif

        MDSPAN_INLINE_FUNCTION_DEFAULTED constexpr mapping(const mapping &) noexcept = default;
        MDSPAN_INLINE_FUNCTION_DEFAULTED constexpr mapping &operator=(const mapping &) noexcept = default;

        /**
         * Initializes the mapping with the given extents.
         *
         * \param ext the given extents
         */
        MDSPAN_INLINE_FUNCTION
        constexpr mapping(const extents_type &ext) : padded_stride(padded_stride_type::init_padding(ext)), exts(ext) {
            MDSPAN_IMPL_PRECONDITION(detail::check_extents_representability(ext));
            MDSPAN_IMPL_PRECONDITION(
                detail::check_extents_and_right_padding_representability<index_type>(ext, padded_stride.value(0))
            );
        }

        /**
         * Initializes the mapping with the given extents and the specified padding value.
         *
         * This overload participates in overload resolution only if `is_convertible_v<Size, index_type>`
         * is `true` and `is_nothrow_constructible_v<index_type, Size>` is `true`
         *
         * \param ext the given extents
         * \param padding_value the padding value
         */
        MDSPAN_TEMPLATE_REQUIRES(
            class Size,
            /* requires */ (std::is_convertible_v<Size, index_type> &&std::is_nothrow_constructible_v<index_type, Size>)
        )
        MDSPAN_INLINE_FUNCTION
        constexpr mapping(const extents_type &ext, Size dynamic_padding_value)
            : padded_stride(padded_stride_type::init_padding(ext, static_cast<index_type>(dynamic_padding_value))),
              exts(ext) {
            assert(
                (padding_value == dynamic_extent) ||
                (static_cast<index_type>(padding_value) == static_cast<index_type>(dynamic_padding_value))
            );
            MDSPAN_IMPL_PRECONDITION(detail::check_extents_representability(ext));
            MDSPAN_IMPL_PRECONDITION(
                detail::check_extents_and_right_padding_representability<index_type>(ext, dynamic_padding_value)
            );
        }

        /**
         * Converting constructor from `layout_right::mapping`.
         *
         * This overload participates in overload resolution only if `is_constructible_v<extents_type, OtherExtents>` is
         * true. If `OtherExtents::rank() > 1` then one of `padding_value`, `static_extent(0)`, or
         * `OtherExtents::static_extent(0)` must be `dynamic_extent`; otherwise, `OtherExtents::static_extent(0)` must
         * be equal to the least multiple of `padding_value` greater than or equal to `extents_type::static_extent(0)`
         */
        MDSPAN_TEMPLATE_REQUIRES(
            class OtherExtents,
            /* requires */ (std::is_constructible_v<extents_type, OtherExtents>)
        )
        MDSPAN_CONDITIONAL_EXPLICIT((!std::is_convertible_v<OtherExtents, extents_type>))
        MDSPAN_INLINE_FUNCTION
        constexpr mapping(const layout_right::mapping<OtherExtents> &other_mapping)
            : padded_stride(
                  padded_stride_type::init_padding(other_mapping, std::integral_constant<size_t, padded_stride_idx>{})
              ),
              exts(other_mapping.extents()) {
            static_assert(
                (OtherExtents::rank() > 1) || (padded_stride_type::static_value() != dynamic_extent) ||
                (OtherExtents::static_extent(extent_to_pad_idx) != dynamic_extent) ||
                (padded_stride_type::static_value() == OtherExtents::static_extent(extent_to_pad_idx))
            );
            MDSPAN_IMPL_PRECONDITION(detail::check_extents_representability(exts));
            MDSPAN_IMPL_PRECONDITION(
                detail::check_extents_and_right_padding_representability<index_type>(exts, padded_stride.value(0))
            );
        }

        /**
         * Converting constructor from `layout_stride::mapping`.
         *
         * This overload participates in overload resolution only if
         * `is_constructible_v<extents_type, OtherExtents>` is true
         */
        MDSPAN_TEMPLATE_REQUIRES(
            class OtherExtents,
            /* requires */ (std::is_constructible_v<extents_type, OtherExtents>)
        )
        MDSPAN_CONDITIONAL_EXPLICIT((extents_type::rank() > 0))
        MDSPAN_INLINE_FUNCTION
        constexpr mapping(const layout_stride::mapping<OtherExtents> &other_mapping)
            : padded_stride(
                  padded_stride_type::init_padding(other_mapping, std::integral_constant<size_t, padded_stride_idx>{})
              ),
              exts(other_mapping.extents()) {
            MDSPAN_IMPL_PRECONDITION(detail::check_extents_representability(exts));
            MDSPAN_IMPL_PRECONDITION(
                detail::check_extents_and_right_padding_representability<index_type>(exts, padded_stride.value(0))
            );
        }

        /**
         * Converting constructor from `layout_right_padded::mapping`.
         *
         * This overload participates in overload resolution only if
         * `is_constructible_v<extents_type, OtherExtents>` is true. Either
         * `padding_value` or `OtherPaddingStride` must be `std::dynamic_extent`, or
         * `padding_value == OtherPaddingStride`.
         */
        MDSPAN_TEMPLATE_REQUIRES(
            class Mapping,
            /* requires */ (detail::is_layout_right_padded_mapping<Mapping>::value
                                &&std::is_constructible_v<extents_type, typename Mapping::extents_type>)
        )
        MDSPAN_CONDITIONAL_EXPLICIT(
            (extents_type::rank() > 1 && (padding_value == dynamic_extent || Mapping::padding_value == dynamic_extent))
        )
        MDSPAN_INLINE_FUNCTION
        constexpr mapping(const Mapping &other_mapping)
            : padded_stride(
                  padded_stride_type::init_padding(other_mapping, std::integral_constant<size_t, padded_stride_idx>{})
              ),
              exts(other_mapping.extents()) {
            static_assert(
                padding_value == dynamic_extent || Mapping::padding_value == dynamic_extent ||
                padding_value == Mapping::padding_value
            );
            MDSPAN_IMPL_PRECONDITION(detail::check_extents_representability(exts));
            MDSPAN_IMPL_PRECONDITION(
                detail::check_extents_and_right_padding_representability<index_type>(exts, padded_stride.value(0))
            );
        }

        /**
         * Converting constructor from `layout_left_padded::mapping`.
         *
         * This overload participates in overload resolution only if
         * `extents_type::rank()` is 0 or 1 and `is_constructible_v<extents_type,
         * OtherExtents>` is `true`.
         */
        MDSPAN_TEMPLATE_REQUIRES(
            class Mapping,
            /* requires */ (
                detail::is_layout_left_padded_mapping<Mapping>::value &&extents_type::rank() <= 1 &&
                std::is_constructible_v<extents_type, typename Mapping::extents_type>
            )
        )
        MDSPAN_CONDITIONAL_EXPLICIT((!std::is_convertible_v<typename Mapping::extents_type, extents_type>))
        MDSPAN_INLINE_FUNCTION
        constexpr mapping(const Mapping &other_mapping) noexcept
            : padded_stride(
                  padded_stride_type::init_padding(
                      static_cast<extents_type>(other_mapping.extents()),
                      other_mapping.extents().extent(extent_to_pad_idx)
                  )
              ),
              exts(other_mapping.extents()) {
            MDSPAN_IMPL_PRECONDITION(detail::check_extents_representability(exts));
            MDSPAN_IMPL_PRECONDITION(
                detail::check_extents_and_right_padding_representability<index_type>(exts, padded_stride.value(0))
            );
        }

        MDSPAN_INLINE_FUNCTION constexpr const extents_type &extents() const noexcept { return exts; }

        constexpr std::array<index_type, extents_type::rank()> strides() const noexcept {
            if constexpr (extents_type::rank() == 0) {
                return {};
            } else if constexpr (extents_type::rank() == 1) {
                return {1};
            } else {
                index_type value = 1;
                std::array<index_type, extents_type::rank()> s{};
                s[extent_to_pad_idx] = value;
                value *= padded_stride.value(0);
                for (rank_type r = extent_to_pad_idx - 1; r > 0; --r) {
                    s[r] = value;
                    value *= exts.extent(r);
                }
                s[0] = value;
                return s;
            }
        }

        MDSPAN_INLINE_FUNCTION constexpr index_type required_span_size() const noexcept {
            if constexpr (extents_type::rank() == 0) {
                return 1;
            } else if constexpr (extents_type::rank() == 1) {
                return exts.extent(0);
            } else {
                index_type value = padded_stride.value(0);
                for (rank_type r = 0; r < extent_to_pad_idx; ++r) {
                    value *= exts.extent(r);
                }
                return value == 0 ? 0 : value + exts.extent(extent_to_pad_idx) - padded_stride.value(0);
            }
        }

        /**
         * Return the mapping given the provided indices per rank.
         *
         * This overload participates in overload resolution only if:
         * - `sizeof...(Indices) == extents_type::rank()`,
         * - `(is_convertible_v<Indices, index_type> && ...) is true`, and
         * - (is_nothrow_constructible_v<index_type, Indices> && ...) is true.
         */
        MDSPAN_TEMPLATE_REQUIRES(
            class... Indices,
            /* requires */ (
                sizeof...(Indices) == extents_type::rank() &&
                (::MDSPAN_IMPL_STANDARD_NAMESPACE::detail::are_valid_indices<index_type, Indices...>())
            )
        )
        MDSPAN_INLINE_FUNCTION constexpr size_t operator()(Indices... idxs) const noexcept {
            return compute_offset(std::index_sequence_for<Indices...>{}, idxs...);
        }

        MDSPAN_INLINE_FUNCTION static constexpr bool is_always_unique() noexcept { return true; }
        MDSPAN_INLINE_FUNCTION static constexpr bool is_always_exhaustive() noexcept {
            return (extents_type::rank() <= rank_type(1)) ||
                   (extents_type::static_extent(extent_to_pad_idx) != dynamic_extent &&
                    extents_type::static_extent(extent_to_pad_idx) == padded_stride_type::static_value());
        }
        MDSPAN_INLINE_FUNCTION static constexpr bool is_always_strided() noexcept { return true; }

        MDSPAN_INLINE_FUNCTION static constexpr bool is_unique() noexcept { return true; }
        MDSPAN_INLINE_FUNCTION constexpr bool is_exhaustive() const noexcept {
            return (extents_type::rank() < 2) || (exts.extent(extent_to_pad_idx) == padded_stride.value(0));
        }
        MDSPAN_INLINE_FUNCTION static constexpr bool is_strided() noexcept { return true; }

        MDSPAN_INLINE_FUNCTION constexpr index_type stride(rank_type r) const noexcept {
            assert(r < extents_type::rank());
            if (r == extents_type::rank() - 1)
                return index_type(1);

            index_type value = padded_stride.value(0);
            for (rank_type k = extents_type::rank() - 2; k > r; k--)
                value *= exts.extent(k);

            return value;
        }

        /**
         * Equality operator between `layout_right_padded`s
         *
         * This overload only participates in overload resolution if
         * `OtherExtents::rank() == extents_type::rank()`.
         *
         * \note There is currently a difference from p2642r2, where this function is
         * specified as taking `layout_right_padded< padding_value >::mapping<
         * Extents>`. However, this makes `padding_value` non-deducible.
         */
        MDSPAN_TEMPLATE_REQUIRES(
            class Mapping,
            /* requires */ (
                detail::is_layout_right_padded_mapping<Mapping>::value &&
                (Mapping::extents_type::rank() == extents_type::rank())
            )
        )
        MDSPAN_INLINE_FUNCTION friend constexpr bool operator==(const mapping &left, const Mapping &right) noexcept {
            // Workaround for some compilers not short-circuiting properly with
            // compile-time checks i.e. we can't access stride(_padding_stride_idx) of a
            // rank 0 mapping
            bool strides_equal = true;
            if constexpr (extents_type::rank() > rank_type(1)) {
                strides_equal = left.stride(padded_stride_idx) == right.stride(padded_stride_idx);
            }
            return (left.extents() == right.extents()) && strides_equal;
        }

#if !MDSPAN_HAS_CXX_20
        /**
         * Inequality operator between `layout_right_padded`s
         *
         * This overload only participates in overload resolution if
         * `OtherExtents::rank() == extents_type::rank()`.
         */
        MDSPAN_TEMPLATE_REQUIRES(
            class Mapping,
            /* requires */ (
                detail::is_layout_right_padded_mapping<Mapping>::value &&
                (Mapping::extents_type::rank() == extents_type::rank())
            )
        )
        MDSPAN_INLINE_FUNCTION friend constexpr bool operator!=(const mapping &left, const Mapping &right) noexcept {
            return !(left == right);
        }
#endif

        // [mdspan.submdspan.mapping], submdspan mapping specialization
        template <class... SliceSpecifiers>
        MDSPAN_INLINE_FUNCTION constexpr auto submdspan_mapping_impl(SliceSpecifiers... slices) const;

        template <class... SliceSpecifiers>
        MDSPAN_INLINE_FUNCTION friend constexpr auto submdspan_mapping(const mapping &src, SliceSpecifiers... slices) {
            return src.submdspan_mapping_impl(slices...);
        }
    };
} // namespace MDSPAN_IMPL_PROPOSED_NAMESPACE
} // namespace MDSPAN_IMPL_STANDARD_NAMESPACE
// END_FILE_INCLUDE: mdspan/include/experimental/__p2642_bits/layout_padded.hpp
// BEGIN_FILE_INCLUDE: mdspan/include/experimental/__p2630_bits/submdspan.hpp
//@HEADER
//  ************************************************************************
//
//                         Kokkos v. 4.0
//        Copyright (2022) National Technology & Engineering
//                Solutions of Sandia, LLC (NTESS).
//
//  Under the terms of Contract DE-NA0003525 with NTESS,
//  the U.S. Government retains certain rights in this software.
//
//  Part of Kokkos, under the Apache License v2.0 with LLVM Exceptions.
//  See https://kokkos.org/LICENSE for license information.
//  SPDX-License-Identifier: Apache-2.0 WITH LLVM-exception
//
//@HEADER

// BEGIN_FILE_INCLUDE: mdspan/include/experimental/__p2630_bits/submdspan_extents.hpp
//@HEADER
//  ************************************************************************
//
//                         Kokkos v. 4.0
//        Copyright (2022) National Technology & Engineering
//                Solutions of Sandia, LLC (NTESS).
//
//  Under the terms of Contract DE-NA0003525 with NTESS,
//  the U.S. Government retains certain rights in this software.
//
//  Part of Kokkos, under the Apache License v2.0 with LLVM Exceptions.
//  See https://kokkos.org/LICENSE for license information.
//  SPDX-License-Identifier: Apache-2.0 WITH LLVM-exception
//
//@HEADER

#include <complex>

// BEGIN_FILE_INCLUDE: mdspan/include/experimental/__p2630_bits/strided_slice.hpp

//@HEADER
// ************************************************************************
//
//                        Kokkos v. 4.0
//       Copyright (2022) National Technology & Engineering
//               Solutions of Sandia, LLC (NTESS).
//
// Under the terms of Contract DE-NA0003525 with NTESS,
// the U.S. Government retains certain rights in this software.
//
// Part of Kokkos, under the Apache License v2.0 with LLVM Exceptions.
// See https://kokkos.org/LICENSE for license information.
// SPDX-License-Identifier: Apache-2.0 WITH LLVM-exception
//
//@HEADER

#include <type_traits>

namespace MDSPAN_IMPL_STANDARD_NAMESPACE {

namespace detail {
    template <class T>
    struct mdspan_is_integral_constant : std::false_type {};

    template <class T, T val>
    struct mdspan_is_integral_constant<std::integral_constant<T, val>> : std::true_type {};
} // namespace detail

// Slice Specifier allowing for strides and compile time extent
template <class OffsetType, class ExtentType, class StrideType>
struct strided_slice {
    using offset_type = OffsetType;
    using extent_type = ExtentType;
    using stride_type = StrideType;

    MDSPAN_IMPL_NO_UNIQUE_ADDRESS OffsetType offset{};
    MDSPAN_IMPL_NO_UNIQUE_ADDRESS ExtentType extent{};
    MDSPAN_IMPL_NO_UNIQUE_ADDRESS StrideType stride{};

    static_assert(std::is_integral_v<OffsetType> || detail::mdspan_is_integral_constant<OffsetType>::value);
    static_assert(std::is_integral_v<ExtentType> || detail::mdspan_is_integral_constant<ExtentType>::value);
    static_assert(std::is_integral_v<StrideType> || detail::mdspan_is_integral_constant<StrideType>::value);
};

} // namespace MDSPAN_IMPL_STANDARD_NAMESPACE
// END_FILE_INCLUDE: mdspan/include/experimental/__p2630_bits/strided_slice.hpp

namespace MDSPAN_IMPL_STANDARD_NAMESPACE {
namespace detail {

    // Mapping from submapping ranks to srcmapping ranks
    // InvMapRank is an index_sequence, which we build recursively
    // to contain the mapped indices.
    // end of recursion specialization containing the final index_sequence
    template <size_t Counter, size_t... MapIdxs>
    MDSPAN_INLINE_FUNCTION constexpr auto
    inv_map_rank(std::integral_constant<size_t, Counter>, std::index_sequence<MapIdxs...>) {
        return std::index_sequence<MapIdxs...>();
    }

    // specialization reducing rank by one (i.e., integral slice specifier)
    template <size_t Counter, class Slice, class... SliceSpecifiers, size_t... MapIdxs>
    MDSPAN_INLINE_FUNCTION constexpr auto inv_map_rank(
        std::integral_constant<size_t, Counter>, std::index_sequence<MapIdxs...>, Slice, SliceSpecifiers... slices
    ) {
        using next_idx_seq_t = std::conditional_t<
            std::is_convertible_v<Slice, size_t>, std::index_sequence<MapIdxs...>,
            std::index_sequence<MapIdxs..., Counter>>;

        return inv_map_rank(std::integral_constant<size_t, Counter + 1>(), next_idx_seq_t(), slices...);
    }

    // Helper for identifying strided_slice
    template <class T>
    struct is_strided_slice : std::false_type {};

    template <class OffsetType, class ExtentType, class StrideType>
    struct is_strided_slice<strided_slice<OffsetType, ExtentType, StrideType>> : std::true_type {};

    // Helper for identifying valid pair like things
    template <class T, class IndexType>
    struct index_pair_like : std::false_type {};

    template <class IdxT1, class IdxT2, class IndexType>
    struct index_pair_like<std::pair<IdxT1, IdxT2>, IndexType> {
        static constexpr bool value =
            std::is_convertible_v<IdxT1, IndexType> && std::is_convertible_v<IdxT2, IndexType>;
    };

    template <class IdxT1, class IdxT2, class IndexType>
    struct index_pair_like<std::tuple<IdxT1, IdxT2>, IndexType> {
        static constexpr bool value =
            std::is_convertible_v<IdxT1, IndexType> && std::is_convertible_v<IdxT2, IndexType>;
    };

    template <class IdxT1, class IdxT2, class IndexType>
    struct index_pair_like<tuple<IdxT1, IdxT2>, IndexType> {
        static constexpr bool value =
            std::is_convertible_v<IdxT1, IndexType> && std::is_convertible_v<IdxT2, IndexType>;
    };

    template <class IdxT, class IndexType>
    struct index_pair_like<std::complex<IdxT>, IndexType> {
        static constexpr bool value = std::is_convertible_v<IdxT, IndexType>;
    };

    template <class IdxT, class IndexType>
    struct index_pair_like<std::array<IdxT, 2>, IndexType> {
        static constexpr bool value = std::is_convertible_v<IdxT, IndexType>;
    };

    // first_of(slice): getting begin of slice specifier range
    MDSPAN_TEMPLATE_REQUIRES(
        class Integral,
        /* requires */ (std::is_convertible_v<Integral, size_t>)
    )
    MDSPAN_INLINE_FUNCTION
    constexpr Integral first_of(const Integral &i) { return i; }

    template <class Integral, Integral v>
    MDSPAN_INLINE_FUNCTION constexpr Integral first_of(const std::integral_constant<Integral, v> &) {
        return integral_constant<Integral, v>();
    }

    MDSPAN_INLINE_FUNCTION
    constexpr integral_constant<size_t, 0> first_of(const ::MDSPAN_IMPL_STANDARD_NAMESPACE::full_extent_t &) {
        return integral_constant<size_t, 0>();
    }

    MDSPAN_TEMPLATE_REQUIRES(
        class Slice,
        /* requires */ (index_pair_like<Slice, size_t>::value)
    )
    MDSPAN_INLINE_FUNCTION
    constexpr auto first_of(const Slice &i) { return get<0>(i); }

    MDSPAN_TEMPLATE_REQUIRES(
        class IdxT1, class IdxT2,
        /* requires */ (index_pair_like<std::tuple<IdxT1, IdxT2>, size_t>::value)
    )
    constexpr auto first_of(const std::tuple<IdxT1, IdxT2> &i) { return get<0>(i); }

    MDSPAN_TEMPLATE_REQUIRES(
        class IdxT1, class IdxT2,
        /* requires */ (index_pair_like<std::pair<IdxT1, IdxT2>, size_t>::value)
    )
    MDSPAN_INLINE_FUNCTION
    constexpr auto first_of(const std::pair<IdxT1, IdxT2> &i) { return i.first; }

    template <class T>
    MDSPAN_INLINE_FUNCTION constexpr auto first_of(const std::complex<T> &i) {
        return i.real();
    }

    template <class OffsetType, class ExtentType, class StrideType>
    MDSPAN_INLINE_FUNCTION constexpr OffsetType first_of(const strided_slice<OffsetType, ExtentType, StrideType> &r) {
        return r.offset;
    }

    // last_of(slice): getting end of slice specifier range
    // We need however not just the slice but also the extents
    // of the original view and which rank from the extents.
    // This is needed in the case of slice being full_extent_t.
    MDSPAN_TEMPLATE_REQUIRES(
        size_t k, class Extents, class Integral,
        /* requires */ (std::is_convertible_v<Integral, size_t>)
    )
    MDSPAN_INLINE_FUNCTION
    constexpr Integral last_of(std::integral_constant<size_t, k>, const Extents &, const Integral &i) { return i; }

    MDSPAN_TEMPLATE_REQUIRES(
        size_t k, class Extents, class Slice,
        /* requires */ (index_pair_like<Slice, size_t>::value)
    )
    MDSPAN_INLINE_FUNCTION
    constexpr auto last_of(std::integral_constant<size_t, k>, const Extents &, const Slice &i) { return get<1>(i); }

    MDSPAN_TEMPLATE_REQUIRES(
        size_t k, class Extents, class IdxT1, class IdxT2,
        /* requires */ (index_pair_like<std::tuple<IdxT1, IdxT2>, size_t>::value)
    )
    constexpr auto last_of(std::integral_constant<size_t, k>, const Extents &, const std::tuple<IdxT1, IdxT2> &i) {
        return get<1>(i);
    }

    MDSPAN_TEMPLATE_REQUIRES(
        size_t k, class Extents, class IdxT1, class IdxT2,
        /* requires */ (index_pair_like<std::pair<IdxT1, IdxT2>, size_t>::value)
    )
    MDSPAN_INLINE_FUNCTION
    constexpr auto last_of(std::integral_constant<size_t, k>, const Extents &, const std::pair<IdxT1, IdxT2> &i) {
        return i.second;
    }

    template <size_t k, class Extents, class T>
    MDSPAN_INLINE_FUNCTION constexpr auto
    last_of(std::integral_constant<size_t, k>, const Extents &, const std::complex<T> &i) {
        return i.imag();
    }

// Suppress spurious warning with NVCC about no return statement.
// This is a known issue in NVCC and NVC++
// Depending on the CUDA and GCC version we need both the builtin
// and the diagnostic push. I tried really hard to find something shorter
// but no luck ...
#if defined __NVCC__
#ifdef __NVCC_DIAG_PRAGMA_SUPPORT__
#pragma nv_diagnostic push
#pragma nv_diag_suppress = implicit_return_from_non_void_function
#else
#ifdef __CUDA_ARCH__
#pragma diagnostic push
#pragma diag_suppress implicit_return_from_non_void_function
#endif
#endif
#elif defined __NVCOMPILER
#pragma diagnostic push
#pragma diag_suppress = implicit_return_from_non_void_function
#endif
    template <size_t k, class Extents>
    MDSPAN_INLINE_FUNCTION constexpr auto
    last_of(std::integral_constant<size_t, k>, const Extents &ext, ::MDSPAN_IMPL_STANDARD_NAMESPACE::full_extent_t) {
        if constexpr (Extents::static_extent(k) == dynamic_extent) {
            return ext.extent(k);
        } else {
            return integral_constant<size_t, Extents::static_extent(k)>();
        }
#if defined(__NVCC__) && !defined(__CUDA_ARCH__) && defined(__GNUC__)
        // Even with CUDA_ARCH protection this thing warns about calling host function
        __builtin_unreachable();
#endif
    }
#if defined __NVCC__
#ifdef __NVCC_DIAG_PRAGMA_SUPPORT__
#pragma nv_diagnostic pop
#else
#ifdef __CUDA_ARCH__
#pragma diagnostic pop
#endif
#endif
#elif defined __NVCOMPILER
#pragma diagnostic pop
#endif

    template <size_t k, class Extents, class OffsetType, class ExtentType, class StrideType>
    MDSPAN_INLINE_FUNCTION constexpr OffsetType last_of(
        std::integral_constant<size_t, k>, const Extents &, const strided_slice<OffsetType, ExtentType, StrideType> &r
    ) {
        return r.extent;
    }

    // get stride of slices
    template <class T>
    MDSPAN_INLINE_FUNCTION constexpr auto stride_of(const T &) {
        return integral_constant<size_t, 1>();
    }

    template <class OffsetType, class ExtentType, class StrideType>
    MDSPAN_INLINE_FUNCTION constexpr auto stride_of(const strided_slice<OffsetType, ExtentType, StrideType> &r) {
        return r.stride;
    }

    // divide which can deal with integral constant preservation
    template <class IndexT, class T0, class T1>
    MDSPAN_INLINE_FUNCTION constexpr auto divide(const T0 &v0, const T1 &v1) {
        return IndexT(v0) / IndexT(v1);
    }

    template <class IndexT, class T0, T0 v0, class T1, T1 v1>
    MDSPAN_INLINE_FUNCTION constexpr auto
    divide(const std::integral_constant<T0, v0> &, const std::integral_constant<T1, v1> &) {
        // cutting short division by zero
        // this is used for strided_slice with zero extent/stride
        return integral_constant<IndexT, v0 == 0 ? 0 : v0 / v1>();
    }

    // multiply which can deal with integral constant preservation
    template <class IndexT, class T0, class T1>
    MDSPAN_INLINE_FUNCTION constexpr auto multiply(const T0 &v0, const T1 &v1) {
        return IndexT(v0) * IndexT(v1);
    }

    template <class IndexT, class T0, T0 v0, class T1, T1 v1>
    MDSPAN_INLINE_FUNCTION constexpr auto
    multiply(const std::integral_constant<T0, v0> &, const std::integral_constant<T1, v1> &) {
        return integral_constant<IndexT, v0 * v1>();
    }

    // compute new static extent from range, preserving static knowledge
    template <class Arg0, class Arg1>
    struct StaticExtentFromRange {
        constexpr static size_t value = dynamic_extent;
    };

    template <class Integral0, Integral0 val0, class Integral1, Integral1 val1>
    struct StaticExtentFromRange<std::integral_constant<Integral0, val0>, std::integral_constant<Integral1, val1>> {
        constexpr static size_t value = val1 - val0;
    };

    template <class Integral0, Integral0 val0, class Integral1, Integral1 val1>
    struct StaticExtentFromRange<integral_constant<Integral0, val0>, integral_constant<Integral1, val1>> {
        constexpr static size_t value = val1 - val0;
    };

    // compute new static extent from strided_slice, preserving static
    // knowledge
    template <class Arg0, class Arg1>
    struct StaticExtentFromStridedRange {
        constexpr static size_t value = dynamic_extent;
    };

    template <class Integral0, Integral0 val0, class Integral1, Integral1 val1>
    struct StaticExtentFromStridedRange<
        std::integral_constant<Integral0, val0>, std::integral_constant<Integral1, val1>> {
        constexpr static size_t value = val0 > 0 ? 1 + (val0 - 1) / val1 : 0;
    };

    template <class Integral0, Integral0 val0, class Integral1, Integral1 val1>
    struct StaticExtentFromStridedRange<integral_constant<Integral0, val0>, integral_constant<Integral1, val1>> {
        constexpr static size_t value = val0 > 0 ? 1 + (val0 - 1) / val1 : 0;
    };

    // creates new extents through recursive calls to next_extent member function
    // next_extent has different overloads for different types of stride specifiers
    template <size_t K, class Extents, size_t... NewExtents>
    struct extents_constructor {
        MDSPAN_TEMPLATE_REQUIRES(
            class Slice, class... SlicesAndExtents,
            /* requires */ (!std::is_convertible_v<Slice, size_t> && !is_strided_slice<Slice>::value)
        )
        MDSPAN_INLINE_FUNCTION
        constexpr static auto next_extent(const Extents &ext, const Slice &sl, SlicesAndExtents... slices_and_extents) {
            constexpr size_t new_static_extent = StaticExtentFromRange<
                decltype(first_of(std::declval<Slice>())), decltype(last_of(
                                                               std::integral_constant<size_t, Extents::rank() - K>(),
                                                               std::declval<Extents>(), std::declval<Slice>()
                                                           ))>::value;

            using next_t = extents_constructor<K - 1, Extents, NewExtents..., new_static_extent>;
            using index_t = typename Extents::index_type;
            return next_t::next_extent(
                ext, slices_and_extents...,
                index_t(last_of(std::integral_constant<size_t, Extents::rank() - K>(), ext, sl)) - index_t(first_of(sl))
            );
        }

        MDSPAN_TEMPLATE_REQUIRES(
            class Slice, class... SlicesAndExtents,
            /* requires */ (std::is_convertible_v<Slice, size_t>)
        )
        MDSPAN_INLINE_FUNCTION
        constexpr static auto next_extent(const Extents &ext, const Slice &, SlicesAndExtents... slices_and_extents) {
            using next_t = extents_constructor<K - 1, Extents, NewExtents...>;
            return next_t::next_extent(ext, slices_and_extents...);
        }

        template <class OffsetType, class ExtentType, class StrideType, class... SlicesAndExtents>
        MDSPAN_INLINE_FUNCTION constexpr static auto next_extent(
            const Extents &ext, const strided_slice<OffsetType, ExtentType, StrideType> &r,
            SlicesAndExtents... slices_and_extents
        ) {
            using index_t = typename Extents::index_type;
            using new_static_extent_t = StaticExtentFromStridedRange<ExtentType, StrideType>;
            if constexpr (new_static_extent_t::value == dynamic_extent) {
                using next_t = extents_constructor<K - 1, Extents, NewExtents..., dynamic_extent>;
                return next_t::next_extent(
                    ext, slices_and_extents..., r.extent > 0 ? 1 + divide<index_t>(r.extent - 1, r.stride) : 0
                );
            } else {
                constexpr size_t new_static_extent = new_static_extent_t::value;
                using next_t = extents_constructor<K - 1, Extents, NewExtents..., new_static_extent>;
                return next_t::next_extent(
                    ext, slices_and_extents..., index_t(divide<index_t>(ExtentType(), StrideType()))
                );
            }
        }
    };

    template <class Extents, size_t... NewStaticExtents>
    struct extents_constructor<0, Extents, NewStaticExtents...> {

        template <class... NewExtents>
        MDSPAN_INLINE_FUNCTION constexpr static auto next_extent(const Extents &, NewExtents... new_exts) {
            return extents<typename Extents::index_type, NewStaticExtents...>(new_exts...);
        }
    };

} // namespace detail

// submdspan_extents creates new extents given src extents and submdspan slice
// specifiers
template <class IndexType, size_t... Extents, class... SliceSpecifiers>
MDSPAN_INLINE_FUNCTION constexpr auto
submdspan_extents(const extents<IndexType, Extents...> &src_exts, SliceSpecifiers... slices) {

    using ext_t = extents<IndexType, Extents...>;
    return detail::extents_constructor<ext_t::rank(), ext_t>::next_extent(src_exts, slices...);
}
} // namespace MDSPAN_IMPL_STANDARD_NAMESPACE
// END_FILE_INCLUDE: mdspan/include/experimental/__p2630_bits/submdspan_extents.hpp
// BEGIN_FILE_INCLUDE: mdspan/include/experimental/__p2630_bits/submdspan_mapping.hpp
//@HEADER
//  ************************************************************************
//
//                         Kokkos v. 4.0
//        Copyright (2022) National Technology & Engineering
//                Solutions of Sandia, LLC (NTESS).
//
//  Under the terms of Contract DE-NA0003525 with NTESS,
//  the U.S. Government retains certain rights in this software.
//
//  Part of Kokkos, under the Apache License v2.0 with LLVM Exceptions.
//  See https://kokkos.org/LICENSE for license information.
//  SPDX-License-Identifier: Apache-2.0 WITH LLVM-exception
//
//@HEADER

#include <array>
#include <type_traits>
#include <utility> // index_sequence

// Suppress spurious warning with NVCC about no return statement.
// This is a known issue in NVCC and NVC++
// Depending on the CUDA and GCC version we need both the builtin
// and the diagnostic push. I tried really hard to find something shorter
// but no luck ...
#if defined __NVCC__
#ifdef __NVCC_DIAG_PRAGMA_SUPPORT__
#pragma nv_diagnostic push
#pragma nv_diag_suppress = implicit_return_from_non_void_function
#else
#ifdef __CUDA_ARCH__
#pragma diagnostic push
#pragma diag_suppress implicit_return_from_non_void_function
#endif
#endif
#elif defined __NVCOMPILER
#pragma diagnostic push
#pragma diag_suppress = implicit_return_from_non_void_function
#endif

namespace MDSPAN_IMPL_STANDARD_NAMESPACE {
//******************************************
// Return type of submdspan_mapping overloads
//******************************************
template <class LayoutMapping>
struct submdspan_mapping_result {
    MDSPAN_IMPL_NO_UNIQUE_ADDRESS LayoutMapping mapping{};
    size_t offset;
};

namespace detail {

    // We use const Slice& and not Slice&& because the various
    // submdspan_mapping_impl overloads use their slices arguments
    // multiple times.  This makes perfect forwarding not useful, but we
    // still don't want to pass those (possibly of size 64 x 3 bits)
    // objects by value.
    template <class IndexType, class Slice>
    MDSPAN_INLINE_FUNCTION constexpr bool one_slice_out_of_bounds(const IndexType &ext, const Slice &slice) {
        using common_t = std::common_type_t<decltype(detail::first_of(slice)), IndexType>;
        return static_cast<common_t>(detail::first_of(slice)) == static_cast<common_t>(ext);
    }

    template <size_t... RankIndices, class IndexType, size_t... Exts, class... Slices>
    MDSPAN_INLINE_FUNCTION constexpr bool any_slice_out_of_bounds_helper(
        std::index_sequence<RankIndices...>, const extents<IndexType, Exts...> &exts, const Slices &...slices
    ) {
        return MDSPAN_IMPL_FOLD_OR((one_slice_out_of_bounds(exts.extent(RankIndices), slices)));
    }

    template <class IndexType, size_t... Exts, class... Slices>
    MDSPAN_INLINE_FUNCTION constexpr bool
    any_slice_out_of_bounds(const extents<IndexType, Exts...> &exts, const Slices &...slices) {
        return any_slice_out_of_bounds_helper(std::make_index_sequence<sizeof...(Slices)>(), exts, slices...);
    }

    // constructs sub strides
    template <class T, size_t N>
    struct sub_strides {
        T values[N > 0 ? N : 1];
    };

    template <class SrcMapping, class... slice_strides, size_t... InvMapIdxs>
    MDSPAN_INLINE_FUNCTION constexpr auto construct_sub_strides(
        const SrcMapping &src_mapping, std::index_sequence<InvMapIdxs...>,
        const MDSPAN_IMPL_STANDARD_NAMESPACE::detail::tuple<slice_strides...> &slices_stride_factor
    ) {
        using index_type = typename SrcMapping::index_type;
        return sub_strides<typename SrcMapping::index_type, sizeof...(InvMapIdxs)>{
            {(static_cast<index_type>(src_mapping.stride(InvMapIdxs)) *
              static_cast<index_type>(get<InvMapIdxs>(slices_stride_factor)))...}
        };
    }

    template <class SliceSpecifier, class IndexType>
    struct is_range_slice {
        constexpr static bool value =
            std::is_same_v<SliceSpecifier, full_extent_t> || index_pair_like<SliceSpecifier, IndexType>::value;
    };

    template <class SliceSpecifier, class IndexType>
    constexpr bool is_range_slice_v = is_range_slice<SliceSpecifier, IndexType>::value;

    template <class SliceSpecifier, class IndexType>
    struct is_index_slice {
        constexpr static bool value = std::is_convertible_v<SliceSpecifier, IndexType>;
    };

    template <class SliceSpecifier, class IndexType>
    constexpr bool is_index_slice_v = is_index_slice<SliceSpecifier, IndexType>::value;

} // namespace detail

//**********************************
// layout_left submdspan_mapping
//*********************************
namespace detail {

    // Figure out whether to preserve layout_left
    template <class IndexType, size_t SubRank, class IndexSequence, class... SliceSpecifiers>
    struct deduce_layout_left_submapping;

    template <class IndexType, size_t SubRank, size_t... Idx, class... SliceSpecifiers>
    struct deduce_layout_left_submapping<IndexType, SubRank, std::index_sequence<Idx...>, SliceSpecifiers...> {

        using count_range = index_sequence_scan_impl<0u, (is_index_slice_v<SliceSpecifiers, IndexType> ? 0u : 1u)...>;

        constexpr static int gap_len =
            (((Idx > 0 && count_range::get(Idx) == 1 && is_index_slice_v<SliceSpecifiers, IndexType>) ? 1 : 0) + ... +
             0);

        MDSPAN_INLINE_FUNCTION
        constexpr static bool layout_left_value() {
            // Use layout_left for rank 0
            if constexpr (SubRank == 0) {
                return true;
                // Use layout_left for rank 1 result if leftmost slice specifier is range like
            } else if constexpr (SubRank == 1) {
                return ((Idx > 0 || is_range_slice_v<SliceSpecifiers, IndexType>) && ...);
            } else {
                // Preserve if leftmost SubRank-1 slices are full_extent_t and
                // the slice at idx Subrank - 1 is a range and
                // for idx > SubRank the slice is an index
                return (
                    (((Idx < SubRank - 1) && std::is_same_v<SliceSpecifiers, full_extent_t>) ||
                     ((Idx == SubRank - 1) && is_range_slice_v<SliceSpecifiers, IndexType>) ||
                     ((Idx > SubRank - 1) && is_index_slice_v<SliceSpecifiers, IndexType>)) &&
                    ...
                );
            }
#if defined(__NVCC__) && !defined(__CUDA_ARCH__) && defined(__GNUC__)
            __builtin_unreachable();
#endif
        }

        MDSPAN_INLINE_FUNCTION
        constexpr static bool layout_left_padded_value() {
            // Technically could also keep layout_left_padded for SubRank==0
            // and SubRank==1 with leftmost slice specifier being a contiguous range
            // but we intercept these cases separately

            // In all other cases:
            // leftmost slice must be range
            // then there can be a gap with index slices
            // then SubRank - 2 full_extent slices
            // then another range slice
            // then more index slices
            // e.g. R I I I F F F R I I for obtaining a rank-5 from a rank-10
            return (
                (((Idx == 0) && is_range_slice_v<SliceSpecifiers, IndexType>) ||
                 ((Idx > 0 && Idx <= gap_len) && is_index_slice_v<SliceSpecifiers, IndexType>) ||
                 ((Idx > gap_len && Idx < gap_len + SubRank - 1) && std::is_same_v<SliceSpecifiers, full_extent_t>) ||
                 ((Idx == gap_len + SubRank - 1) && is_range_slice_v<SliceSpecifiers, IndexType>) ||
                 ((Idx > gap_len + SubRank - 1) && is_index_slice_v<SliceSpecifiers, IndexType>)) &&
                ...
            );
        }
    };

    // We are reusing the same thing for layout_left and layout_left_padded
    // For layout_left as source StaticStride is static_extent(0)
    template <class Extents, size_t NumGaps, size_t StaticStride, size_t... Idx>
    MDSPAN_INLINE_FUNCTION constexpr size_t compute_s_static_layout_left(std::index_sequence<Idx...>) {
        // Neither StaticStride nor any of the provided extents can be zero.
        // StaticStride can never be zero, the static_extents we are looking at are associated with
        // integral slice specifiers - which wouldn't be valid for zero extent
        size_t val =
            ((Idx > 0 && Idx <= NumGaps
                  ? (Extents::static_extent(Idx) == dynamic_extent ? 0 : Extents::static_extent(Idx))
                  : 1) *
             ... * (StaticStride == dynamic_extent ? 0 : StaticStride));
        return val == 0 ? dynamic_extent : val;
    }

} // namespace detail

// Actual submdspan mapping call
template <class Extents>
template <class... SliceSpecifiers>
MDSPAN_INLINE_FUNCTION constexpr auto
layout_left::mapping<Extents>::submdspan_mapping_impl(SliceSpecifiers... slices) const {

    // compute sub extents
    using src_ext_t = Extents;
    auto dst_ext = submdspan_extents(extents(), slices...);
    using dst_ext_t = decltype(dst_ext);

    // figure out sub layout type
    using deduce_layout = detail::deduce_layout_left_submapping<
        typename dst_ext_t::index_type, dst_ext_t::rank(), std::make_index_sequence<src_ext_t::rank()>,
        SliceSpecifiers...>;

    // Figure out if any slice's lower bound equals the corresponding extent.
    // If so, bypass evaluating the layout mapping.  This fixes LWG Issue 4060.
    const bool out_of_bounds = detail::any_slice_out_of_bounds(this->extents(), slices...);
    auto offset =
        static_cast<size_t>(out_of_bounds ? this->required_span_size() : this->operator()(detail::first_of(slices)...));

    if constexpr (deduce_layout::layout_left_value()) {
        // layout_left case
        using dst_mapping_t = typename layout_left::template mapping<dst_ext_t>;
        return submdspan_mapping_result<dst_mapping_t>{dst_mapping_t(dst_ext), offset};
    } else if constexpr (deduce_layout::layout_left_padded_value()) {
        constexpr size_t S_static = MDSPAN_IMPL_STANDARD_NAMESPACE::detail::compute_s_static_layout_left<
            Extents, deduce_layout::gap_len, Extents::static_extent(0)>(std::make_index_sequence<Extents::rank()>());
        using dst_mapping_t =
            typename MDSPAN_IMPL_PROPOSED_NAMESPACE::layout_left_padded<S_static>::template mapping<dst_ext_t>;
        return submdspan_mapping_result<dst_mapping_t>{
            dst_mapping_t(dst_ext, stride(1 + deduce_layout::gap_len)), offset
        };
    } else {
        // layout_stride case
        using dst_mapping_t = typename layout_stride::mapping<dst_ext_t>;
        auto inv_map = detail::inv_map_rank(std::integral_constant<size_t, 0>(), std::index_sequence<>(), slices...);
        return submdspan_mapping_result<dst_mapping_t>{
            dst_mapping_t(
                mdspan_non_standard, dst_ext,
                detail::construct_sub_strides(
                    *this, inv_map,
// HIP needs deduction guides to have markups so we need to be explicit
// NVCC 11.0 has a bug with deduction guide here, tested that 11.2 does not have
// the issue but Clang-CUDA also doesn't accept the use of deduction guide so
// disable it for CUDA altogether
#if defined(MDSPAN_IMPL_HAS_HIP) || defined(MDSPAN_IMPL_HAS_CUDA)
                    detail::tuple<decltype(detail::stride_of(slices))...>{detail::stride_of(slices)...}
                )
                    .values
            ),
#else
                    detail::tuple{detail::stride_of(slices)...}
                )
                    .values
            ),
#endif
            offset
        };
    }
#if defined(__NVCC__) && !defined(__CUDA_ARCH__) && defined(__GNUC__)
    __builtin_unreachable();
#endif
}

template <size_t PaddingValue>
template <class Extents>
template <class... SliceSpecifiers>
MDSPAN_INLINE_FUNCTION constexpr auto
MDSPAN_IMPL_PROPOSED_NAMESPACE::layout_left_padded<PaddingValue>::mapping<Extents>::submdspan_mapping_impl(
    SliceSpecifiers... slices
) const {

    // compute sub extents
    using src_ext_t = Extents;
    auto dst_ext = submdspan_extents(extents(), slices...);
    using dst_ext_t = decltype(dst_ext);

    if constexpr (Extents::rank() == 0) { // rank-0 case
        using dst_mapping_t =
            typename MDSPAN_IMPL_PROPOSED_NAMESPACE::layout_left_padded<PaddingValue>::template mapping<Extents>;
        return submdspan_mapping_result<dst_mapping_t>{*this, 0};
    } else {
        const bool out_of_bounds =
            MDSPAN_IMPL_STANDARD_NAMESPACE::detail::any_slice_out_of_bounds(this->extents(), slices...);
        auto offset = static_cast<size_t>(
            out_of_bounds ? this->required_span_size()
                          : this->operator()(MDSPAN_IMPL_STANDARD_NAMESPACE::detail::first_of(slices)...)
        );
        if constexpr (dst_ext_t::rank() == 0) { // result rank-0
            // The following for some reasons leads to compiler error later, while not using a typedef works:
            // Compilers: CUDA 11.2 with GCC 9.1
            //
            // using dst_mapping_t = typename layout_left::template mapping<dst_ext_t>;
            // return submdspan_mapping_result<dst_mapping_t>{dst_mapping_t{dst_ext}, offset};
            //
            // Error: submdspan_mapping.hpp:299:23: error: 'dst_mapping_t' does not name a type
            //         299 |         using dst_mapping_t = typename layout_left::template mapping<dst_ext_t>;
            // The same error is given (about dst_mapping_t not naming type) when a different name is used in 299:
            //        using dst_mapping_t2 = typename layout_left::template mapping<dst_ext_t>;

            return submdspan_mapping_result<typename layout_left::template mapping<dst_ext_t>>{
                typename layout_left::template mapping<dst_ext_t>{dst_ext}, offset
            };
        } else { // general case
            // Figure out if any slice's lower bound equals the corresponding extent.
            // If so, bypass evaluating the layout mapping.  This fixes LWG Issue 4060.
            // figure out sub layout type
            using deduce_layout = MDSPAN_IMPL_STANDARD_NAMESPACE::detail::deduce_layout_left_submapping<
                typename dst_ext_t::index_type, dst_ext_t::rank(),
                decltype(std::make_index_sequence<src_ext_t::rank()>()), SliceSpecifiers...>;

            if constexpr (deduce_layout::layout_left_value() &&
                          dst_ext_t::rank() == 1) { // getting rank-1 from leftmost
                using dst_mapping_t = typename layout_left::template mapping<dst_ext_t>;
                return submdspan_mapping_result<dst_mapping_t>{dst_mapping_t{dst_ext}, offset};
            } else if constexpr (deduce_layout::layout_left_padded_value()) { // can keep layout_left_padded
                constexpr size_t S_static = MDSPAN_IMPL_STANDARD_NAMESPACE::detail::compute_s_static_layout_left<
                    Extents, deduce_layout::gap_len, static_padding_stride>(
                    std::make_index_sequence<Extents::rank()>()
                );
                using dst_mapping_t =
                    typename MDSPAN_IMPL_PROPOSED_NAMESPACE::layout_left_padded<S_static>::template mapping<dst_ext_t>;
                return submdspan_mapping_result<dst_mapping_t>{
                    dst_mapping_t(dst_ext, stride(1 + deduce_layout::gap_len)), offset
                };
            } else { // layout_stride
                auto inv_map = MDSPAN_IMPL_STANDARD_NAMESPACE::detail::inv_map_rank(
                    std::integral_constant<size_t, 0>(), std::index_sequence<>(), slices...
                );
                using dst_mapping_t = typename layout_stride::template mapping<dst_ext_t>;
                return submdspan_mapping_result<dst_mapping_t>{
                    dst_mapping_t(
                        mdspan_non_standard, dst_ext,
                        MDSPAN_IMPL_STANDARD_NAMESPACE::detail::construct_sub_strides(
                            *this, inv_map,
// HIP needs deduction guides to have markups so we need to be explicit
// NVCC 11.0 has a bug with deduction guide here, tested that 11.2 does not have
// the issue but Clang-CUDA also doesn't accept the use of deduction guide so
// disable it for CUDA alltogether
#if defined(MDSPAN_IMPL_HAS_HIP) || defined(MDSPAN_IMPL_HAS_CUDA)
                            MDSPAN_IMPL_STANDARD_NAMESPACE::detail::tuple<
                                decltype(MDSPAN_IMPL_STANDARD_NAMESPACE::detail::stride_of(slices))...>{
                                MDSPAN_IMPL_STANDARD_NAMESPACE::detail::stride_of(slices)...
                            }
                        )
                            .values
                    ),
#else
                            MDSPAN_IMPL_STANDARD_NAMESPACE::detail::tuple{
                                MDSPAN_IMPL_STANDARD_NAMESPACE::detail::stride_of(slices)...
                            }
                        )
                            .values
                    ),
#endif
                    offset
                };
            }
        }
    }

#if defined(__NVCC__) && !defined(__CUDA_ARCH__) && defined(__GNUC__)
    __builtin_unreachable();
#endif
}

//**********************************
// layout_right submdspan_mapping
//*********************************
namespace detail {

    // Figure out whether to preserve layout_right
    template <class IndexType, size_t SubRank, class IndexSequence, class... SliceSpecifiers>
    struct deduce_layout_right_submapping;

    template <class IndexType, size_t SubRank, size_t... Idx, class... SliceSpecifiers>
    struct deduce_layout_right_submapping<IndexType, SubRank, std::index_sequence<Idx...>, SliceSpecifiers...> {

        static constexpr size_t Rank = sizeof...(Idx);
        using count_range =
            index_sequence_scan_impl<0u, (std::is_convertible_v<SliceSpecifiers, IndexType> ? 0u : 1u)...>;
        //__static_partial_sums<!std::is_convertible_v<SliceSpecifiers,
        // IndexType>...>;
        constexpr static int gap_len =
            (((Idx < Rank - 1 && count_range::get(Idx) == SubRank - 1 &&
               std::is_convertible_v<SliceSpecifiers, IndexType>)
                  ? 1
                  : 0) +
             ... + 0);

        MDSPAN_INLINE_FUNCTION
        constexpr static bool layout_right_value() {
            // Use layout_right for rank 0
            if constexpr (SubRank == 0) {
                return true;
                // Use layout_right for rank 1 result if rightmost slice specifier is range like
            } else if constexpr (SubRank == 1) {
                return ((Idx < Rank - 1 || is_range_slice_v<SliceSpecifiers, IndexType>) && ...);
            } else {
                // Preserve if rightmost SubRank-1 slices are full_extent_t and
                // the slice at idx Rank-Subrank is a range and
                // for idx < Rank - SubRank the slice is an index
                return (
                    (((Idx >= Rank - SubRank) && std::is_same_v<SliceSpecifiers, full_extent_t>) ||
                     ((Idx == Rank - SubRank) && is_range_slice_v<SliceSpecifiers, IndexType>) ||
                     ((Idx < Rank - SubRank) && is_index_slice_v<SliceSpecifiers, IndexType>)) &&
                    ...
                );
            }
#if defined(__NVCC__) && !defined(__CUDA_ARCH__) && defined(__GNUC__)
            __builtin_unreachable();
#endif
        }

        MDSPAN_INLINE_FUNCTION
        constexpr static bool layout_right_padded_value() {
            // Technically could also keep layout_right_padded for SubRank==0
            // and SubRank==1 with rightmost slice specifier being a contiguous range
            // but we intercept these cases separately

            // In all other cases:
            // rightmost slice must be range
            // then there can be a gap with index slices
            // then SubRank - 2 full_extent slices
            // then another range slice
            // then more index slices
            // e.g. I I R F F F I I I R for obtaining a rank-5 from a rank-10
            return (
                (((Idx == Rank - 1) && is_range_slice_v<SliceSpecifiers, IndexType>) ||
                 ((Idx >= Rank - gap_len - 1 && Idx < Rank - 1) && is_index_slice_v<SliceSpecifiers, IndexType>) ||
                 ((Idx > Rank - gap_len - SubRank && Idx < Rank - gap_len - 1) &&
                  std::is_same_v<SliceSpecifiers, full_extent_t>) ||
                 ((Idx == Rank - gap_len - SubRank) && is_range_slice_v<SliceSpecifiers, IndexType>) ||
                 ((Idx < Rank - gap_len - SubRank) && is_index_slice_v<SliceSpecifiers, IndexType>)) &&
                ...
            );
        }
    };

    // We are reusing the same thing for layout_right and layout_right_padded
    // For layout_right as source StaticStride is static_extent(Rank-1)
    template <class Extents, size_t NumGaps, size_t StaticStride, size_t... Idx>
    MDSPAN_INLINE_FUNCTION constexpr size_t compute_s_static_layout_right(std::index_sequence<Idx...>) {
        // Neither StaticStride nor any of the provided extents can be zero.
        // StaticStride can never be zero, the static_extents we are looking at are associated with
        // integral slice specifiers - which wouldn't be valid for zero extent
        size_t val =
            ((Idx >= Extents::rank() - 1 - NumGaps && Idx < Extents::rank() - 1
                  ? (Extents::static_extent(Idx) == dynamic_extent ? 0 : Extents::static_extent(Idx))
                  : 1) *
             ... * (StaticStride == dynamic_extent ? 0 : StaticStride));
        return val == 0 ? dynamic_extent : val;
    }

} // namespace detail

// Actual submdspan mapping call
template <class Extents>
template <class... SliceSpecifiers>
MDSPAN_INLINE_FUNCTION constexpr auto
layout_right::mapping<Extents>::submdspan_mapping_impl(SliceSpecifiers... slices) const {

    // compute sub extents
    using src_ext_t = Extents;
    auto dst_ext = submdspan_extents(extents(), slices...);
    using dst_ext_t = decltype(dst_ext);

    // figure out sub layout type
    using deduce_layout = detail::deduce_layout_right_submapping<
        typename dst_ext_t::index_type, dst_ext_t::rank(), std::make_index_sequence<src_ext_t::rank()>,
        SliceSpecifiers...>;

    // Figure out if any slice's lower bound equals the corresponding extent.
    // If so, bypass evaluating the layout mapping.  This fixes LWG Issue 4060.
    const bool out_of_bounds = detail::any_slice_out_of_bounds(this->extents(), slices...);
    auto offset =
        static_cast<size_t>(out_of_bounds ? this->required_span_size() : this->operator()(detail::first_of(slices)...));

    if constexpr (deduce_layout::layout_right_value()) {
        // layout_right case
        using dst_mapping_t = typename layout_right::mapping<dst_ext_t>;
        return submdspan_mapping_result<dst_mapping_t>{dst_mapping_t(dst_ext), offset};
    } else if constexpr (deduce_layout::layout_right_padded_value()) {
        constexpr size_t S_static = MDSPAN_IMPL_STANDARD_NAMESPACE::detail::compute_s_static_layout_left<
            Extents, deduce_layout::gap_len, Extents::static_extent(Extents::rank() - 1)>(
            std::make_index_sequence<Extents::rank()>()
        );
        using dst_mapping_t =
            typename MDSPAN_IMPL_PROPOSED_NAMESPACE::layout_right_padded<S_static>::template mapping<dst_ext_t>;
        return submdspan_mapping_result<dst_mapping_t>{
            dst_mapping_t(dst_ext, stride(src_ext_t::rank() - 2 - deduce_layout::gap_len)), offset
        };
    } else {
        // layout_stride case
        using dst_mapping_t = typename layout_stride::mapping<dst_ext_t>;
        auto inv_map = detail::inv_map_rank(std::integral_constant<size_t, 0>(), std::index_sequence<>(), slices...);
        return submdspan_mapping_result<dst_mapping_t>{
            dst_mapping_t(
                mdspan_non_standard, dst_ext,
                detail::construct_sub_strides(
                    *this, inv_map,
// HIP needs deduction guides to have markups so we need to be explicit
// NVCC 11.0 has a bug with deduction guide here, tested that 11.2 does not have
// the issue but Clang-CUDA also doesn't accept the use of deduction guide so
// disable it for CUDA altogether
#if defined(MDSPAN_IMPL_HAS_HIP) || defined(MDSPAN_IMPL_HAS_CUDA)
                    MDSPAN_IMPL_STANDARD_NAMESPACE::detail::tuple<decltype(detail::stride_of(slices))...>{
                        detail::stride_of(slices)...
                    }
                )
                    .values
            ),
#else
                    MDSPAN_IMPL_STANDARD_NAMESPACE::detail::tuple{detail::stride_of(slices)...}
                )
                    .values
            ),
#endif
            offset
        };
    }
#if defined(__NVCC__) && !defined(__CUDA_ARCH__) && defined(__GNUC__)
    __builtin_unreachable();
#endif
}

template <size_t PaddingValue>
template <class Extents>
template <class... SliceSpecifiers>
MDSPAN_INLINE_FUNCTION constexpr auto
MDSPAN_IMPL_PROPOSED_NAMESPACE::layout_right_padded<PaddingValue>::mapping<Extents>::submdspan_mapping_impl(
    SliceSpecifiers... slices
) const {

    // compute sub extents
    using src_ext_t = Extents;
    auto dst_ext = submdspan_extents(extents(), slices...);
    using dst_ext_t = decltype(dst_ext);

    if constexpr (Extents::rank() == 0) { // rank-0 case
        using dst_mapping_t =
            typename MDSPAN_IMPL_PROPOSED_NAMESPACE::layout_right_padded<PaddingValue>::template mapping<Extents>;
        return submdspan_mapping_result<dst_mapping_t>{*this, 0};
    } else {
        // Figure out if any slice's lower bound equals the corresponding extent.
        // If so, bypass evaluating the layout mapping.  This fixes LWG Issue 4060.
        // figure out sub layout type
        const bool out_of_bounds =
            MDSPAN_IMPL_STANDARD_NAMESPACE::detail::any_slice_out_of_bounds(this->extents(), slices...);
        auto offset = static_cast<size_t>(
            out_of_bounds ? this->required_span_size()
                          : this->operator()(MDSPAN_IMPL_STANDARD_NAMESPACE::detail::first_of(slices)...)
        );
        if constexpr (dst_ext_t::rank() == 0) { // result rank-0
            // Same issue as in layout_left_padded: see comment there
            // using dst_mapping_t = typename layout_right::template mapping<dst_ext_t>;
            // return submdspan_mapping_result<dst_mapping_t>{dst_mapping_t{dst_ext}, offset};
            return submdspan_mapping_result<typename layout_right::template mapping<dst_ext_t>>{
                typename layout_right::template mapping<dst_ext_t>{dst_ext}, offset
            };
        } else { // general case
            using deduce_layout = MDSPAN_IMPL_STANDARD_NAMESPACE::detail::deduce_layout_right_submapping<
                typename dst_ext_t::index_type, dst_ext_t::rank(),
                decltype(std::make_index_sequence<src_ext_t::rank()>()), SliceSpecifiers...>;

            if constexpr (deduce_layout::layout_right_value() &&
                          dst_ext_t::rank() == 1) { // getting rank-1 from rightmost
                using dst_mapping_t = typename layout_right::template mapping<dst_ext_t>;
                return submdspan_mapping_result<dst_mapping_t>{dst_mapping_t{dst_ext}, offset};
            } else if constexpr (deduce_layout::layout_right_padded_value()) { // can keep layout_right_padded
                constexpr size_t S_static = MDSPAN_IMPL_STANDARD_NAMESPACE::detail::compute_s_static_layout_right<
                    Extents, deduce_layout::gap_len, static_padding_stride>(
                    std::make_index_sequence<Extents::rank()>()
                );
                using dst_mapping_t =
                    typename MDSPAN_IMPL_PROPOSED_NAMESPACE::layout_right_padded<S_static>::template mapping<dst_ext_t>;
                return submdspan_mapping_result<dst_mapping_t>{
                    dst_mapping_t(dst_ext, stride(Extents::rank() - 2 - deduce_layout::gap_len)), offset
                };
            } else { // layout_stride
                auto inv_map = MDSPAN_IMPL_STANDARD_NAMESPACE::detail::inv_map_rank(
                    std::integral_constant<size_t, 0>(), std::index_sequence<>(), slices...
                );
                using dst_mapping_t = typename layout_stride::template mapping<dst_ext_t>;
                return submdspan_mapping_result<dst_mapping_t>{
                    dst_mapping_t(
                        mdspan_non_standard, dst_ext,
                        MDSPAN_IMPL_STANDARD_NAMESPACE::detail::construct_sub_strides(
                            *this, inv_map,
// HIP needs deduction guides to have markups so we need to be explicit
// NVCC 11.0 has a bug with deduction guide here, tested that 11.2 does not have
// the issue but Clang-CUDA also doesn't accept the use of deduction guide so
// disable it for CUDA alltogether
#if defined(MDSPAN_IMPL_HAS_HIP) || defined(MDSPAN_IMPL_HAS_CUDA)
                            MDSPAN_IMPL_STANDARD_NAMESPACE::detail::tuple<
                                decltype(MDSPAN_IMPL_STANDARD_NAMESPACE::detail::stride_of(slices))...>{
                                MDSPAN_IMPL_STANDARD_NAMESPACE::detail::stride_of(slices)...
                            }
                        )
                            .values
                    ),
#else
                            MDSPAN_IMPL_STANDARD_NAMESPACE::detail::tuple{
                                MDSPAN_IMPL_STANDARD_NAMESPACE::detail::stride_of(slices)...
                            }
                        )
                            .values
                    ),
#endif
                    offset
                };
            }
        }
    }

#if defined(__NVCC__) && !defined(__CUDA_ARCH__) && defined(__GNUC__)
    __builtin_unreachable();
#endif
}

//**********************************
// layout_stride submdspan_mapping
//*********************************
template <class Extents>
template <class... SliceSpecifiers>
MDSPAN_INLINE_FUNCTION constexpr auto
layout_stride::mapping<Extents>::submdspan_mapping_impl(SliceSpecifiers... slices) const {
    auto dst_ext = submdspan_extents(extents(), slices...);
    using dst_ext_t = decltype(dst_ext);
    auto inv_map = detail::inv_map_rank(std::integral_constant<size_t, 0>(), std::index_sequence<>(), slices...);
    using dst_mapping_t = typename layout_stride::template mapping<dst_ext_t>;

    // Figure out if any slice's lower bound equals the corresponding extent.
    // If so, bypass evaluating the layout mapping.  This fixes LWG Issue 4060.
    const bool out_of_bounds = detail::any_slice_out_of_bounds(this->extents(), slices...);
    auto offset =
        static_cast<size_t>(out_of_bounds ? this->required_span_size() : this->operator()(detail::first_of(slices)...));

    return submdspan_mapping_result<dst_mapping_t>{
        dst_mapping_t(
            mdspan_non_standard, dst_ext,
            detail::construct_sub_strides(
                *this, inv_map,
// HIP needs deduction guides to have markups so we need to be explicit
// NVCC 11.0 has a bug with deduction guide here, tested that 11.2 does not have
// the issue but Clang-CUDA also doesn't accept the use of deduction guide so
// disable it for CUDA alltogether
#if defined(MDSPAN_IMPL_HAS_HIP) || defined(MDSPAN_IMPL_HAS_CUDA)
                MDSPAN_IMPL_STANDARD_NAMESPACE::detail::tuple<decltype(detail::stride_of(slices))...>(
                    detail::stride_of(slices)...
                )
            )
                .values
        ),
#else
                MDSPAN_IMPL_STANDARD_NAMESPACE::detail::tuple(detail::stride_of(slices)...)
            )
                .values
        ),
#endif
        offset
    };
}

} // namespace MDSPAN_IMPL_STANDARD_NAMESPACE

#if defined __NVCC__
#ifdef __NVCC_DIAG_PRAGMA_SUPPORT__
#pragma nv_diagnostic pop
#else
#ifdef __CUDA_ARCH__
#pragma diagnostic pop
#endif
#endif
#elif defined __NVCOMPILER
#pragma diagnostic pop
#endif
// END_FILE_INCLUDE: mdspan/include/experimental/__p2630_bits/submdspan_mapping.hpp

namespace MDSPAN_IMPL_STANDARD_NAMESPACE {
template <class ElementType, class Extents, class LayoutPolicy, class AccessorPolicy, class... SliceSpecifiers>
MDSPAN_INLINE_FUNCTION constexpr auto
submdspan(const mdspan<ElementType, Extents, LayoutPolicy, AccessorPolicy> &src, SliceSpecifiers... slices) {
    const auto sub_submdspan_mapping_result = submdspan_mapping(src.mapping(), slices...);
    // NVCC has a problem with the deduction so lets figure out the type
    using sub_mapping_t = std::remove_cv_t<decltype(sub_submdspan_mapping_result.mapping)>;
    using sub_extents_t = typename sub_mapping_t::extents_type;
    using sub_layout_t = typename sub_mapping_t::layout_type;
    using sub_accessor_t = typename AccessorPolicy::offset_policy;
    return mdspan<ElementType, sub_extents_t, sub_layout_t, sub_accessor_t>(
        src.accessor().offset(src.data_handle(), sub_submdspan_mapping_result.offset),
        sub_submdspan_mapping_result.mapping, sub_accessor_t(src.accessor())
    );
}
} // namespace MDSPAN_IMPL_STANDARD_NAMESPACE
// END_FILE_INCLUDE: mdspan/include/experimental/__p2630_bits/submdspan.hpp
#endif
// BEGIN_FILE_INCLUDE: mdspan/include/experimental/__p2389_bits/dims.hpp
//@HEADER
//  ************************************************************************
//
//                         Kokkos v. 4.0
//        Copyright (2022) National Technology & Engineering
//                Solutions of Sandia, LLC (NTESS).
//
//  Under the terms of Contract DE-NA0003525 with NTESS,
//  the U.S. Government retains certain rights in this software.
//
//  Part of Kokkos, under the Apache License v2.0 with LLVM Exceptions.
//  See https://kokkos.org/LICENSE for license information.
//  SPDX-License-Identifier: Apache-2.0 WITH LLVM-exception
//
//@HEADER

// backward compatibility import into experimental
namespace MDSPAN_IMPL_STANDARD_NAMESPACE {
namespace MDSPAN_IMPL_PROPOSED_NAMESPACE {

    template <::std::size_t Rank, class IndexType = std::size_t>
    using dims = ::MDSPAN_IMPL_STANDARD_NAMESPACE ::dextents<IndexType, Rank>;

} // namespace MDSPAN_IMPL_PROPOSED_NAMESPACE
} // namespace MDSPAN_IMPL_STANDARD_NAMESPACE
// END_FILE_INCLUDE: mdspan/include/experimental/__p2389_bits/dims.hpp

#endif // MDSPAN_HPP_
// END_FILE_INCLUDE: mdspan/include/mdspan/mdspan.hpp
#endif // MDSPAN_SINGLE_HEADER_INCLUDE_GUARD_
