#pragma once

/*!
 * \file preprocesor.h
 * \brief YT Preprocessor metaprogramming macroses
 */

#if !defined(_MSC_VER) && !defined(__GNUC__)
#   error Your compiler is not currently supported.
#endif

/*!
 * \defgroup yt_preprocessor Preprocessor metaprogramming macroses
 * \ingroup YT
 * This is collection of macro definitions for various metaprogramming
 * tasks with the preprocessor.
 * \todo Write documentation on sequence data type and possible applications.
 * \{
 */

/*! \page concepts Concepts
 * Everything revolves around the concept of a <em>sequence</em>. Typical
 * sequence is encoded like this, <tt>(1)(2)(3)(4)...</tt>.
 */

//! Concatenates two tokens.
#define PP_CONCAT(x, y)   PP_CONCAT_A(x, y)
//! \cond Implementation
#define PP_CONCAT_A(x, y) PP_CONCAT_B(x, y)
#define PP_CONCAT_B(x, y) x ## y
//! \endcond

//! Transforms token into the string forcing argument expansion.
#define PP_STRINGIZE(x)   PP_STRINGIZE_A(x)
//! \cond Implementation
#define PP_STRINGIZE_A(x) PP_STRINGIZE_B(x)
#define PP_STRINGIZE_B(x) #x
//! \endcond

//! \cond Implementation
#define PP_LEFT_PARENTHESIS (
#define PP_RIGHT_PARENTHESIS )
#define PP_COMMA() ,
#define PP_EMPTY()
//! \endcond

//! Performs (non-lazy) conditional expansion.
/*!
 * \param cond Condition; should expands to either PP_TRUE or PP_FALSE.
 * \param _then Expansion result in case when #cond holds.
 * \param _else Expansion result in case when #cond does not hold.
 */
#define PP_IF(cond, _then, _else) PP_CONCAT(PP_IF_, cond)(_then, _else)
//! \cond Implementation
#define PP_IF_PP_TRUE(x, y) x
#define PP_IF_PP_FALSE(x, y) y
//! \endcond

//! Tests whether supplied argument can be treated as a sequence (i. e. ()()()...)
#define PP_IS_SEQUENCE(arg) PP_CONCAT(PP_IS_SEQUENCE_B_, PP_COUNT((PP_NIL PP_IS_SEQUENCE_A arg PP_NIL)))
//! \cond Implementation
#define PP_IS_SEQUENCE_A(_) PP_RIGHT_PARENTHESIS PP_LEFT_PARENTHESIS
#define PP_IS_SEQUENCE_B_1 PP_FALSE
#define PP_IS_SEQUENCE_B_2 PP_TRUE
//! \endcond

//! Computes the number of elements in the sequence.
#define PP_COUNT(...) PP_COUNT_IMPL(__VA_ARGS__)

//! Removes first #n elements from the sequence.
#define PP_KILL(seq, n) PP_KILL_IMPL(seq, n)

//! Extracts the head of the sequence.
/*! For example, \code PP_HEAD((0)(1)(2)(3)) == 0 \endcode
 */
#define PP_HEAD(...) PP_HEAD_IMPL(__VA_ARGS__)

//! Extracts the tail of the sequence.
/*! For example, \code PP_TAIL((0)(1)(2)(3)) == (1)(2)(3) \endcode
 */
#define PP_TAIL(...) PP_TAIL_IMPL(__VA_ARGS__)

//! Extracts the element with the specified index from the sequence.
#define PP_ELEMENT(seq, index) PP_ELEMENT_IMPL(seq, index)

//! Applies the macro to every member of the sequence.
#define PP_FOR_EACH(what, seq) PP_FOR_EACH_IMPL(what, seq)

//! \cond Implementation
#define PREPROCESSOR_GEN_H_
#include "preprocessor-gen.h"
#undef PREPROCESSOR_GEN_H_
//! \endcond

//! \}

