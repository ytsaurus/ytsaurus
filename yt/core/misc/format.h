#pragma once

#include "public.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

/*
 *  Format: a type-safe and fast formatting utility.
 *
 *  Basically works as a type-safe analogue of |sprintf| and is expected to
 *  be backwards-compatible with the latter.
 *  
 *  Like Go's |Sprintf|, supports the ultimate format specifier |v|
 *  causing arguments to be emitted in default format.
 *  This is the default and preferred way of formatting things,
 *  which should be used in newer code.
 *  
 *  |Format| may currently invoke |sprintf| internally for emitting numeric and some other
 *  types. You can always write your own optimized implementation, if you wish :)
 *  
 *  In additional to the usual |sprintf|, supports a number of non-standard flags:
 *  
 *  |q|   Causes the argument to be surrounded with single quotes (|'|).
 *        Applies to all types.
 *       
 *  |Q|   Causes the argument to be surrounded with double quotes (|"|).
 *        Applies to all types.
 *       
 *  |l|   The argument is emitted in "lowercase" style.
 *        Only applies to enums and bools.
 *  
 *  The following argument types are supported:
 *  
 *  Strings (including |const char*|, |TStringBuf|, and |TString|) and chars:
 *  Emitted as is. Fast.
 *  
 *  Numerics and pointers:
 *  Emitted using |sprintf|. Maybe not that fast.
 *  
 *  |bool|:
 *  Emitted either as |True| and |False| or |true| and |false| (if lowercase mode is ON).
 *  
 *  Enums:
 *  Emitted in either camel (|SomeName|) or in lowercase-with-underscores style
 *  (|some_name|, if lowercase mode is ON).
 *  
 *  Nullables:
 *  |std::nullopt| is emitted as |<null>|.
 *  
 *  All others:
 *  Emitted as strings by calling |ToString|.
 *
 */

extern const TStringBuf DefaultRangeEllipsisFormat;

template <class... TArgs, size_t FormatLength>
void Format(TStringBuilder* builder, const char (&format)[FormatLength], TArgs&&... args);
template <class... TArgs>
void Format(TStringBuilder* builder, TStringBuf format, TArgs&&... args);

template <class... TArgs, size_t FormatLength>
TString Format(const char (&format)[FormatLength], TArgs&&... args);
template <class... TArgs>
TString Format(TStringBuf format, TArgs&&... args);

////////////////////////////////////////////////////////////////////////////////

template <class TRange, class TFormatter>
struct TFormattableRange
{
    TRange Range;
    TFormatter Formatter;
    size_t Limit = std::numeric_limits<size_t>::max();
};

//! Annotates a given #range with #formatter to be applied to each item.
template <class TRange, class TFormatter>
TFormattableRange<TRange, TFormatter> MakeFormattableRange(
    const TRange& range,
    const TFormatter& formatter);

////////////////////////////////////////////////////////////////////////////////

template <class TRange, class TFormatter>
TFormattableRange<TRange, TFormatter> MakeShrunkFormattableRange(
    const TRange& range,
    const TFormatter& formatter,
    size_t limit);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

#define FORMAT_INL_H_
#include "format-inl.h"
#undef FORMAT_INL_H_
