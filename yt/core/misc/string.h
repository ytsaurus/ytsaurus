#pragma once

#include "public.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

// Forward declarations, avoid including format.h directly here.
template <class... TArgs, size_t FormatLength>
void Format(TStringBuilderBase* builder, const char (&format)[FormatLength], TArgs&&... args);
template <class... TArgs>
void Format(TStringBuilderBase* builder, TStringBuf format, TArgs&&... args);

////////////////////////////////////////////////////////////////////////////////

//! A simple helper for constructing strings by a sequence of appends.
class TStringBuilderBase
{
public:
    char* Preallocate(size_t size);

    size_t GetLength() const;

    TStringBuf GetBuffer() const;

    void Advance(size_t size);

    void AppendChar(char ch);
    void AppendChar(char ch, int n);

    void AppendString(TStringBuf str);
    void AppendString(const char* str);

    template <class... TArgs, size_t FormatLength>
    void AppendFormat(const char (&format)[FormatLength], TArgs&&... args);
    template <class... TArgs>
    void AppendFormat(TStringBuf format, TArgs&&... args);

    void Reset();

protected:
    char* Begin_ = nullptr;
    char* Current_ = nullptr;
    char* End_ = nullptr;

    virtual void DoReset() = 0;
    virtual void DoPreallocate(size_t newLength) = 0;

    static constexpr size_t MinBufferLength = 1024;
};

////////////////////////////////////////////////////////////////////////////////

class TStringBuilder
    : public TStringBuilderBase
{
public:
    TString Flush();

protected:
    TString Buffer_;

    virtual void DoReset() override;
    virtual void DoPreallocate(size_t size) override;
};

////////////////////////////////////////////////////////////////////////////////

template <class T>
TString ToStringViaBuilder(const T& value, TStringBuf spec = AsStringBuf("v"));

////////////////////////////////////////////////////////////////////////////////

//! Appends a certain delimiter starting from the second call.
class TDelimitedStringBuilderWrapper
    : private TNonCopyable
{
public:
    TDelimitedStringBuilderWrapper(
        TStringBuilderBase* builder,
        TStringBuf delimiter = AsStringBuf(", "))
        : Builder_(builder)
        , Delimiter_(delimiter)
    { }

    TStringBuilderBase* operator->()
    {
        if (!FirstCall_) {
            Builder_->AppendString(Delimiter_);
        }
        FirstCall_ = false;
        return Builder_;
    }

private:
    TStringBuilderBase* const Builder_;
    const TStringBuf Delimiter_;

    bool FirstCall_ = true;
};

////////////////////////////////////////////////////////////////////////////////

//! Formatters enable customizable way to turn an object into a string.
//! This default implementation uses |FormatValue|.
struct TDefaultFormatter
{
    template <class T>
    void operator()(TStringBuilderBase* builder, const T& obj) const
    {
        FormatValue(builder, obj, AsStringBuf("v"));
    }
};

extern const TStringBuf DefaultJoinToStringDelimiter;
extern const TStringBuf DefaultKeyValueDelimiter;

//! Joins a range of items into a string intermixing them with the delimiter.
/*!
 *  \param builder String builder where the output goes.
 *  \param begin Iterator pointing to the first item (inclusive).
 *  \param end Iterator pointing to the last item (not inclusive).
 *  \param formatter Formatter to apply to the items.
 *  \param delimiter A delimiter to be inserted between items: ", " by default.
 *  \return The resulting combined string.
 */
template <class TIterator, class TFormatter>
void JoinToString(
    TStringBuilderBase* builder,
    const TIterator& begin,
    const TIterator& end,
    const TFormatter& formatter,
    TStringBuf delimiter = DefaultJoinToStringDelimiter)
{
    for (auto current = begin; current != end; ++current) {
        if (current != begin) {
            builder->AppendString(delimiter);
        }
        formatter(builder, *current);
    }
}

template <class TIterator, class TFormatter>
TString JoinToString(
    const TIterator& begin,
    const TIterator& end,
    const TFormatter& formatter,
    TStringBuf delimiter = DefaultJoinToStringDelimiter)
{
    TStringBuilder builder;
    JoinToString(&builder, begin, end, formatter, delimiter);
    return builder.Flush();
}

//! A handy shortcut with default formatter.
template <class TIterator>
TString JoinToString(
    const TIterator& begin,
    const TIterator& end,
    TStringBuf delimiter = DefaultJoinToStringDelimiter)
{
    return JoinToString(begin, end, TDefaultFormatter(), delimiter);
}

//! Joins a collection of given items into a string intermixing them with the delimiter.
/*!
 *  \param collection A collection containing the items to be joined.
 *  \param formatter Formatter to apply to the items.
 *  \param delimiter A delimiter to be inserted between items; ", " by default.
 */
template <class TCollection, class TFormatter>
TString JoinToString(
    const TCollection& collection,
    const TFormatter& formatter,
    TStringBuf delimiter = DefaultJoinToStringDelimiter)
{
    using std::begin;
    using std::end;
    return JoinToString(begin(collection), end(collection), formatter, delimiter);
}

//! A handy shortcut with the default formatter.
template <class TCollection>
TString JoinToString(
    const TCollection& collection,
    TStringBuf delimiter = DefaultJoinToStringDelimiter)
{
    return JoinToString(collection, TDefaultFormatter(), delimiter);
}

//! Converts a range of items into strings.
template <class TIter, class TFormatter>
std::vector<TString> ConvertToStrings(
    const TIter& begin,
    const TIter& end,
    const TFormatter& formatter,
    size_t maxSize = std::numeric_limits<size_t>::max())
{
    std::vector<TString> result;
    for (auto it = begin; it != end; ++it) {
        TStringBuilder builder;
        formatter(&builder, *it);
        result.push_back(builder.Flush());
        if (result.size() == maxSize) {
            break;
        }
    }
    return result;
}

//! A handy shortcut with the default formatter.
template <class TIter>
std::vector<TString> ConvertToStrings(
    const TIter& begin,
    const TIter& end,
    size_t maxSize = std::numeric_limits<size_t>::max())
{
    return ConvertToStrings(begin, end, TDefaultFormatter(), maxSize);
}

//! Converts a given collection of items into strings.
/*!
 *  \param collection A collection containing the items to be converted.
 *  \param formatter Formatter to apply to the items.
 *  \param maxSize Size limit for the resulting vector.
 */
template <class TCollection, class TFormatter>
std::vector<TString> ConvertToStrings(
    const TCollection& collection,
    const TFormatter& formatter,
    size_t maxSize = std::numeric_limits<size_t>::max())
{
    using std::begin;
    using std::end;
    return ConvertToStrings(begin(collection), end(collection), formatter, maxSize);
}

//! A handy shortcut with default formatter.
template <class TCollection>
std::vector<TString> ConvertToStrings(
    const TCollection& collection,
    size_t maxSize = std::numeric_limits<size_t>::max())
{
    return ConvertToStrings(collection, TDefaultFormatter(), maxSize);
}

////////////////////////////////////////////////////////////////////////////////

void UnderscoreCaseToCamelCase(TStringBuilderBase* builder, TStringBuf str);
TString UnderscoreCaseToCamelCase(TStringBuf str);

void CamelCaseToUnderscoreCase(TStringBuilderBase* builder, TStringBuf str);
TString CamelCaseToUnderscoreCase(TStringBuf str);

TString TrimLeadingWhitespaces(const TString& str);
TString Trim(const TString& str, const TString& whitespaces);

bool ParseBool(const TString& value);
TStringBuf FormatBool(bool value);

////////////////////////////////////////////////////////////////////////////////

//! Implemented for |[u]i(32|64)|.
template <class T>
char* WriteIntToBufferBackwards(char* ptr, T value);

char* WriteGuidToBuffer(char* ptr, TGuid value);

////////////////////////////////////////////////////////////////////////////////

TString DecodeEnumValue(TStringBuf value);
TString EncodeEnumValue(TStringBuf value);

template <class T>
T ParseEnum(TStringBuf value, typename TEnumTraits<T>::TType* = 0)
{
    return TEnumTraits<T>::FromString(DecodeEnumValue(value));
}

template <class T>
TString FormatEnum(T value, typename TEnumTraits<T>::TType* = 0)
{
    return EncodeEnumValue(ToString(value));
}

////////////////////////////////////////////////////////////////////////////////

struct TCaseInsensitiveStringHasher
{
    size_t operator()(TStringBuf arg) const;
};

struct TCaseInsensitiveStringEqualityComparer
{
    bool operator()(TStringBuf lhs, TStringBuf rhs) const;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

#define STRING_INL_H_
#include "string-inl.h"
#undef STRING_INL_H_
