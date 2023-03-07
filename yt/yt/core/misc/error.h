#pragma once

#include "public.h"
#include "property.h"
#include "optional.h"

#include <yt/core/yson/string.h>

#include <yt/core/ytree/attributes.h>

#include <yt/core/tracing/public.h>

#include <yt/core/concurrency/public.h>

#include <util/system/getpid.h>

#include <type_traits>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

//! An opaque wrapper around |int| value capable of conversions from |int|s and
//! arbitrary enum types.
class TErrorCode
{
public:
    using TUnderlying = int;

    TErrorCode();
    TErrorCode(int value);
    template <class E>
    TErrorCode(E value, typename std::enable_if<TEnumTraits<E>::IsEnum, bool>::type* = nullptr);

    operator int() const;

    void Save(TStreamSaveContext& context) const;
    void Load(TStreamLoadContext& context);

private:
    int Value_;

};

template <class E>
typename std::enable_if<TEnumTraits<E>::IsEnum, bool>::type operator == (E lhs, TErrorCode rhs);
template <class E>
typename std::enable_if<TEnumTraits<E>::IsEnum, bool>::type operator != (E lhs, TErrorCode rhs);

template <class E>
typename std::enable_if<TEnumTraits<E>::IsEnum, bool>::type operator == (TErrorCode lhs, E rhs);
template <class E>
typename std::enable_if<TEnumTraits<E>::IsEnum, bool>::type operator != (TErrorCode lhs, E rhs);

inline bool operator == (TErrorCode lhs, TErrorCode rhs);
inline bool operator != (TErrorCode lhs, TErrorCode rhs);

////////////////////////////////////////////////////////////////////////////////

template <>
class [[nodiscard]] TErrorOr<void>
{
public:
    TErrorOr();

    TErrorOr(const TError& other);
    TErrorOr(TError&& other) noexcept;

    TErrorOr(const std::exception& ex);

    explicit TErrorOr(TString message);
    template <class... TArgs>
    explicit TErrorOr(const char* format, const TArgs&... args);

    TErrorOr(TErrorCode code, TString message);
    template <class... TArgs>
    TErrorOr(TErrorCode code, const char* format, const TArgs&... args);

    TError& operator = (const TError& other);
    TError& operator = (TError&& other) noexcept;

    static TError FromSystem();
    static TError FromSystem(int error);

    TErrorCode GetCode() const;
    TError& SetCode(TErrorCode code);

    TErrorCode GetNonTrivialCode() const;

    const TString& GetMessage() const;
    TError& SetMessage(TString message);

    bool HasOriginAttributes() const;
    TStringBuf GetHost() const;
    TInstant GetDatetime() const;
    TProcessId GetPid() const;
    NConcurrency::TThreadId GetTid() const;
    NConcurrency::TFiberId GetFid() const;

    bool HasTracingAttributes() const;
    NTracing::TTraceId GetTraceId() const;
    NTracing::TSpanId GetSpanId() const;

    const NYTree::IAttributeDictionary& Attributes() const;
    NYTree::IAttributeDictionary& Attributes();

    const std::vector<TError>& InnerErrors() const;
    std::vector<TError>& InnerErrors();

    TError Sanitize() const;
    TError Sanitize(TInstant datetime) const;

    TError Truncate() const;

    bool IsOK() const;

    void ThrowOnError() const;

    std::optional<TError> FindMatching(TErrorCode code) const;

    template <class... TArgs>
    TError Wrap(TArgs&&... args) const;
    TError Wrap() const;

    void Save(TStreamSaveContext& context) const;
    void Load(TStreamLoadContext& context);

private:
    TErrorCode Code_;
    TString Message_;
    // Most errors are local; for these Host_ refers to a static buffer and HostHolder_ is not used.
    // This saves one allocation on TError construction.
    TStringBuf Host_;
    TString HostHolder_;
    TInstant Datetime_;
    TProcessId Pid_ = 0;
    NConcurrency::TThreadId Tid_ = NConcurrency::InvalidThreadId;
    NConcurrency::TFiberId Fid_ = NConcurrency::InvalidFiberId;
    NTracing::TTraceId TraceId_;
    NTracing::TSpanId SpanId_;
    std::unique_ptr<NYTree::IAttributeDictionary> Attributes_;
    std::vector<TError> InnerErrors_;

    void CaptureOriginAttributes();
    void ExtractOriginAttributes();

    friend void ToProto(NProto::TError* protoError, const TError& error);
    friend void FromProto(TError* error, const NProto::TError& protoError);

    friend void Serialize(
        const TError& error,
        NYson::IYsonConsumer* consumer,
        const std::function<void(NYson::IYsonConsumer*)>* valueProducer);
    friend void Deserialize(TError& error, const NYTree::INodePtr& node);

};

bool operator == (const TErrorOr<void>& lhs, const TErrorOr<void>& rhs);
bool operator != (const TErrorOr<void>& lhs, const TErrorOr<void>& rhs);

void ToProto(NProto::TError* protoError, const TError& error);
void FromProto(TError* error, const NProto::TError& protoError);

void Serialize(
    const TError& error,
    NYson::IYsonConsumer* consumer,
    const std::function<void(NYson::IYsonConsumer*)>* valueProducer = nullptr);
void Deserialize(
    TError& error,
    const NYTree::INodePtr& node);

void FormatValue(TStringBuilderBase* builder, const TError& error, TStringBuf spec);
TString ToString(const TError& error);

////////////////////////////////////////////////////////////////////////////////

template <class T>
struct TErrorTraits
{
    typedef TErrorOr<T> TWrapped;
    typedef T TUnwrapped;
};

template <class T>
struct TErrorTraits<TErrorOr<T>>
{
    typedef T TUnderlying;
    typedef TErrorOr<T> TWrapped;
    typedef T TUnwrapped;
};

////////////////////////////////////////////////////////////////////////////////

namespace NYTree {

// Avoid dependency on convert.h

template <class T>
NYson::TYsonString ConvertToYsonString(const T& value);
NYson::TYsonString ConvertToYsonString(const char* value);
NYson::TYsonString ConvertToYsonString(TStringBuf value);

} // namespace NYTree

struct TErrorAttribute
{
    template <class T>
    TErrorAttribute(const TString& key, const T& value)
        : Key(key)
        , Value(NYTree::ConvertToYsonString(value))
    { }

    TErrorAttribute(const TString& key, const NYson::TYsonString& value)
        : Key(key)
        , Value(value)
    { }

    TString Key;
    NYson::TYsonString Value;
};

TError operator << (TError error, const TErrorAttribute& attribute);
TError operator << (TError error, const std::vector<TErrorAttribute>& attributes);
TError operator << (TError error, const TError& innerError);
TError operator << (TError error, TError&& innerError);
TError operator << (TError error, const std::vector<TError>& innerErrors);
TError operator << (TError error, std::vector<TError>&& innerErrors);
TError operator << (TError error, const NYTree::IAttributeDictionary& attributes);

////////////////////////////////////////////////////////////////////////////////

class TErrorException
    : public std::exception
{
public:
    DEFINE_BYREF_RW_PROPERTY(TError, Error);

public:
    TErrorException() = default;
    TErrorException(const TErrorException& other) = default;
    TErrorException(TErrorException&& other) = default;

    virtual const char* what() const noexcept override;

private:
    mutable TString CachedWhat_;

};

// Make it template to avoid type erasure during throw.
template <class TException>
TException&& operator <<= (TException&& ex, const TError& error)
{
    ex.Error() = error;
    return std::move(ex);
}

template <class TException>
TException&& operator <<= (TException&& ex, TError&& error)
{
    ex.Error() = std::move(error);
    return std::move(ex);
}

////////////////////////////////////////////////////////////////////////////////

#define THROW_ERROR \
    throw ::NYT::TErrorException() <<=

#define THROW_ERROR_EXCEPTION(...) \
    THROW_ERROR ::NYT::TError(__VA_ARGS__)

#define THROW_ERROR_EXCEPTION_IF_FAILED(error, ...) \
    if ((error).IsOK()) {\
    } else { \
        THROW_ERROR (error).Wrap(__VA_ARGS__); \
    }\

////////////////////////////////////////////////////////////////////////////////

template <class T>
class [[nodiscard]] TErrorOr
    : public TError
{
public:
    TErrorOr();

    TErrorOr(const T& value);
    TErrorOr(T&& value) noexcept;

    TErrorOr(const TErrorOr<T>& other);
    TErrorOr(TErrorOr<T>&& other) noexcept;

    TErrorOr(const TError& other);
    TErrorOr(TError&& other) noexcept;

    TErrorOr(const std::exception& ex);

    template <class U>
    TErrorOr(const TErrorOr<U>& other);
    template <class U>
    TErrorOr(TErrorOr<U>&& other) noexcept;

    TErrorOr<T>& operator = (const TErrorOr<T>& other);
    TErrorOr<T>& operator = (TErrorOr<T>&& other) noexcept;

    const T& Value() const &;
    T& Value() &;
    T&& Value() &&;


    const T& ValueOrThrow() const &;
    T& ValueOrThrow() &;
    T&& ValueOrThrow() &&;

private:
    std::optional<T> Value_;

};

template <class T>
void FormatValue(TStringBuilderBase* builder, const TErrorOr<T>& error, TStringBuf spec);
template <class T>
TString ToString(const TErrorOr<T>& valueOrError);

////////////////////////////////////////////////////////////////////////////////

template <class F, class... As>
auto RunNoExcept(F&& functor, As&&... args) noexcept -> decltype(functor(std::forward<As>(args)...))
{
    return functor(std::forward<As>(args)...);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

#define ERROR_INL_H_
#include "error-inl.h"
#undef ERROR_INL_H_
