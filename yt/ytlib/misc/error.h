#pragma once

#include "common.h"
#include "property.h"

#include <ytlib/misc/error.pb.h>

#include <ytlib/actions/future.h>

#include <ytlib/ytree/public.h>
#include <ytlib/ytree/yson_string.h>
#include <ytlib/ytree/attributes.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

#undef GetMessage // Fucking WinAPI

class TError
{
public:
    TError();
    TError(const TError& other);
    TError(TError&& other);

    TError(const std::exception& ex);

    explicit TError(const Stroka& message);
    TError(const char* format, ...);

    TError(int code, const Stroka& message);
    TError(int code, const char* format, ...);

    static TError FromSystem();
    static TError FromSystem(int error);

    TError& operator = (const TError& other);
    TError& operator = (TError&& other);

    int GetCode() const;
    TError& SetCode(int code);

    const Stroka& GetMessage() const;
    TError& SetMessage(const Stroka& message);

    const NYTree::IAttributeDictionary& Attributes() const;
    NYTree::IAttributeDictionary& Attributes();

    const std::vector<TError>& InnerErrors() const;
    std::vector<TError>& InnerErrors();

    bool IsOK() const;

    enum
    {
        OK = 0, 
        Fail = INT_MAX
    };

private:
    int Code_;
    Stroka Message_;
    TAutoPtr<NYTree::IAttributeDictionary> Attributes_;
    std::vector<TError> InnerErrors_;

    void CaptureOriginAttributes();

};

Stroka ToString(const TError& error);

void ToProto(NProto::TError* protoError, const TError& error);
TError FromProto(const NProto::TError& protoError);

void Serialize(const TError& error, NYTree::IYsonConsumer* consumer);
void Deserialize(TError& error, NYTree::INodePtr node);

////////////////////////////////////////////////////////////////////////////////

namespace NYTree {

// Avoid dependency on convert.h

class TRawString;

template <class T>
TYsonString ConvertToYsonString(
    const T& value,
    NYTree::EYsonFormat format = NYTree::EYsonFormat::Binary);

} // namespace NYTree

struct TErrorAttribute
{
    template <class T>
    TErrorAttribute(const Stroka& key, const T& value)
        : Key(key)
        , Value(NYTree::ConvertToYsonString(value, NYTree::EYsonFormat::Binary))
    { }

    TErrorAttribute(const Stroka& key, const NYTree::TYsonString& value)
        : Key(key)
        , Value(value)
    { }

    Stroka Key;
    NYTree::TYsonString Value;
};

TError operator << (TError error, const TErrorAttribute& attribute);
TError operator << (TError error, const TError& innerError);

TError operator >>= (const TErrorAttribute& attribute, TError error);

////////////////////////////////////////////////////////////////////////////////

class TErrorException
    : public std::exception
{
    DEFINE_BYREF_RW_PROPERTY(TError, Error);

public:
    TErrorException();
    TErrorException(const TErrorException& other);

    virtual const char* what() const override throw();

private:
    mutable Stroka CachedWhat;

};

TErrorException operator <<= (TErrorException ex, const TError& error);

////////////////////////////////////////////////////////////////////////////////

#define ERROR_SOURCE_LOCATION() \
    ::NYT::TErrorAttribute( \
        "file", \
         ::NYT::NYTree::ConvertToYsonString( \
             ::NYT::NYTree::TRawString(__FILE__), \
             ::NYT::NYTree::EYsonFormat::Binary) ) \
    >>= \
    ::NYT::TErrorAttribute( \
        "line", \
        ::NYT::NYTree::ConvertToYsonString( \
            __LINE__, \
            ::NYT::NYTree::EYsonFormat::Binary))

#define THROW_ERROR \
    throw \
        ::NYT::TErrorException() <<= \
        ERROR_SOURCE_LOCATION() >>= \

#define THROW_ERROR_EXCEPTION \
    THROW_ERROR NYT::TError

////////////////////////////////////////////////////////////////////////////////

typedef TFuture<TError>  TAsyncError;
typedef TPromise<TError> TAsyncErrorPromise;

////////////////////////////////////////////////////////////////////////////////

template <class T>
class TValueOrError
    : public TError
{
    DEFINE_BYREF_RW_PROPERTY(T, Value);

public:
    TValueOrError()
    { }

    TValueOrError(const T& value)
        : Value_(value)
    { }

    TValueOrError(T&& value)
        : Value_(MoveRV(value))
    { }

    TValueOrError(const TValueOrError<T>& other)
        : TError(other)
        , Value_(other.Value_)
    { }

    TValueOrError(const TError& other)
        : TError(other)
    { }

    TValueOrError(int code, const Stroka& message)
        : TError(code, message)
    { }

    template <class TOther>
    TValueOrError(const TValueOrError<TOther>& other)
        : TError(other)
        , Value_(other.Value())
    { }

};

template <class T>
Stroka ToString(const TValueOrError<T>& valueOrError)
{
    return ToString(TError(valueOrError));
}

////////////////////////////////////////////////////////////////////////////////

template <>
class TValueOrError<void>
    : public TError
{
public:
    TValueOrError()
    { }

    TValueOrError(const TValueOrError<void>& other)
        : TError(other)
    { }

    TValueOrError(const TError& other)
        : TError(other)
    { }

    TValueOrError(int code, const Stroka& message)
        : TError(code, message)
    { }
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
