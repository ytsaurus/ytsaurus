#pragma once

#include "public.h"

#include <yt/core/misc/optional.h>
#include <yt/core/misc/property.h>

namespace NYT::NYson {

////////////////////////////////////////////////////////////////////////////////

//! Contains a sequence of bytes in YSON encoding annotated with EYsonType describing
//! the content. Could be null.
//!
//! |TData| may be either |TString| or |TStringBuf|;
//! it defines the ownership status of |TYsonStringBase|.
template <typename TData>
class TYsonStringBase
{
public:
    //! Constructs a null instance.
    TYsonStringBase();

    //! Constructs an non-null instance with given type and content.
    explicit TYsonStringBase(
        TData data,
        EYsonType type = EYsonType::Node);

    TYsonStringBase(
        const char* data,
        size_t length,
        EYsonType type = EYsonType::Node);

    //! Returns |true| if the instance is not null.
    explicit operator bool() const;

    //! Returns the underlying YSON bytes. The instance must be non-null.
    const TData& GetData() const;

    //! Returns type of YSON contained here. The instance must be non-null.
    EYsonType GetType() const;

    //! If the instance is not null, invokes the parser (which may throw).
    void Validate() const;

protected:
    bool Null_;
    TData Data_;
    EYsonType Type_;
};

class TYsonString
    : public TYsonStringBase<TString>
{
public:
    using TYsonStringBase::TYsonStringBase;

    void Save(TStreamSaveContext& context) const;
    void Load(TStreamLoadContext& context);
};

class TYsonStringBuf
    : public TYsonStringBase<TStringBuf>
{
public:
    using TYsonStringBase::TYsonStringBase;

    TYsonStringBuf(const TYsonString& ysonString)
        : TYsonStringBase(ysonString.GetData(), ysonString.GetType())
    { }
};

////////////////////////////////////////////////////////////////////////////////

void Serialize(const TYsonString& yson, IYsonConsumer* consumer);
void Serialize(const TYsonStringBuf& yson, IYsonConsumer* consumer);

bool operator == (const TYsonString& lhs, const TYsonString& rhs);
bool operator == (const TYsonString& lhs, const TYsonStringBuf& rhs);
bool operator == (const TYsonStringBuf& lhs, const TYsonString& rhs);
bool operator == (const TYsonStringBuf& lhs, const TYsonStringBuf& rhs);

bool operator != (const TYsonString& lhs, const TYsonString& rhs);
bool operator != (const TYsonString& lhs, const TYsonStringBuf& rhs);
bool operator != (const TYsonStringBuf& lhs, const TYsonString& rhs);
bool operator != (const TYsonStringBuf& lhs, const TYsonStringBuf& rhs);

TString ToString(const TYsonString& yson);
TString ToString(const TYsonStringBuf& yson);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYson

//! A hasher for TYsonString
template <>
struct THash<NYT::NYson::TYsonString>
{
    size_t operator () (const NYT::NYson::TYsonString& str) const
    {
        return THash<TString>()(str.GetData());
    }
};

//! A hasher for TYsonStringBuf
template <>
struct THash<NYT::NYson::TYsonStringBuf>
{
    size_t operator () (const NYT::NYson::TYsonStringBuf& str) const
    {
        return THash<TStringBuf>()(str.GetData());
    }
};

////////////////////////////////////////////////////////////////////////////////
