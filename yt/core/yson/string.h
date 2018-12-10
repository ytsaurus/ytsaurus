#pragma once

#include "public.h"

#include <yt/core/misc/optional.h>
#include <yt/core/misc/property.h>

namespace NYT::NYson {

////////////////////////////////////////////////////////////////////////////////

//! Contains a sequence of bytes in YSON encoding annotated with EYsonType describing
//! the content. Could be null.
class TYsonString
{
public:
    //! Constructs a null instance.
    TYsonString();

    //! Constructs an non-null instance with given type and content.
    explicit TYsonString(
        TString data,
        EYsonType type = EYsonType::Node);

    TYsonString(
        const char* data,
        size_t length,
        EYsonType type = EYsonType::Node);

    //! Returns |true| if the instance is not null.
    explicit operator bool() const;

    //! Returns the underlying YSON bytes. The instance must be non-null.
    const TString& GetData() const;

    //! Returns type of YSON contained here. The instance must be non-null.
    EYsonType GetType() const;

    //! If the instance is not null, invokes the parser (which may throw).
    void Validate() const;

    void Save(TStreamSaveContext& context) const;
    void Load(TStreamLoadContext& context);

private:
    bool Null_;
    TString Data_;
    EYsonType Type_;

};

////////////////////////////////////////////////////////////////////////////////

void Serialize(const TYsonString& yson, IYsonConsumer* consumer);

bool operator == (const TYsonString& lhs, const TYsonString& rhs);
bool operator != (const TYsonString& lhs, const TYsonString& rhs);

TString ToString(const TYsonString& yson);

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

////////////////////////////////////////////////////////////////////////////////
