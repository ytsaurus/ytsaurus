#pragma once

#include "public.h"

#include <ytlib/misc/nullable.h>
#include <ytlib/misc/property.h>

#include <ytlib/yson/public.h>

namespace NYT {
namespace NYTree {

////////////////////////////////////////////////////////////////////////////////

class TYsonString
{
public:
    TYsonString();

    explicit TYsonString(
        const Stroka& data,
        NYson::EYsonType type = NYson::EYsonType::Node);

    DEFINE_BYREF_RO_PROPERTY(Stroka, Data);
    DEFINE_BYVAL_RO_PROPERTY(NYson::EYsonType, Type);

    void Validate() const;

    void Save(TStreamSaveContext& context) const;
    void Load(TStreamLoadContext& context);

};

////////////////////////////////////////////////////////////////////////////////

void Serialize(const TYsonString& yson, NYson::IYsonConsumer* consumer);

bool operator == (const TYsonString& lhs, const TYsonString& rhs);
bool operator != (const TYsonString& lhs, const TYsonString& rhs);

////////////////////////////////////////////////////////////////////////////////

//! Direct conversion from Stroka to Node or Producer is forbidden.
//! For this case, use TRawString wrapper.
class TRawString
    : public Stroka
{
public:
    TRawString(const Stroka& str);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT
