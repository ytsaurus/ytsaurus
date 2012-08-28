#pragma once

#include "public.h"
#include "yson_string.h"
#include "yson_writer.h"
#include "ephemeral.h"

namespace NYT {
namespace NYTree {

////////////////////////////////////////////////////////////////////////////////

//! Direct conversion from Stroka to Node or Producer is forbidden.
//! For this case, use TRawString wrapper.
class TRawString
    : public Stroka
{
public:
    TRawString(const Stroka& str)
        : Stroka(str)
    { }
};

////////////////////////////////////////////////////////////////////////////////

template <class T>
void Consume(const T& value, IYsonConsumer* consumer);

////////////////////////////////////////////////////////////////////////////////

template <class T>
TYsonProducer ConvertToProducer(T&& value);

template <class T>
TYsonString ConvertToYsonString(
    const T& value,
    EYsonFormat format = EYsonFormat::Binary);

template <class T>
INodePtr ConvertToNode(
    const T& value,
    INodeFactoryPtr factory = GetEphemeralNodeFactory());

////////////////////////////////////////////////////////////////////////////////

template <class TTo>
TTo ConvertTo(INodePtr node);

template <class TTo, class T>
TTo ConvertTo(const T& value);

////////////////////////////////////////////////////////////////////////////////

Stroka YsonizeString(const Stroka& string, EYsonFormat format);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT

#define CONVERT_INL_H_
#include "convert-inl.h"
#undef CONVERT_INL_H_
