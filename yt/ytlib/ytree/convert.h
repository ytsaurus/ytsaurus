#pragma once

#include "public.h"
#include "yson_string.h"
#include "yson_writer.h"
#include "ephemeral.h"

namespace NYT {
namespace NYTree {

////////////////////////////////////////////////////////////////////////////////

//! Direct convertion from Stroka to Node or Producer is forbidden.
//! Use RawString wrapper in this case.
class RawString: public Stroka {
public:
    RawString(const Stroka& str):
        Stroka(str)
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
    INodeFactory* factory = GetEphemeralNodeFactory());

////////////////////////////////////////////////////////////////////////////////

template <class TTo>
TTo ConvertTo(INodePtr node);

template <class TTo, class T>
TTo ConvertTo(const T& value);

////////////////////////////////////////////////////////////////////////////////

inline Stroka YsonizeString(const Stroka& string, EYsonFormat format);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT

#define CONVERT_INL_H_
#include "convert-inl.h"
#undef CONVERT_INL_H_
