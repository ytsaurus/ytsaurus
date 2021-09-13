#pragma once

#include <yt/yt/core/ytree/serialize.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

template <typename U, int P>
void Serialize(const TFixedPointNumber<U, P>& number, NYson::IYsonConsumer* consumer)
{
    NYTree::Serialize(static_cast<double>(number), consumer);
}

template <typename U, int P>
void Deserialize(TFixedPointNumber<U, P>& number, NYTree::INodePtr node)
{
    double doubleValue;
    Deserialize(doubleValue, std::move(node));
    number = TFixedPointNumber<U, P>(doubleValue);
}

template <typename U, int P>
TString ToString(const TFixedPointNumber<U, P>& number)
{
    return ToString(static_cast<double>(number));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace std
