#include "unwrapping_consumer.h"

#include <yt/yt/core/misc/parser_helpers.h>

#include <yt/yt/core/yson/parser.h>

#include <library/cpp/yt/yson_string/format.h>

namespace NYT::NOrm::NAttributes {

////////////////////////////////////////////////////////////////////////////////

TUnwrappingConsumer::TUnwrappingConsumer(IYsonConsumer* underlying)
    : Underlying_(underlying)
{ }

void TUnwrappingConsumer::OnMyBeginMap()
{
    Forward(Underlying_, nullptr, NYson::EYsonType::MapFragment);
}

void TUnwrappingConsumer::OnMyEndMap()
{ }

void TUnwrappingConsumer::OnMyEntity()
{ }

void TUnwrappingConsumer::OnMyRaw(TStringBuf buffer, NYson::EYsonType type)
{
    if (type == NYson::EYsonType::Node) {
        auto [nodeType, leftStrippedYson] = NYson::ParseYsonStringNodeType(buffer);
        if (nodeType == NYTree::ENodeType::Map) {
            auto strippedYson = StripStringRight(leftStrippedYson, &IsSpacePtr);
            THROW_ERROR_EXCEPTION_UNLESS(
                strippedYson.front() == NYson::NDetail::BeginMapSymbol &&
                strippedYson.back() == NYson::NDetail::EndMapSymbol,
                "Cannot unwrap invalid yson map %Qv",
                strippedYson);
            Underlying_->OnRaw(strippedYson.SubStr(1, strippedYson.size() - 2), NYson::EYsonType::MapFragment);
            return;
        }
    }

    TYsonConsumerBase::OnRaw(buffer, type);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NAttributes
