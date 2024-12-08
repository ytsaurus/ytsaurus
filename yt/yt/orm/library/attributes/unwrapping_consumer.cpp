#include "unwrapping_consumer.h"

#include <yt/yt/core/misc/parser_helpers.h>

#include <library/cpp/yt/yson_string/format.h>

#include <ranges>

namespace NYT::NOrm::NAttributes {
namespace {

////////////////////////////////////////////////////////////////////////////////

TStringBuf TrimSpaces(TStringBuf buf)
{
    int prefixSpaces = std::ranges::find_if_not(buf, IsSpace) - buf.begin();
    int suffixSpaces = std::ranges::find_if_not(buf | std::views::reverse, IsSpace) - buf.rbegin();
    if (prefixSpaces + suffixSpaces >= std::ssize(buf)) {
        return TStringBuf{};
    } else {
        return buf.SubString(prefixSpaces, buf.size() - prefixSpaces - suffixSpaces);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace

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

void TUnwrappingConsumer::OnMyRaw(TStringBuf yson, NYson::EYsonType type)
{
    yson = TrimSpaces(yson);
    if (type == NYson::EYsonType::Node &&
        yson.size() >= 2 &&
        yson.front() == NYson::NDetail::BeginMapSymbol &&
        yson.back() == NYson::NDetail::EndMapSymbol)
    {
        Underlying_->OnRaw(yson.SubStr(1, yson.size() - 2), NYson::EYsonType::MapFragment);
        return;
    }

    TYsonConsumerBase::OnRaw(yson, type);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NAttributes
