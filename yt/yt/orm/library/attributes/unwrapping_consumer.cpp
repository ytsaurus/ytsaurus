#include "unwrapping_consumer.h"

#include <yt/yt/core/misc/parser_helpers.h>

#include <library/cpp/yt/yson_string/format.h>

namespace NYT::NOrm::NAttributes {
namespace {

////////////////////////////////////////////////////////////////////////////////

TStringBuf TrimSpaces(TStringBuf buf)
{
    int prefixSpaces = std::find_if_not(buf.begin(), buf.end(), &IsSpace) - buf.begin();
    int suffixSpaces = std::find_if_not(buf.rbegin(), buf.rend(), &IsSpace) - buf.rbegin();
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
        yson.Size() >= 2 &&
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
