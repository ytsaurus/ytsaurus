#include "re2.h"

#include <yt/yt/core/yson/consumer.h>

#include <yt/yt/core/ytree/fluent.h>

namespace NYT::NRe2 {

using namespace NYson;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

void Serialize(const TRe2Ptr& re, IYsonConsumer* consumer)
{
    if (re) {
        BuildYsonFluently(consumer)
            .Value(re->pattern());
    } else {
        consumer->OnEntity();
    }
}

void Deserialize(TRe2Ptr& re, INodePtr node)
{
    if (node->GetType() != ENodeType::Entity) {
        auto pattern = node->GetValue<TString>();
        re = New<TRe2>(pattern);
        if (!re->ok()) {
            THROW_ERROR_EXCEPTION("Error parsing RE2 regex")
                << TErrorAttribute("error", re->error());
        }
    } else {
        re.Reset();
    }
}

void Deserialize(TRe2Ptr& re, TYsonPullParserCursor* cursor)
{
    MaybeSkipAttributes(cursor);
    if ((*cursor)->GetType() == EYsonItemType::EntityValue) {
        re.Reset();
        return;
    }

    EnsureYsonToken("TRe2", *cursor, EYsonItemType::StringValue);
    re = New<TRe2>((*cursor)->UncheckedAsString());
    cursor->Next();
    if (!re->ok()) {
        THROW_ERROR_EXCEPTION("Error parsing RE2 regex")
            << TErrorAttribute("error", re->error());
    }
}

void FormatValue(TStringBuilderBase* builder, const TRe2Ptr& object, TStringBuf spec)
{
    if (object) {
        FormatValue(builder, object->pattern(), spec);
    } else {
        FormatValue(builder, std::nullopt, spec);
    }
}

void ToProto(TProtoStringType* protoRe, const TRe2Ptr& re)
{
    if (re) {
        *protoRe = re->pattern();
    } else {
        protoRe->clear();
    }
}

void FromProto(TRe2Ptr* re, const TProtoStringType& protoRe)
{
    if (!protoRe.empty()) {
        *re = New<TRe2>(protoRe);
        if (!(*re)->ok()) {
            THROW_ERROR_EXCEPTION("Error parsing RE2 regex")
                << TErrorAttribute("error", (*re)->error());
        }
    } else {
        re->Reset();
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRe2
