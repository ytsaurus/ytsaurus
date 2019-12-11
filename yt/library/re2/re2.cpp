#include "re2.h"

#include <yt/core/yson/consumer.h>

#include <yt/core/ytree/fluent.h>

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

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRe2
