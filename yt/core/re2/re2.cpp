#include "re2.h"

#include <yt/core/yson/consumer.h>

#include <yt/core/ytree/fluent.h>

namespace NYT {
namespace NRe2 {

using namespace NYson;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

void Serialize(const TRe2Ptr& re, IYsonConsumer* consumer)
{
    BuildYsonFluently(consumer)
        .Value(re->pattern());
}

void Deserialize(TRe2Ptr& re, INodePtr& node)
{
    auto pattern = node->AsString()->GetValue();
    re = New<TRe2>(pattern);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NRe2
} // namespace NYT