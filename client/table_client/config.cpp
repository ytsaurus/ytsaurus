#include "config.h"

#include <yt/core/ytree/convert.h>

namespace NYT::NTableClient {

////////////////////////////////////////////////////////////////////////////////

TString ToString(const TRetentionConfigPtr& obj)
{
    static const TString NullPtrName("<nullptr>");
    return obj
        ? NYTree::ConvertToYsonString(obj, NYson::EYsonFormat::Text).GetData()
        : NullPtrName;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
