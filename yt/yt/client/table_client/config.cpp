#include "config.h"

#include <yt/yt/core/ytree/convert.h>

namespace NYT::NTableClient {

////////////////////////////////////////////////////////////////////////////////

TString ToString(const TRetentionConfigPtr& obj)
{
    static const TString NullPtrName("<nullptr>");
    return obj
        ? NYson::ConvertToYsonString(obj, NYson::EYsonFormat::Text).ToString()
        : NullPtrName;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
