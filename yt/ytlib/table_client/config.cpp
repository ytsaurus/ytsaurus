#include "config.h"

#include <yt/core/ytree/convert.h>

namespace NYT {
namespace NTableClient {

////////////////////////////////////////////////////////////////////////////////
 
static const TString NullPtrName("<nullptr>"); 

////////////////////////////////////////////////////////////////////////////////

TString ToString(const TRetentionConfigPtr& obj)
{
    return obj
        ? NYTree::ConvertToYsonString(obj, NYson::EYsonFormat::Text).GetData()
        : NullPtrName;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
