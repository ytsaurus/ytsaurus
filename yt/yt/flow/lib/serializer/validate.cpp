#include "validate.h"

#include <yt/yt/core/ytree/yson_struct.h>

namespace NYT::NFlow::NYsonSerializer {

////////////////////////////////////////////////////////////////////////////////

void ValidateYsonStruct(const NYTree::TYsonStructPtr& ysonStruct)
{
    ysonStruct->GetMeta()->Traverse([ysonStruct](const auto& context) {
        if (context.Parameter->IsRequired()) {
            THROW_ERROR_EXCEPTION("Parameter [%v]:%v does not have default value", TypeName(ysonStruct->GetMeta()->GetStructType()), context.Path);
        }
    });
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NYsonSerializer
