#include "attribute_schema_traits.h"

#include "attribute_schema.h"

namespace NYT::NOrm::NServer::NObjects {

////////////////////////////////////////////////////////////////////////////////

void ValidatePathIsEmpty(const TAttributeSchema* attribute, const NYPath::TYPath& path)
{
    if (!path.empty()) {
        THROW_ERROR_EXCEPTION("Attribute %Qv is scalar and does not support nested access",
            attribute->FormatPathEtc());
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NServer::NObjects
