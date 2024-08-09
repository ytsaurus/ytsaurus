#include "attributes.h"

namespace NRoren::NPrivate {

////////////////////////////////////////////////////////////////////////////////

TAttributeSetter TAttributeSetter::operator >> (const TAttributeSetter& other) const
{
    TAttributeSetter result(other);
    MergeAttributes(result, *this);
    return result;
}

////////////////////////////////////////////////////////////////////////////////

void MergeAttributes(IWithAttributes& destination, const TAttributes& source)
{
    for (const auto& [key, attribute] : source.Attributes_) {
        destination.SetAttribute(key, attribute);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NRoren::NPrivate
