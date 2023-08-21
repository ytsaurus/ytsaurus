#include "attributes.h"

namespace NRoren::NPrivate {

////////////////////////////////////////////////////////////////////////////////

void MergeAttributes(IWithAttributes& destination, const TAttributes& source)
{
    for (const auto& [key, attribute] : source.Attributes_) {
        destination.SetAttribute(key, attribute);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NRoren::NPrivate
