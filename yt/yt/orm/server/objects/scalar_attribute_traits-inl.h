#ifndef SCALAR_ATTRIBUTE_TRAITS_H_
#error "Direct inclusion of this file is not allowed, include scalar_attribute_traits.h"
// For the sake of sane code completion.
#include "scalar_attribute_traits.h"
#endif

namespace NYT::NOrm::NServer::NObjects {

////////////////////////////////////////////////////////////////////////////////

template <class T>
T TScalarAttributeTraits<T>::GetDefaultValue()
    requires (!NMpl::DerivedFromSpecializationOf<T, TIntrusivePtr>)
{
    return T{};
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NSever::NObjects
