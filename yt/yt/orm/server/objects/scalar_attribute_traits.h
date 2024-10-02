#pragma once

#include <yt/yt/core/misc/mpl.h>

#include <yt/yt/core/ytree/node.h>

#include <library/cpp/yt/memory/intrusive_ptr.h>

namespace NYT::NOrm::NServer::NObjects {

////////////////////////////////////////////////////////////////////////////////

template <class T>
struct TScalarAttributeTraits
{
    static T GetDefaultValue()
        requires (!NMpl::DerivedFromSpecializationOf<T, TIntrusivePtr>);
};

template <>
struct TScalarAttributeTraits<NYTree::IMapNodePtr>
{
    static NYTree::IMapNodePtr GetDefaultValue();
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NServer::NObjects

#define SCALAR_ATTRIBUTE_TRAITS_H_
#include "scalar_attribute_traits-inl.h"
#undef SCALAR_ATTRIBUTE_TRAITS_H_
