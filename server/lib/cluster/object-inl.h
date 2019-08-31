#pragma once
#ifndef OBJECT_INL_H_
#error "Direct inclusion of this file is not allowed, include object.h"
// For the sake of sane code completion.
#include "object.h"
#endif

namespace NYP::NServer::NCluster {

////////////////////////////////////////////////////////////////////////////////

template <class T>
const T* TObject::As() const
{
    return static_cast<const T*>(this);
}

template <class T>
T* TObject::As()
{
    return static_cast<T*>(this);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NServer::NCluster
