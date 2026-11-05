#pragma once

#include <yt/yt/core/ytree/public.h>

namespace NYT::NFlow::NYsonSerializer {

////////////////////////////////////////////////////////////////////////////////

void ValidateYsonStruct(const NYTree::TYsonStructPtr& ysonStruct);

template <class T>
void ValidateYsonStruct()
{
    ValidateYsonStruct(New<T>());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NYsonSerializer
