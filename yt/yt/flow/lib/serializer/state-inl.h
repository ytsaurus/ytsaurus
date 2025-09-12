#ifndef STATE_INL_H_
#error "Direct inclusion of this file is not allowed, include state.h"
// For the sake of sane code completion.
#include "state.h"
#endif
#undef STATE_INL_H_

#include <yt/yt/core/ytree/convert.h>

namespace NYT::NFlow::NYsonSerializer {

////////////////////////////////////////////////////////////////////////////////

template <class T>
TStateSchemaPtr GetYsonStateSchema()
{
    auto ctor = []() {
        return New<T>();
    };

    return NPrivate::BuildYsonStateSchema(ctor);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NYsonSerializer
