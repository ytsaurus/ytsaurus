#pragma once

#include <type_traits>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

template <
    class TBase,
    class TDerived
>
struct TIsBaseOf
{
    enum {
        Value = std::is_base_of<TBase, TDerived>::value && !std::is_same<TBase, TDerived>::value
    };
};

////////////////////////////////////////////////////////////////////////////////

}
