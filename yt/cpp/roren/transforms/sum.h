#pragma once

#include <yt/cpp/roren/interface/transforms.h>

namespace NRoren {

////////////////////////////////////////////////////////////////////////////////

template <typename T>
auto Sum()
{
    return MakeIntrusive<TLambdaCombineFn<T>>(
        [] (T* accum, const T& current) {
            *accum += current;
        });
}

class TSumPerKeyTransform
{
public:
    TString GetName() const
    {
        return "SumPerKey";
    }

    template <typename K, typename V>
    TPCollection<TKV<K, V>> ApplyTo(const TPCollection<TKV<K, V>>& pCollection) const
    {
        return pCollection | CombinePerKey(Sum<V>());
    }
};

TSumPerKeyTransform SumPerKey();

////////////////////////////////////////////////////////////////////////////////

} // namespace NRoren
