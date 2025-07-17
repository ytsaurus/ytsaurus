#pragma once

#include <yt/cpp/roren/interface/transforms.h>

namespace NRoren {

////////////////////////////////////////////////////////////////////////////////

template <typename T>
auto Sum()
{
    auto f = WrapToSerializableFunctor(
        [] (T* accum, const T& current) {
            *accum += current;
        });
    return NYT::New<TFunctorCombineFn<std::decay_t<decltype(f)>, T>>(f);
}

class TSumPerKeyApplicator
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

TSumPerKeyApplicator SumPerKey();

////////////////////////////////////////////////////////////////////////////////

} // namespace NRoren
