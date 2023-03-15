#pragma once

#include <yt/cpp/roren/interface/transforms.h>

namespace NRoren {

////////////////////////////////////////////////////////////////////////////////

template <typename T>
class TSumCombineFn
    : public ICombineFn<T, T, T>
{
public:
    T CreateAccumulator() override
    {
        return T{};
    }

    void AddInput(T* accum, const T& input) override
    {
        *accum += input;
    }

    T MergeAccumulators(TInput<T>& accums) override
    {
        T t = CreateAccumulator();
        while (const T* cur = accums.Next()) {
            t += *cur;
        }
        return t;
    }

    T ExtractOutput(const T& accum) override
    {
        return accum;
    }
};

template <typename T>
::TIntrusivePtr<TSumCombineFn<T>> Sum()
{
    return MakeIntrusive<TSumCombineFn<T>>();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NRoren
