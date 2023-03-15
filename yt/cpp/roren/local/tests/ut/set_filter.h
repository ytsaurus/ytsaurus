#pragma once

#include <yt/cpp/roren/interface/roren.h>

template <typename T>
class TSetFilterParDo
    : public NRoren::IDoFn<T, T>
{
public:
    TSetFilterParDo() = default;

    explicit TSetFilterParDo(THashSet<T> set)
        : Set_(std::move(set))
    { }

    void Do(const T& input, NRoren::TOutput<T>& output) override
    {
        if (Set_.contains(input)) {
            output.Add(input);
        }
    }

    Y_SAVELOAD_DEFINE_OVERRIDE(Set_);

private:
    THashSet<T> Set_;
};
