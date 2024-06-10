#pragma once

namespace NRoren::NPrivate
{

template <class T, size_t N>
class TStaticRingBuffer
{
private:
    using TContainer = std::array<T, N>;

    TContainer Values_;
    size_t Position_ = 0;

protected:
    T& GetCurrentValue()
    {
        return Values_[Position_];
    }

public:
    void Push(T value) {
        GetCurrentValue() = std::move(value);
        ++Position_;
        if (Position_ == N) {
            Position_ = 0;
        }
    }

    const T& GetCurrentValue() const
    {
        return Values_[Position_];
    }

    const T& GetLastValue() const
    {
        if (Position_ == 0) {
            return Values_[N-1];
        } else {
            return Values_[Position_-1];
        }
    }

};

template <class T, size_t N>
class TAvgWindow
    : protected TStaticRingBuffer<T, N>
{
private:
    using TStaticRingBuffer<T, N>::GetCurrentValue;
    using TStaticRingBuffer<T, N>::GetLastValue;

public:
    using TStaticRingBuffer<T, N>::Push;

    T GetAvg() const {
        T result = GetLastValue();
        result -= GetCurrentValue();
        //result /= static_cast<double>(N);
        result /= N;
        return result;
    }
};

}  // namespace NRoren::NPrivate
