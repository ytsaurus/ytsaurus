#pragma once

#include <yt/cpp/roren/interface/transforms.h>

#include <util/system/mutex.h>

#include <deque>

namespace NRoren {

////////////////////////////////////////////////////////////////////////////////

template <typename T>
class TClonablePool
{
    static_assert(std::is_base_of_v<NPrivate::IClonable<T>, T>);
public:
    using TValue = T;
    using TValuePtr = ::TIntrusivePtr<T>;
    class TGuard;

public:
    explicit TClonablePool(TValuePtr templ)
        : Template_(templ)
    { }

    TValuePtr Acquire()
    {
        {
            auto g = Guard(Lock_);
            if (!Pool_.empty()) {
                auto result = std::move(Pool_.back());
                Pool_.pop_back();
                return result;
            }
        }
        return Template_->Clone();
    }

    std::tuple<TGuard, TValue*> GuardedAcquire()
    {
        auto value = Acquire();
        auto* valuePtr = value.Get();
        return {TGuard{this, std::move(value)}, valuePtr};
    }

    void Release(TValuePtr value)
    {
        if (!value) {
            return;
        }
        auto g = Guard(Lock_);
        Pool_.emplace_back(std::move(value));
    }

public:
    class TGuard
    {
        public:
            TGuard(TGuard&& other)
                : Pool_(other.Pool_)
                , Value_(std::move(other.Value_))
            {
                other.Pool_ = nullptr;
            }

            ~TGuard()
            {
                if (Pool_) {
                    Pool_->Release(std::move(Value_));
                }
            }

        private:
            TGuard(TClonablePool* pool, TValuePtr value)
                : Pool_(pool)
                , Value_(std::move(value))
            { }

            TClonablePool* Pool_;
            TValuePtr Value_;

            friend class TClonablePool<T>;
    };

private:
    const TValuePtr Template_;
    TMutex Lock_;
    std::deque<TValuePtr> Pool_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NRoren
