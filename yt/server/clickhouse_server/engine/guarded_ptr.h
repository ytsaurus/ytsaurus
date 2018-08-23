#pragma once

#include <memory>
#include <mutex>

namespace NYT {
namespace NClickHouse {

////////////////////////////////////////////////////////////////////////////////

// TODO: move to engine/support/guarded_ptr.h

template <typename T>
class TGuardedPtr
{
public:
    using TGuard = std::unique_lock<std::mutex>;
    using TPtr = std::shared_ptr<T>;
    using TWeakPtr = std::weak_ptr<T>;

    class TAccessor
    {
    private:
        TGuard Guard;
        TPtr Ptr;

    public:
        TAccessor(TGuard guard, TPtr ptr)
            : Guard(std::move(guard))
            , Ptr(std::move(ptr))
        {}

        std::shared_ptr<T> operator->()
        {
            return Ptr;
        }

        explicit operator bool() const
        {
            return !!Ptr;
        }
    };

    TGuardedPtr(TPtr ptr)
        : Ptr(std::move(ptr))
    {}

    TAccessor Lock()
    {
        TGuard guard(Mutex);
        return {std::move(guard), Ptr};
    }

    void Release()
    {
        TGuard guard(Mutex);
        Ptr.reset();
    }

private:
    std::mutex Mutex;
    std::shared_ptr<T> Ptr;
};

}   // namespace NClickHouse
}   // namespace NYT
