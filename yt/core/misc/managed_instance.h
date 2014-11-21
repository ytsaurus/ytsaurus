#pragma once

#include "common.h"
#include "at_exit_manager.h"

#include <util/system/yield.h>

#include <atomic>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {
#define XX(name, ty, def) \
template <class T, typename = int> struct TGet##name : std::integral_constant<ty, def> { }; \
template <class T> struct TGet##name <T, decltype((void)T::name, 0)> : std::integral_constant<ty, T::name> { };
XX(Priority, int, 0)
XX(DeleteAtExit, bool, true)
XX(ResetAtFork, bool, true)
#undef XX
} // namespace NDetail

template <class TObject>
struct THeapInstanceMixin
{
    TObject* New()
    {
        return new TObject();
    }

    void Delete(TObject* object)
    {
        delete object;
    }

    void Reset(TObject* /*object*/)
    {
        // Well, we could release memory back to the OS, but we just don't care right now.
    }
};

template <class TObject>
struct TRefCountedInstanceMixin
{
    TObject* New()
    {
        return new TObject(); // Leaves a dangling reference.
    }

    void Delete(TObject* object)
    {
        object->Unref(); // Drops the dangling reference.
    }

    void Reset(TObject* /*object*/)
    { }
};

template <class TObject>
struct TStaticInstanceMixin
{
    typedef typename std::aligned_storage<sizeof(TObject), alignof(TObject)>::type TStorage;

    std::atomic_flag Alive_;
    TStorage Storage_;

    TObject* New()
    {
        YCHECK(!Alive_.test_and_set());
        memset(&Storage_, 0x00, sizeof(Storage_));
        ::new(&Storage_) TObject();
        return reinterpret_cast<TObject*>(&Storage_);
    }

    void Delete(TObject* object)
    {
        YCHECK(reinterpret_cast<TObject*>(&Storage_) == object);
        YCHECK(Alive_.test_and_set());
        reinterpret_cast<TObject*>(&Storage_)->~TObject();
        memset(&Storage_, 0xBA, sizeof(Storage_));
        Alive_.clear();
    }

    void Reset(TObject* object)
    {
        YCHECK(reinterpret_cast<TObject*>(&Storage_) == object);
        Alive_.test_and_set();
        memset(&Storage_, 0x00, sizeof(Storage_));
        Alive_.clear();
    }
};

template <class TObject, class TMixin = THeapInstanceMixin<TObject>>
class TManagedInstance
    : private TMixin // For empty-base optimization.
{
private:
    std::atomic<TObject*> Instance_;

    // OH DAT C++!
    static TObject* GetBeingConstructedMarker()
    {
        return (TObject*) 0x1;
    }

    static constexpr int Priority_ =
        NYT::NDetail::TGetPriority<TObject>::value;
    static constexpr bool DeleteAtExit_ =
        NYT::NDetail::TGetDeleteAtExit<TObject>::value;
    static constexpr bool ResetAtFork_ =
        NYT::NDetail::TGetResetAtFork<TObject>::value;

public:
#ifdef _win_
    TManagedInstance() noexcept = default;
#else
    constexpr TManagedInstance() noexcept = default;
#endif

    TManagedInstance(const TManagedInstance&) = delete;

    TManagedInstance(TManagedInstance&&) = delete;

    TObject* Get()
    {
#ifndef _win_
        // Basically, check that managed instances could be const-initialized.
        static_assert(
            std::is_literal_type<TManagedInstance>::value,
            "Managed instances must be of a literal type.");
#endif

        // Loading with 'acquire' semantics to ensure that thread sees all instance data.
        auto value = Instance_.load(std::memory_order_acquire);
        if (UNLIKELY(value == nullptr || value == GetBeingConstructedMarker())) {
            // Keep function inlineable.
            value = GetSlow();
        }
        return value;
    }

    TObject* TryGet()
    {
#ifndef _win_
        // Basically, check that managed instances could be const-initialized.
        static_assert(
            std::is_literal_type<TManagedInstance>::value,
            "Managed instances must be of a literal type.");
#endif

        // Loading with 'acquire' semantics to ensure that thread sees all instance data.
        auto value = Instance_.load(std::memory_order_acquire);
        if (UNLIKELY(value == nullptr || value == GetBeingConstructedMarker())) {
            value = nullptr;
        }
        return value;
    }

    TObject* operator->()
    {
        return Get();
    }

private:
    TObject* GetSlow()
    {
        TObject* value = nullptr;

        if (Instance_.compare_exchange_strong(value, GetBeingConstructedMarker(), std::memory_order_acquire)) {
            // Construct the object.
            value = TMixin::New();

            // Storing with 'release' to provide readers with proper visibility of the instance.
            Instance_.store(value, std::memory_order_release);

            if (DeleteAtExit_) {
                TAtExitManager::RegisterAtExit(
                    std::bind(&TManagedInstance::OnExit, this),
                    Priority_);
            }
            if (ResetAtFork_) {
                TAtExitManager::RegisterAtFork(nullptr,
                    nullptr,
                    std::bind(&TManagedInstance::OnFork, this),
                    Priority_);
            }
        } else {
            while (value == GetBeingConstructedMarker()) {
                ThreadYield();
                value = Instance_.load(std::memory_order_acquire);
            }
        }

        return value;
    }

    void OnExit()
    {
        auto value = Instance_.load(std::memory_order_acquire);

        while (value == GetBeingConstructedMarker()) {
            ThreadYield();
            value = Instance_.load(std::memory_order_acquire);
        }

        if (Instance_.compare_exchange_strong(value, nullptr, std::memory_order_release)) {
            // Destruct the object.
            TMixin::Delete(value);
        }
    }

    void OnFork()
    {
        auto value = Instance_.load(std::memory_order_acquire);

        Instance_.store(GetBeingConstructedMarker(), std::memory_order_release);
        if (value) {
            TMixin::Reset(value);
        }
        Instance_.store(nullptr, std::memory_order_release);
    }
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
