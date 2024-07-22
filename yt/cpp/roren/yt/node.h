#pragma once

#include <concepts>

namespace NRoren::NPrivate {

////////////////////////////////////////////////////////////////////////////////

template <typename TDerived>
class TNodeHierarchyRoot
{
public:
    virtual ~TNodeHierarchyRoot() = default;

    template <typename T>
    T* VerifiedAsPtr()
    {
        static_assert(std::derived_from<T, TDerived>, "Class is not derived from TNodeHierarchyRoot");
        auto* result = dynamic_cast<T*>(this);
        Y_ABORT_UNLESS(result);
        return result;
    }

    template <typename T>
    const T* VerifiedAsPtr() const
    {
        static_assert(std::derived_from<T, TDerived>, "Class is not derived from TNodeHierarchyRoot");
        const auto* result = dynamic_cast<const T*>(this);
        Y_ABORT_UNLESS(result);
        return result;
    }

    template <typename T>
    T* TryAsPtr()
    {
        return dynamic_cast<T*>(this);
    }

    template <typename T>
    const T* TryAsPtr() const
    {
        return dynamic_cast<const T*>(this);
    }
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NRoren::NPrivate
