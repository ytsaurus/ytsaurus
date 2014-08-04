#pragma once

#include "public.h"
#include "managed_instance.h"

#include <atomic>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

template <class TObject, class TMixin = THeapInstanceMixin<TObject>>
class TSingleton
{
private:
    TSingleton() = default;
    TSingleton(const TSingleton&) = delete;
    TSingleton(TSingleton&&) = delete;

    static TManagedInstance<TObject, TMixin> Instance_;

protected:
    friend TObject;

    static TObject* Get()
    {
        static_assert(
            std::is_literal_type<TSingleton>::value,
            "Singletons must be of a literal type.");
        return Instance_.Get();
    }

    static TObject* TryGet()
    {
        static_assert(
            std::is_literal_type<TSingleton>::value,
            "Singletons must be of a literal type.");
        return Instance_.TryGet();
    }
};

// XXX(sandello):
// http://en.cppreference.com/w/cpp/language/initialization
//
// Unordered dynamic initialization, which applies only to (static/thread-local)
// class template data members that aren't explicitly specialized.
// Initialization of such static variables is indeterminately sequenced
// with respect to all other dynamic initialization.
// Initialization of such thread-local variables is unsequenced
// with respect to all other dynamic initialization
//
// The compilers are allowed to initialize dynamically-initialized variables
// as part of static initialization (essentially, at compile time),
// if both of these are true:
//
// 1) the dynamic version of the initialization does not change the value of
// any other object of namespace scope prior to its initialization
// 2) the static version of the initialization produces the same value
// in the initialized variable as would be produced by the dynamic initialization
// if all variables not required to be initialized statically were initialized
// dynamically.

template <class TObject, class TMixin>
TManagedInstance<TObject, TMixin> TSingleton<TObject, TMixin>::Instance_;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
