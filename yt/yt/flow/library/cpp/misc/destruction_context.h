#pragma once

#include <library/cpp/yt/memory/ref_counted.h>

#include <vector>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

//! Defers destruction of ref-counted objects until the end of the outermost
//! guarded scope on the current fiber.
//!
//! Useful when an object must not be destroyed in the current scope, e.g. while
//! holding a lock that its destructor may (indirectly) contend on.
//!
//! Guards may nest: an inner guard joins the outermost (root) context, so values
//! added from an inner scope are destroyed only when the outermost guard goes out
//! of scope.
class [[nodiscard]] TDestructionContextGuard
{
public:
    TDestructionContextGuard();
    ~TDestructionContextGuard();

    TDestructionContextGuard(const TDestructionContextGuard&) = delete;
    TDestructionContextGuard& operator=(const TDestructionContextGuard&) = delete;

    //! Defers destruction of #value until the outermost guard on the current fiber
    //! goes out of scope. If no guard is active, destroys #value immediately.
    static void Add(TRefCountedPtr value);

private:
    TDestructionContextGuard** const Current_;
    TDestructionContextGuard* const Previous_;
    TDestructionContextGuard* const Root_;

    // Only populated in the root guard.
    std::vector<TRefCountedPtr> Values_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
