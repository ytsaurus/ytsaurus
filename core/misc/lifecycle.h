#pragma once

#include "public.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

class TLifecycle
{
public:
    TLifecycle();

    TLifecycle(const TLifecycle&) = delete;
    TLifecycle(TLifecycle&&) = delete;

    ~TLifecycle();

    static void RegisterAtExit(
        std::function<void()> callback,
        size_t priority = 0);

    static void RegisterAtFork(
        std::function<void()> prepareCallback,
        std::function<void()> parentCallback,
        std::function<void()> childCallback,
        size_t priority = 0);

protected:
    explicit TLifecycle(bool allowShadow);

    void FireAtExit();
    void FireAtForkPrepare();
    void FireAtForkParent();
    void FireAtForkChild();

    static void GlobalAtExitCallback();
    static void GlobalAtForkPrepareCallback();
    static void GlobalAtForkParentCallback();
    static void GlobalAtForkChildCallback();

private:
    class TImpl;
    std::unique_ptr<TImpl> Impl_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
