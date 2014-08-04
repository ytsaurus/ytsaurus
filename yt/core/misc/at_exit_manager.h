#pragma once

#include "public.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

class TAtExitManager
{
public:
    TAtExitManager();

    TAtExitManager(const TAtExitManager&) = delete;
    TAtExitManager(TAtExitManager&&) = delete;

    ~TAtExitManager();

    static void RegisterAtExit(
        std::function<void()> callback,
        size_t priority = std::numeric_limits<size_t>::max());

    static void RegisterAtFork(
        std::function<void()> prepareCallback,
        std::function<void()> parentCallback,
        std::function<void()> childCallback,
        size_t priority = std::numeric_limits<size_t>::max());

protected:
    explicit TAtExitManager(bool allowShadowing);

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
