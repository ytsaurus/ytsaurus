#pragma once

#include <functional>

namespace NYT {
namespace NDetail {

////////////////////////////////////////////////////////////////////

// NOTE: YT server part already has similar class in NYT namespace.
class TFinallyGuard {
public:
    TFinallyGuard(std::function<void(void)> finally);
    TFinallyGuard(const TFinallyGuard& finallyGuard) = delete;

    ~TFinallyGuard();

private:
    std::function<void(void)> Finally_;
};

////////////////////////////////////////////////////////////////////

} // namespace NDetail
} // namespace NYT
