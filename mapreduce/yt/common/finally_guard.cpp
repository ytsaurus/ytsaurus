#include "finally_guard.h"

namespace NYT {
namespace NDetail {

////////////////////////////////////////////////////////////////////

TFinallyGuard::TFinallyGuard(std::function<void(void)> finally)
    : Finally_(std::move(finally))
{ }

TFinallyGuard::~TFinallyGuard()
{
    Finally_();
}

////////////////////////////////////////////////////////////////////

} // namespace NDetail
} // namespace NYT
