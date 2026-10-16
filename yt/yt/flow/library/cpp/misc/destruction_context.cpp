#include "destruction_context.h"

#include <yt/yt/core/concurrency/fls.h>

#include <utility>

namespace NYT::NFlow {
namespace {

////////////////////////////////////////////////////////////////////////////////

NConcurrency::TFlsSlot<TDestructionContextGuard*>& GetDestructionContextGuardSlot()
{
    static NConcurrency::TFlsSlot<TDestructionContextGuard*> Slot;
    return Slot;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace

TDestructionContextGuard::TDestructionContextGuard()
    : Current_(GetDestructionContextGuardSlot().GetOrCreate())
    , Previous_(std::exchange(*Current_, this))
    , Root_(Previous_ ? Previous_->Root_ : this)
{ }

TDestructionContextGuard::~TDestructionContextGuard()
{
    *Current_ = Previous_;
    // Destroy in the order the values were added (std::vector destroys backwards).
    for (auto& value : Values_) {
        value.Reset();
    }
}

void TDestructionContextGuard::Add(TRefCountedPtr value)
{
    auto* current = GetDestructionContextGuardSlot().TryGet();
    if (!current || !*current) {
        return;
    }

    (*current)->Root_->Values_.push_back(std::move(value));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
