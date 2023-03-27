#include "fls.h"

#include <library/cpp/yt/threading/fork_aware_spin_lock.h>

#include <util/system/sanitizers.h>

#include <array>

namespace NYT::NConcurrency {

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

constexpr int MaxFlsSize = 256;
std::atomic<int> FlsSize;

NThreading::TForkAwareSpinLock FlsLock;
std::array<TFlsSlotDtor, MaxFlsSize> FlsDtors;

thread_local TFls* PerThreadFls =
    [] {
        auto* fls = new TFls();
        NSan::MarkAsIntentionallyLeaked(fls);
        return fls;
    }();
thread_local TFls* CurrentFls = PerThreadFls;

int AllocateFlsSlot(TFlsSlotDtor dtor)
{
    auto guard = Guard(FlsLock);

    int index = FlsSize++;
    YT_VERIFY(index < MaxFlsSize);

    FlsDtors[index] = dtor;

    return index;
}

void DestructFlsSlot(int index, TFls::TCookie cookie)
{
    FlsDtors[index](cookie);
}

} // namespace NDetail

////////////////////////////////////////////////////////////////////////////////

TFls::~TFls()
{
    for (int index = 0; index < std::ssize(Slots_); ++index) {
        if (auto cookie = Slots_[index]) {
            NDetail::DestructFlsSlot(index, cookie);
        }
    }
}

void TFls::Set(int index, TCookie cookie)
{
    if (Y_UNLIKELY(index >= std::ssize(Slots_))) {
        int newSize = NDetail::FlsSize.load();
        YT_VERIFY(index < newSize);
        Slots_.resize(newSize);
    }
    Slots_[index] = cookie;
}

////////////////////////////////////////////////////////////////////////////////

TFls* SwapCurrentFls(TFls* newFls)
{
    auto* oldFls = NDetail::CurrentFls == NDetail::PerThreadFls ? nullptr : NDetail::CurrentFls;
    NDetail::CurrentFls = newFls ? newFls : NDetail::PerThreadFls;
    return oldFls;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NConcurrency::NDetail

