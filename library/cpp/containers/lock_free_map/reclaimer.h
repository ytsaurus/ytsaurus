#pragma once

#include <library/cpp/threading/hazard_pointer/hazard_pointer.h>

namespace NLockFreeMap {

    class THazardPointerReclaimer {
    public:
        THazardPointerReclaimer(NHp::THazardPointerDomain& domain = NHp::GetDefaultDomain())
            : Domain_(&domain)
        {}

        using TGuard = NHp::THazardPointer;
        TGuard MakeGuard() const noexcept;

        template <class TValue>
        class TEnableRetirement;

        template <class TValue>
        void Retire(TValue* ptr) const noexcept;

    private:
        NHp::THazardPointerDomain* Domain_;
    };

    inline THazardPointerReclaimer::TGuard THazardPointerReclaimer::MakeGuard() const noexcept {
        return NHp::MakeHazardPointer(*Domain_);
    }

    template <class TValue>
    class THazardPointerReclaimer::TEnableRetirement: public NHp::THazardPointerObjBase<TValue> {};

    template <class TValue>
    void THazardPointerReclaimer::Retire(TValue* ptr) const noexcept {
        ptr->Retire(*Domain_);
    }

} // namespace NLockFreeMap
