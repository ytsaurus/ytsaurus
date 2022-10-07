#include "ref.h"

#include "undumpable.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

static constexpr size_t MinUndumpableSize = 64_KB;

struct TUndumpableHolder
    : public TSharedRangeHolder
{
    TUndumpableHolder(const TSharedRef& ref)
        : Inner(ref.GetHolder())
        , Mark(MarkUndumpable(
            const_cast<void*>(reinterpret_cast<const void*>(&(ref[0]))),
            ref.Size()))
    { }

    ~TUndumpableHolder()
    {
        if (Mark) {
            UnmarkUndumpable(Mark);
        }
    }

    TSharedRangeHolderPtr Inner;
    TUndumpableMark* Mark = nullptr;
};

TSharedRef MarkUndumpable(const TSharedRef& ref)
{
    if (ref.Size() >= MinUndumpableSize) {
        return TSharedRef{ref, New<TUndumpableHolder>(ref)};   
    } else {
        return ref;
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
