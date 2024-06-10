#include <library/cpp/yt/memory/intrusive_ptr.h>

namespace NRoren::NPrivate {

////////////////////////////////////////////////////////////////////////////////

template <typename T>
struct TUnwrapIntrusivePtr
{
    static_assert(false && T::NotExistentMember, "T is not NYT::TIntrusivePtr");
};

template <typename T>
struct TUnwrapIntrusivePtr<NYT::TIntrusivePtr<T>>
{
    using TUnwrapped = T;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NRoren::NPrivate
