#ifndef ACE_ITERATOR_INL_H_
#error "Direct inclusion of this file is not allowed, include ace_iterator.h"
// For the sake of sane code completion.
#include "ace_iterator.h"
#endif

namespace NYT::NSecurityServer {

Y_FORCE_INLINE const TAccessControlEntry& TAceIterator::operator*() noexcept
{
    return *Ace_;
}

Y_FORCE_INLINE bool TAceIterator::operator==(const TAceIterator& another) noexcept
{
    return Ace_ == another.Ace_;
}

} // namespace NYT::NSecurityServer
