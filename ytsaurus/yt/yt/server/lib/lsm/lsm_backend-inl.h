#ifndef LSM_BACKEND_INL_H_
#error "Direct inclusion of this file is not allowed, include lsm_backend.h"
// For the sake of sane code completion.
#include "lsm_backend.h"
#endif

namespace NYT::NLsm {

////////////////////////////////////////////////////////////////////////////////

template <class TRequest>
void TLsmActionBatch::DoMerge(TLsmActionBatch& other, std::vector<TRequest> TLsmActionBatch::* member)
{
    if ((other.*member).empty()) {
        return;
    } else if ((this->*member).empty()) {
        this->*member = std::move(other.*member);
        return;
    }

    (this->*member).insert(
        (this->*member).end(),
        std::make_move_iterator((other.*member).begin()),
        std::make_move_iterator((other.*member).end()));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NLsm
