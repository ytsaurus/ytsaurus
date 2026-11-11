#ifndef RECORD_HELPERS_INL_H_
#error "Direct inclusion of this file is not allowed, include record_helpers.h"
// For the sake of sane code completion.
#include "record_helpers.h"
#endif

namespace NYT::NSequoiaClient {

////////////////////////////////////////////////////////////////////////////////

template <CHasKey TRecord>
TStrongOrderingTags GetStrongOrderingTags(const TRecord& record)
{
    return GetStrongOrderingTags(record.Key);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSequoiaClient
