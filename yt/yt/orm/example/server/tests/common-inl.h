#ifndef COMMON_INL_H_
#error "Direct inclusion of this file is not allowed, include common.h"
// For the sake of sane code completion.
#include "common.h"
#endif

namespace NYT::NOrm::NExample::NServer::NTests {

////////////////////////////////////////////////////////////////////////////////

template<typename TTypedObject, std::same_as<TObjectKey> ...TObjectKeys>
auto GetObjects(const TTransactionPtr& transaction, TObjectKeys... keys)
{
    return std::tuple(transaction->GetTypedObject<TTypedObject>(keys)...);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NExample::NServer::NTests
