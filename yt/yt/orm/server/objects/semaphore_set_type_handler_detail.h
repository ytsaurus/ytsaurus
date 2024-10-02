#pragma once

#include "semaphore_set_detail.h"

#include <yt/yt/core/misc/mpl.h>

namespace NYT::NOrm::NServer::NObjects {

////////////////////////////////////////////////////////////////////////////////

template <
    NMpl::DerivedFromSpecializationOf<TSemaphoreSetMixin> TSemaphoreSet,
    class TProtoSemaphoreSet,
    std::derived_from<IObjectTypeHandler> TGeneratedSemaphoreSetTypeHandler>
std::unique_ptr<IObjectTypeHandler> CreateSemaphoreSetTypeHandler(
    NMaster::IBootstrap* bootstrap,
    TObjectManagerConfigPtr config);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NServer::NObjects

#define SEMAPHORE_SET_TYPE_HANDLER_DETAIL_INL_H_
#include "semaphore_set_type_handler_detail-inl.h"
#undef SEMAPHORE_SET_TYPE_HANDLER_DETAIL_INL_H_
