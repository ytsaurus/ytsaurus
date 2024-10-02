#pragma once

#include "semaphore_detail.h"

#include <yt/yt/core/misc/mpl.h>

namespace NYT::NOrm::NServer::NObjects {

////////////////////////////////////////////////////////////////////////////////

template <
    NMpl::DerivedFromSpecializationOf<TSemaphoreMixin> TSemaphore,
    class TProtoSemaphore,
    std::derived_from<IObjectTypeHandler> TGeneratedSemaphoreTypeHandler>
std::unique_ptr<IObjectTypeHandler> CreateSemaphoreTypeHandler(
    NMaster::IBootstrap* bootstrap,
    TObjectManagerConfigPtr config);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NServer::NObjects

#define SEMAPHORE_TYPE_HANDLER_DETAIL_INL_H_
#include "semaphore_type_handler_detail-inl.h"
#undef SEMAPHORE_TYPE_HANDLER_DETAIL_INL_H_
