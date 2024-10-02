#pragma once

#include "object.h"
#include "type_handler.h"

namespace NYT::NOrm::NServer::NObjects {

////////////////////////////////////////////////////////////////////////////////

template <std::derived_from<TObject> TGroup, std::derived_from<IObjectTypeHandler> TGeneratedGroupTypeHandler>
std::unique_ptr<IObjectTypeHandler> CreateGroupTypeHandler(
    NMaster::IBootstrap* bootstrap,
    TObjectManagerConfigPtr config);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NServer::NObjects

#define GROUP_TYPE_HANDLER_DETAIL_INL_H_
#include "group_type_handler_detail-inl.h"
#undef GROUP_TYPE_HANDLER_DETAIL_INL_H_
