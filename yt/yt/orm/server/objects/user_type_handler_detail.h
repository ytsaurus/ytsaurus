#pragma once

#include "object.h"
#include "type_handler.h"

namespace NYT::NOrm::NServer::NObjects {

////////////////////////////////////////////////////////////////////////////////

template <std::derived_from<TObject> TUser, std::derived_from<IObjectTypeHandler> TGeneratedUserTypeHandler>
std::unique_ptr<IObjectTypeHandler> CreateUserTypeHandler(
    NMaster::IBootstrap* bootstrap,
    TObjectManagerConfigPtr config);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NServer::NObjects

#define USER_TYPE_HANDLER_DETAIL_INL_H_
#include "user_type_handler_detail-inl.h"
#undef USER_TYPE_HANDLER_DETAIL_INL_H_
