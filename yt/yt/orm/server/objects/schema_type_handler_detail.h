#pragma once

#include "type_handler.h"

namespace NYT::NOrm::NServer::NObjects {

////////////////////////////////////////////////////////////////////////////////

template <std::derived_from<IObjectTypeHandler> TGeneratedSchemaTypeHandler>
std::unique_ptr<IObjectTypeHandler> CreateSchemaTypeHandler(
    NMaster::IBootstrap* bootstrap,
    TObjectManagerConfigPtr config);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NServer::NObjects

#define SCHEMA_TYPE_HANDLER_DETAIL_INL_H_
#include "schema_type_handler_detail-inl.h"
#undef SCHEMA_TYPE_HANDLER_DETAIL_INL_H_
