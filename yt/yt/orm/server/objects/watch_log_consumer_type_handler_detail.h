#pragma once

#include "object.h"
#include "type_handler.h"

namespace NYT::NOrm::NServer::NObjects {

////////////////////////////////////////////////////////////////////////////////

template <
    std::derived_from<TObject> TWatchLogConsumer,
    std::derived_from<IObjectTypeHandler> TGeneratedWatchLogConsumerTypeHandler>
std::unique_ptr<IObjectTypeHandler> CreateWatchLogConsumerTypeHandler(
    NMaster::IBootstrap* bootstrap,
    TObjectManagerConfigPtr config);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NServer::NObjects

#define WATCH_LOG_CONSUMER_TYPE_HANDLER_DETAIL_INL_H_
#include "watch_log_consumer_type_handler_detail-inl.h"
#undef WATCH_LOG_CONSUMER_TYPE_HANDLER_DETAIL_INL_H_
