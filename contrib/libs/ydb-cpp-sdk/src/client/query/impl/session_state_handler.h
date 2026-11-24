#pragma once

#include <src/client/impl/session/kqp_session_common.h>

#include <contrib/ydb/public/api/protos/ydb_query.pb.h>

#include <memory>

namespace NYdb::inline V3::NQuery {

enum class EAttachStreamReadAction {
    Continue,
    Stop,
};

EAttachStreamReadAction HandleAttachSessionState(
    const Ydb::Query::SessionState& state,
    TKqpSessionCommon* session,
    const std::shared_ptr<ISessionClient>& client);

}
