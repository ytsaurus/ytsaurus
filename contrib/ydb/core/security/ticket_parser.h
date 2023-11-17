#pragma once
#include <contrib/ydb/core/protos/config.pb.h>
#include <contrib/ydb/core/base/ticket_parser.h>

namespace NKikimr {
    IActor* CreateTicketParser(const NKikimrProto::TAuthConfig& authConfig);
}
