#pragma once
#include <util/generic/string.h>

#include <contrib/ydb/core/protos/msgbus.pb.h>

namespace NKikimr::NSQS {

TString SecureShortUtf8DebugString(const NKikimrClient::TSqsRequest& msg);
TString SecureShortUtf8DebugString(const NKikimrClient::TSqsResponse& msg);

} // namespace NKikimr::NSQS
