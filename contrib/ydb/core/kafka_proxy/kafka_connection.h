#pragma once

#include <contrib/ydb/library/actors/core/actor.h>
#include <contrib/ydb/core/protos/config.pb.h>
#include <contrib/ydb/core/raw_socket/sock_config.h>
#include <contrib/ydb/core/raw_socket/sock_impl.h>

namespace NKafka {

using namespace NKikimr::NRawSocket;

NActors::IActor* CreateKafkaConnection(const TActorId& listenerActorId,
                                       TIntrusivePtr<TSocketDescriptor> socket,
                                       TNetworkConfig::TSocketAddressType address,
                                       const NKikimrConfig::TKafkaProxyConfig& config);

} // namespace NKafka
