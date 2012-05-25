#pragma once

#include <ytlib/misc/common.h>
#include <ytlib/misc/guid.h>

namespace NYT {
namespace NBus {

////////////////////////////////////////////////////////////////////////////////

struct IMessage;
typedef TIntrusivePtr<IMessage> IMessagePtr;

struct IBus;
typedef TIntrusivePtr<IBus> IBusPtr;

struct IMessageHandler;
typedef TIntrusivePtr<IMessageHandler> IMessageHandlerPtr;

struct IBusClient;
typedef TIntrusivePtr<IBusClient> IBusClientPtr;

struct IBusServer;
typedef TIntrusivePtr<IBusServer> IBusServerPtr;

struct TBusStatistics;

typedef TGuid TSesisonId;

struct TTcpBusServerConfig;
typedef TIntrusivePtr<TTcpBusServerConfig> TTcpBusServerConfigPtr;

struct TTcpBusClientConfig;
typedef TIntrusivePtr<TTcpBusClientConfig> TTcpBusClientConfigPtr;

////////////////////////////////////////////////////////////////////////////////

} // namespace NBus
} // namespace NYT

