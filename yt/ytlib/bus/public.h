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

class TTcpBusConfig;
typedef TIntrusivePtr<TTcpBusConfig> TTcpBusConfigPtr;

class TTcpBusServerConfig;
typedef TIntrusivePtr<TTcpBusServerConfig> TTcpBusServerConfigPtr;

class TTcpBusClientConfig;
typedef TIntrusivePtr<TTcpBusClientConfig> TTcpBusClientConfigPtr;

////////////////////////////////////////////////////////////////////////////////

} // namespace NBus
} // namespace NYT

