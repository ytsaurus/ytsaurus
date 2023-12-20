#include "connection.h"

#include "private.h"

#include <yt/yt/core/misc/finally.h>

#include <yt/yt/core/net/connection.h>
#include <yt/yt/core/net/dialer.h>

#include <library/cpp/yt/string/guid.h>

namespace NYT::NTcpProxy {

using namespace NConcurrency;
using namespace NNet;

////////////////////////////////////////////////////////////////////////////////

using TConnectionId = TGuid;

////////////////////////////////////////////////////////////////////////////////

void PipeConnectionReaderToWriter(
    IConnectionReaderPtr reader,
    IConnectionWriterPtr writer,
    TConnectionId connectionId)
{
    auto Logger = TcpProxyLogger.WithTag("ConnectionId: %v", connectionId);

    auto closeGuard = Finally([&] {
        // Fire and forget.
        reader->Abort();
        writer->Abort();
    });

    try {
        struct TConnectionProxyTag
        { };

        auto buffer = TSharedMutableRef::Allocate<TConnectionProxyTag>(1_MB);
        while (true) {
            // TODO(gritukan): Get rid of WaitFor here one day.
            auto readSize = WaitFor(reader->Read(buffer))
                .ValueOrThrow();
            if (readSize == 0) {
                YT_LOG_DEBUG("Connection terminated");
                break;
            }
            auto data = buffer.Slice(0, readSize);
            WaitFor(writer->Write(data))
                .ThrowOnError();
        }
    } catch (const std::exception& ex) {
        YT_LOG_DEBUG(ex, "Terminating connection");
    }
}

void HandleConnection(
    IConnectionPtr sourceConnection,
    TNetworkAddress destinationAddress,
    const IDialerPtr& dialer,
    IInvokerPtr invoker)
{
    auto connectionId = TGuid::Create();
    auto Logger = TcpProxyLogger.WithTag("ConnectionId: %v", connectionId);

    YT_LOG_DEBUG("Connection accepted (SourceAddress: %v, DestinationAddress: %v)",
        sourceConnection->RemoteAddress(),
        destinationAddress);

    auto onDestinationDialed = BIND([=] (const TErrorOr<IConnectionPtr>& connectionOrError) {
        if (!connectionOrError.IsOK()) {
            YT_LOG_DEBUG(connectionOrError, "Failed to dial destination, dropping connection");
            return;
        }
        const auto& destinationConnection = connectionOrError.Value();

        YT_LOG_DEBUG("Started connection proxying");

        invoker->Invoke(BIND(&PipeConnectionReaderToWriter, sourceConnection, destinationConnection, connectionId));
        invoker->Invoke(BIND(&PipeConnectionReaderToWriter, destinationConnection, sourceConnection, connectionId));
    }).AsyncVia(invoker);

    // TODO(gritukan): Think about async dialer.
    dialer->Dial(destinationAddress)
        .Apply(onDestinationDialed);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTcpProxy
