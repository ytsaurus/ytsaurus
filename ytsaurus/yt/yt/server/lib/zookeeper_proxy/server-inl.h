#ifndef SERVER_INL_H_
#error "Direct inclusion of this file is not allowed, include server.h"
// For the sake of sane code completion.
#include "server.h"
#endif

#include <yt/yt/core/actions/bind.h>

namespace NYT::NZookeeperProxy {

////////////////////////////////////////////////////////////////////////////////

template <class TRequest, class TResponse>
void IServer::RegisterTypedHandler(TTypedHandler<TRequest, TResponse> handler)
{
    auto typedHandler = [handler] (const TSharedRefArray& request) -> TSharedRefArray {
        TRequest typedRequest;

        if (request.size() != 1) {
            THROW_ERROR_EXCEPTION("Incoming message has %v, expected 1",
                request.size());
        }

        auto protocolReader = NZookeeper::CreateZookeeperProtocolReader(request[0]);
        typedRequest.Deserialize(protocolReader.get());

        auto typedResponse = handler(typedRequest);

        auto protocolWriter = NZookeeper::CreateZookeeperProtocolWriter();
        typedResponse.Serialize(protocolWriter.get());

        return TSharedRefArray(protocolWriter->Finish());
    };

    RegisterHandler(TRequest::GetRequestType(), BIND(typedHandler));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NZookeeperProxy
