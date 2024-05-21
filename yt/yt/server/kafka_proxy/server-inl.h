#ifndef SERVER_INL_H_
#error "Direct inclusion of this file is not allowed, include server.h"
// For the sake of sane code completion.
#include "server.h"
#endif

#include <yt/yt/core/actions/bind.h>

namespace NYT::NKafkaProxy {

////////////////////////////////////////////////////////////////////////////////

template <class TRequest, class TResponse>
void IServer::RegisterTypedHandler(TTypedHandler<TRequest, TResponse> handler)
{
    auto typedHandler = [handler] (const TConnectionId& connectionId, NKafka::IKafkaProtocolReader* requestReader, int version) -> TSharedRef {
        TRequest typedRequest;

        typedRequest.Deserialize(requestReader, version);

        auto typedResponse = handler(connectionId, typedRequest);

        auto protocolWriter = NKafka::CreateKafkaProtocolWriter();
        typedResponse.Serialize(protocolWriter.get(), version);

        return protocolWriter->Finish();
    };

    RegisterHandler(TRequest::GetRequestType(), BIND(typedHandler));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NKafkaProxy
