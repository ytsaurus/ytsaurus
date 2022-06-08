#include "internal_commands.h"

#include "config.h"

#include <yt/yt/core/ytree/fluent.h>

namespace NYT::NDriver {

using namespace NConcurrency;
using namespace NApi;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

TReadHunksCommand::TSerializableReadHunkRequest::TSerializableReadHunkRequest()
{
    RegisterParameter("chunk_id", ChunkId);
    RegisterParameter("erasure_codec", ErasureCodec)
        .Optional();
    RegisterParameter("block_index", BlockIndex);
    RegisterParameter("block_offset", BlockOffset);
    RegisterParameter("block_size", BlockSize)
        .Optional();
    RegisterParameter("length", Length);
}

////////////////////////////////////////////////////////////////////////////////

TReadHunksCommand::TReadHunksCommand()
{
    RegisterParameter("requests", Requests);
}

void TReadHunksCommand::DoExecute(ICommandContextPtr context)
{
    Options.Config = UpdateYsonStruct(
        context->GetConfig()->ChunkFragmentReader,
        ChunkFragmentReader);

    std::vector<TReadHunkRequest> requests;
    for (const auto& request : Requests) {
        requests.push_back(*request);
    }

    auto internalClient = context->GetInternalClientOrThrow();
    auto responses = WaitFor(internalClient->ReadHunks(requests, Options))
        .ValueOrThrow();

    context->ProduceOutputValue(BuildYsonStringFluently()
        .BeginMap()
            .Item("responses").DoListFor(responses, [&] (auto fluent, const TSharedRef& response) {
                fluent
                    .Item().BeginMap()
                        .Item("payload").Value(TStringBuf(response.Begin(), response.End()))
                    .EndMap();
            })
        .EndMap());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDriver
