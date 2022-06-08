#pragma once

#include "command.h"

#include <yt/yt/client/api/internal_client.h>

#include <yt/yt/core/ytree/public.h>

namespace NYT::NDriver {

////////////////////////////////////////////////////////////////////////////////

class TReadHunksCommand
    : public TTypedCommand<NApi::TReadHunksOptions>
{
public:
    TReadHunksCommand();

private:
    class TSerializableReadHunkRequest
        : public NApi::TReadHunkRequest
        , public NYTree::TYsonSerializable
    {
    public:
        TSerializableReadHunkRequest();
    };

    using TSerializableReadHunkRequestPtr = TIntrusivePtr<TSerializableReadHunkRequest>;

    std::vector<TSerializableReadHunkRequestPtr> Requests;
    NYTree::INodePtr ChunkFragmentReader;

    void DoExecute(ICommandContextPtr context) override;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDriver
