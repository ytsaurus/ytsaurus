#include "internal_client.h"

namespace NYT::NApi {

////////////////////////////////////////////////////////////////////////////////

TSerializableHunkDescriptor::TSerializableHunkDescriptor()
{
    Initialize();
}

TSerializableHunkDescriptor::TSerializableHunkDescriptor(const THunkDescriptor& descriptor)
    : THunkDescriptor(descriptor)
{
    Initialize();
}

void TSerializableHunkDescriptor::Initialize()
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

} // namespace NYT::NApi
