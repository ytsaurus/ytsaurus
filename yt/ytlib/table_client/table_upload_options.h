#pragma once

#include <yt/ytlib/chunk_client/chunk_owner_ypath_proxy.h>

#include <yt/client/table_client/schema.h>

#include <yt/core/erasure/public.h>

#include <yt/core/misc/phoenix.h>

namespace NYT::NTableClient {

////////////////////////////////////////////////////////////////////////////////

struct TTableUploadOptions
{
    NChunkClient::EUpdateMode UpdateMode;
    NCypressClient::ELockMode LockMode;
    TTableSchema TableSchema;
    ETableSchemaMode SchemaMode;
    EOptimizeFor OptimizeFor;
    NCompression::ECodec CompressionCodec;
    NErasure::ECodec ErasureCodec;

    void Persist(NPhoenix::TPersistenceContext& context);
};

TTableUploadOptions GetTableUploadOptions(
    const NYPath::TRichYPath& path,
    const NYTree::IAttributeDictionary& cypressTableAttributes,
    i64 rowCount);

////////////////////////////////////////////////////////////////////////////////

}
