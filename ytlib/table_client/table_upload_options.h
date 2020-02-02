#pragma once

#include "public.h"

#include <yt/ytlib/chunk_client/public.h>

#include <yt/client/table_client/schema.h>

#include <yt/client/security_client/public.h>

#include <yt/library/erasure/public.h>

#include <yt/core/compression/public.h>

#include <yt/core/misc/phoenix.h>

namespace NYT::NTableClient {

////////////////////////////////////////////////////////////////////////////////

struct TTableUploadOptions
{
    NChunkClient::EUpdateMode UpdateMode;
    NCypressClient::ELockMode LockMode;
    TTableSchema TableSchema;
    ETableSchemaModification SchemaModification;
    ETableSchemaMode SchemaMode;
    EOptimizeFor OptimizeFor;
    NCompression::ECodec CompressionCodec;
    NErasure::ECodec ErasureCodec;
    std::optional<std::vector<NSecurityClient::TSecurityTag>> SecurityTags;
    bool PartiallySorted;

    TTableSchema GetUploadSchema() const;

    void Persist(NPhoenix::TPersistenceContext& context);
};

TTableUploadOptions GetTableUploadOptions(
    const NYPath::TRichYPath& path,
    const NYTree::IAttributeDictionary& cypressTableAttributes,
    i64 rowCount);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
