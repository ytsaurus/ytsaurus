#pragma once

#include "public.h"

#include <yt/yt/ytlib/api/native/public.h>
#include <yt/yt/ytlib/chunk_client/helpers.h>

#include <yt/yt/client/api/table_client.h>
#include <yt/yt/client/ypath/rich.h>
#include <yt/yt/client/table_client/table_upload_options.h>

namespace NYT::NTableClient {

////////////////////////////////////////////////////////////////////////////////

// Shared upload setup for writers that create their chunks outside the normal
// schemaless row writer, such as externally attached source objects.
struct TSchemalessTableUploader
{
    TSchemalessTableUploader(
        TTableWriterOptionsPtr options,
        const NYPath::TRichYPath& richPath,
        NApi::NNative::IClientPtr client,
        NTransactionClient::TTransactionId transactionId);

    TTableWriterOptionsPtr Options;
    NChunkClient::TUserObject UserObject;
    TTableSchemaPtr ChunkSchema;
    NChunkClient::TChunkListId ChunkListId;
    TMasterTableSchemaId ChunkSchemaId;
    NApi::ITransactionPtr UploadTransaction;
    TTableUploadOptions TableUploadOptions;

    const TTableSchemaPtr& GetSchema() const;

    void EndUpload(NChunkClient::NProto::TDataStatistics dataStatistics);

private:
    const NYPath::TRichYPath RichPath_;
    const NApi::NNative::IClientPtr Client_;
    const NLogging::TLogger Logger;
    NObjectClient::TObjectId ObjectId_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
