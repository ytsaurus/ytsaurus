#include "config.h"
#include "table_ypath_proxy.h"

#include <yt/yt/ytlib/chunk_client/helpers.h>

#include <yt/yt/ytlib/table_client/table_upload_options.h>

namespace NYT::NTableClient {

using namespace NApi;
using namespace NChunkClient;
using namespace NCypressClient;
using namespace NYPath;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

// TODO(achulkov2): Change to ref-counted class with accessors.
struct TSchemalessTableUploader
{
    TSchemalessTableUploader(
        TTableWriterOptionsPtr options,
        const TRichYPath& richPath,
        NNative::IClientPtr client,
        TTransactionId transactionId);

    INodePtr Attributes;
    TTableWriterConfigPtr WriterConfig;
    TUserObject UserObject;
    TTableSchemaPtr ChunkSchema;
    TLegacyOwningKey WriterLastKey;
    TChunkListId ChunkListId;
    TMasterTableSchemaId ChunkSchemaId;
    ITransactionPtr UploadTransaction;
    TTableUploadOptions TableUploadOptions;

    const TTableSchemaPtr& GetSchema() const;

    void EndUpload(TTableYPathProxy::TReqEndUploadPtr endUpload);

private:
    const TTableWriterOptionsPtr Options_;
    const TRichYPath RichPath_;
    const NNative::IClientPtr Client_;

    const NLogging::TLogger Logger;

    TObjectId ObjectId_;

    TTableSchemaPtr GetChunkSchema() const;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
