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
//! This struct works in two stages:
//!   1. In the constructor it retrieves some data about the table such as
//!      attributes, schema, etc. It can be useful to perform some checks
//!      before starting the upload process.
//!   2. When BeginUpload() is called, it actually starts the upload processes
//!      by creating a transaction, etc.
struct TSchemalessTableUploader
{
    TSchemalessTableUploader(
        TTableWriterOptionsPtr options,
        const TRichYPath& richPath,
        NNative::IClientPtr client,
        TTransactionId transactionId);

    // These fields are available upon construction of the object.
    INodePtr Attributes;
    TUserObject UserObject;
    TTableSchemaPtr ChunkSchema;
    TTableUploadOptions TableUploadOptions;

    // These fields are available after BeginUpload() method was called.
    TLegacyOwningKey WriterLastKey;
    TChunkListId ChunkListId;
    TMasterTableSchemaId ChunkSchemaId;
    ITransactionPtr UploadTransaction;

    const TTableSchemaPtr& GetSchema() const;

    void BeginUpload();
    void EndUpload(TTableYPathProxy::TReqEndUploadPtr endUpload);

private:
    const TTableWriterOptionsPtr Options_;
    const TRichYPath RichPath_;
    const NNative::IClientPtr Client_;
    const TTransactionId TransactionId_;

    const NLogging::TLogger Logger;

    TObjectId ObjectId_;
    NYPath::TYPath ObjectIdPath_;
    NObjectClient::TCellTag NativeCellTag_;
    NObjectClient::TCellTag ExternalCellTag_;

    TTableSchemaPtr GetChunkSchema() const;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
