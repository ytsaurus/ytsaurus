#include "schemaless_chunk_writer.h"

#include "chunk_meta_extensions.h"
#include "config.h"
#include "helpers.h"
#include "partitioner.h"
#include "schemaless_block_writer.h"
#include "skynet_column_evaluator.h"
#include "table_ypath_proxy.h"
#include "versioned_chunk_writer.h"

#include <yt/yt/ytlib/table_chunk_format/column_writer.h>
#include <yt/yt/ytlib/table_chunk_format/data_block_writer.h>
#include <yt/yt/ytlib/table_chunk_format/schemaless_column_writer.h>

#include <yt/yt/ytlib/chunk_client/block_cache.h>
#include <yt/yt/ytlib/chunk_client/chunk_writer.h>
#include <yt/yt/ytlib/chunk_client/deferred_chunk_meta.h>
#include <yt/yt/ytlib/chunk_client/dispatcher.h>
#include <yt/yt/ytlib/chunk_client/encoding_chunk_writer.h>
#include <yt/yt/ytlib/chunk_client/helpers.h>
#include <yt/yt/ytlib/chunk_client/multi_chunk_writer_base.h>

#include <yt/yt/ytlib/chunk_client/medium_directory.h>
#include <yt/yt/ytlib/chunk_client/medium_directory_synchronizer.h>

#include <yt/yt/ytlib/cypress_client/cypress_ypath_proxy.h>
#include <yt/yt/ytlib/cypress_client/rpc_helpers.h>

#include <yt/yt/ytlib/object_client/helpers.h>
#include <yt/yt/ytlib/object_client/object_service_proxy.h>

#include <yt/yt/ytlib/transaction_client/transaction_listener.h>

#include <yt/yt/ytlib/api/native/client.h>
#include <yt/yt/ytlib/api/native/config.h>
#include <yt/yt/ytlib/api/native/connection.h>

#include <yt/yt/library/query/engine_api/column_evaluator.h>

#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt/client/transaction_client/timestamp_provider.h>

#include <yt/yt/client/signature/generator.h>
#include <yt/yt/client/signature/signature.h>

#include <yt/yt/client/table_client/check_schema_compatibility.h>
#include <yt/yt/client/table_client/helpers.h>
#include <yt/yt/client/table_client/logical_type.h>
#include <yt/yt/client/table_client/name_table.h>
#include <yt/yt/client/table_client/row_buffer.h>
#include <yt/yt/client/table_client/table_upload_options.h>
#include <yt/yt/client/table_client/timestamped_schema_helpers.h>

#include <yt/yt/client/api/distributed_table_session.h>
#include <yt/yt/client/api/transaction.h>

#include <yt/yt/client/ypath/rich.h>

#include <yt/yt/core/misc/random.h>

#include <util/generic/cast.h>
#include <util/generic/ylimits.h>

#include <utility>

namespace NYT::NTableClient {

using namespace NApi;
using namespace NChunkClient::NProto;
using namespace NChunkClient;
using namespace NConcurrency;
using namespace NCrypto;
using namespace NCypressClient;
using namespace NNodeTrackerClient;
using namespace NObjectClient;
using namespace NQueryClient;
using namespace NRpc;
using namespace NTableChunkFormat;
using namespace NTableClient::NProto;
using namespace NTabletClient;
using namespace NTracing;
using namespace NTransactionClient;
using namespace NYPath;
using namespace NYTree;
using namespace NYson;

using NYT::FromProto;
using NYT::TRange;
using NYT::ToProto;

////////////////////////////////////////////////////////////////////////////////

class TSchemalessTableUploader
{
public:
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

    void Close(TTableYPathProxy::TReqEndUploadPtr endUpload);

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
