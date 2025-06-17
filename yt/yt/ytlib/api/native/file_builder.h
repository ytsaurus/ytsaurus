#pragma once

#include "public.h"
#include "private.h"

#include <yt/yt/client/api/client.h>
#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt/core/crypto/crypto.h>
#include <yt/yt/core/logging/log.h>
#include <yt/yt/core/ypath/public.h>
#include <yt/yt/ytlib/chunk_client/helpers.h>
#include <yt/yt/ytlib/file_client/file_ypath_proxy.h>
#include <yt/yt/ytlib/transaction_client/transaction_listener.h>

namespace NYT::NApi::NNative {

using namespace NChunkClient;
using namespace NChunkClient::NProto;
using namespace NCrypto;
using namespace NObjectClient;
using namespace NTransactionClient;
using namespace NYPath;

class TFileBuilder
    : public TTransactionListener
{
public:
    TFileBuilder(
        IClientPtr client,
        const TRichYPath& path,
        const TFileWriterOptions& options = TFileWriterOptions());

    TMultiChunkWriterOptionsPtr WriterOptions;
    TCellTag ExternalCellTag = InvalidCellTag;
    TObjectId ObjectId;
    TChunkListId ChunkListId;
    std::optional<TMD5Hasher> MD5Hasher;

    void ValidateAborted();

    void Close(NChunkClient::TChunkOwnerYPathProxy::TReqEndUploadPtr endUpload);

private:
    const IClientPtr Client_;
    const TRichYPath Path_;
    const TFileWriterOptions Options_;
    const TFileWriterConfigPtr Config_;

    NApi::ITransactionPtr Transaction_;
    NApi::ITransactionPtr UploadTransaction_;

    TCellTag NativeCellTag_ = InvalidCellTag;

    const NLogging::TLogger Logger;
};

DECLARE_REFCOUNTED_CLASS(TFileBuilder)
DEFINE_REFCOUNTED_TYPE(TFileBuilder);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi::NNative

