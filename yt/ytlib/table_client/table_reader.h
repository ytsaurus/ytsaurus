#pragma once

#include "public.h"
#include "sync_reader.h"
#include "async_reader.h"

#include <core/logging/tagged_logger.h>

#include <ytlib/ypath/rich.h>

#include <ytlib/transaction_client/public.h>
#include <ytlib/transaction_client/transaction_listener.h>

#include <ytlib/cypress_client/public.h>

#include <ytlib/object_client/object_service_proxy.h>

#include <ytlib/table_client/table_ypath_proxy.h>

#include <ytlib/chunk_client/public.h>

#include <ytlib/node_tracker_client/public.h>

namespace NYT {
namespace NTableClient {

////////////////////////////////////////////////////////////////////////////////

class TAsyncTableReader
    : public NTransactionClient::TTransactionListener
    , public IAsyncReader
{
public:
    TAsyncTableReader(
        TTableReaderConfigPtr config,
        NRpc::IChannelPtr masterChannel,
        NTransactionClient::ITransactionPtr transaction,
        NChunkClient::IBlockCachePtr blockCache,
        const NYPath::TRichYPath& richPath);

    virtual void Open();

    virtual bool FetchNextItem() override;
    virtual TAsyncError GetReadyEvent() override;

    virtual bool IsValid() const override;
    virtual const TRow& GetRow() const override;
    virtual int GetTableIndex() const override;

    virtual i64 GetSessionRowIndex() const override;
    virtual i64 GetSessionRowCount() const override;
    virtual i64 GetTableRowIndex() const override;
    
    virtual NChunkClient::NProto::TDataStatistics GetDataStatistics() const override;
    virtual std::vector<NChunkClient::TChunkId> GetFailedChunkIds() const override;

private:
    TTableReaderConfigPtr Config;
    NRpc::IChannelPtr MasterChannel;
    NTransactionClient::ITransactionPtr Transaction;
    NTransactionClient::TTransactionId TransactionId;
    NChunkClient::IBlockCachePtr BlockCache;
    NNodeTrackerClient::TNodeDirectoryPtr NodeDirectory;
    NYPath::TRichYPath RichPath;
    bool IsOpen;
    bool IsReadStarted_;
    NObjectClient::TObjectServiceProxy Proxy;
    NLog::TTaggedLogger Logger;

    TTableChunkSequenceReaderPtr Reader;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
