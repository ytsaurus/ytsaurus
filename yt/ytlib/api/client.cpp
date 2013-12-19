#include "stdafx.h"
#include "client.h"
#include "connection.h"

#include <core/concurrency/fiber.h>
#include <core/concurrency/parallel_collector.h>

#include <core/ytree/attribute_helpers.h>

#include <ytlib/transaction_client/transaction_manager.h>

#include <ytlib/tablet_client/protocol.h>

#include <ytlib/driver/dispatcher.h>

#include <ytlib/tablet_client/table_mount_cache.h>
#include <ytlib/tablet_client/tablet_service_proxy.h>

#include <ytlib/new_table_client/name_table.h>

#include <ytlib/hive/cell_directory.h>

namespace NYT {
namespace NApi {

using namespace NConcurrency;
using namespace NYPath;
using namespace NYTree;
using namespace NTransactionClient;
using namespace NVersionedTableClient;
using namespace NTabletClient;

////////////////////////////////////////////////////////////////////////////////

TTransactionStartOptions::TTransactionStartOptions()
    : Type(ETransactionType::Master)
    , AutoAbort(true)
    , Ping(true)
    , PingAncestors(false)
    , Attributes(CreateEphemeralAttributes())
{ }

////////////////////////////////////////////////////////////////////////////////

class TRowset;
typedef TIntrusivePtr<TRowset> TRowsetPtr;

class TTransaction;
typedef TIntrusivePtr<TTransaction> TTransactionPtr;

class TClient;
typedef TIntrusivePtr<TClient> TClientPtr;

////////////////////////////////////////////////////////////////////////////////

class TRowset
    : public IRowset
{
public:
    TRowset(std::unique_ptr<TProtocolReader> reader, std::vector<TUnversionedRow> rows)
        : Reader_(std::move(reader))
        , Rows_(std::move(rows))
    { }

    const std::vector<TUnversionedRow>& Rows() const
    {
        return Rows_;
    }

private:
    std::unique_ptr<TProtocolReader> Reader_;
    std::vector<TUnversionedRow> Rows_;

};

////////////////////////////////////////////////////////////////////////////////

class TClient
    : public IClient
{
public:
    explicit TClient(IConnectionPtr connection)
        : Connection_(std::move(connection))
        // TODO(babenko): consider using pool
        , Invoker_(NDriver::TDispatcher::Get()->GetLightInvoker())
    { }


    virtual TFuture<TErrorOr<ITransactionPtr>> StartTransaction(
        const TTransactionStartOptions& options) override;

    virtual TFuture<TErrorOr<IRowsetPtr>> Lookup(
        const TYPath& tablePath,
        TKey key,
        const TLookupOptions& options) override
    {
        return
            BIND(
                &TClient::DoLookup,
                MakeStrong(this),
                tablePath,
                key,
                options)
            .AsyncVia(Invoker_)
            .Run();
    }

private:
    friend class TTransaction;

    IConnectionPtr Connection_;

    IInvokerPtr Invoker_;


    TTableMountInfoPtr GetTableMountInfo(const TYPath& tablePath)
    {
        const auto& tableMountCache = Connection_->GetTableMountCache();
        // TODO(babenko): make async
        auto mountInfoOrError = tableMountCache->LookupInfo(tablePath).Get();
        THROW_ERROR_EXCEPTION_IF_FAILED(mountInfoOrError);
        return mountInfoOrError.GetValue();
    }

    static const TTabletInfo& GetTabletInfo(
        TTableMountInfoPtr mountInfo,
        const TYPath& tablePath,
        TKey key)
    {
        if (mountInfo->Tablets.empty()) {
            THROW_ERROR_EXCEPTION("Table %s is not mounted",
                ~tablePath);
        }
        const auto& tabletInfo = mountInfo->GetTablet(key);
        if (tabletInfo.State != ETabletState::Mounted) {
            THROW_ERROR_EXCEPTION("Tablet %s of table %s is not mounted",
                ~ToString(tabletInfo.TabletId),
                ~tablePath);
        }
        return tabletInfo;
    }


    TErrorOr<IRowsetPtr> DoLookup(
        const TYPath& tablePath,
        TKey key,
        const TLookupOptions& options)
    {
        try {
            auto mountInfo = GetTableMountInfo(tablePath);
            const auto& tabletInfo = GetTabletInfo(mountInfo, tablePath, key);

            const auto& cellDirectory = Connection_->GetCellDirectory();
            auto channel = cellDirectory->GetChannelOrThrow(tabletInfo.CellId);

            TTabletServiceProxy proxy(channel);
            auto req = proxy.Read();

            ToProto(req->mutable_tablet_id(), tabletInfo.TabletId);
            req->set_timestamp(options.Timestamp);

            TProtocolWriter writer;
            writer.WriteCommand(EProtocolCommand::LookupRow);
            writer.WriteUnversionedRow(key);
            writer.WriteColumnFilter(options.ColumnFilter);
            req->set_encoded_request(writer.Finish());

            auto rsp = WaitFor(req->Invoke());
            THROW_ERROR_EXCEPTION_IF_FAILED(*rsp);

            std::unique_ptr<TProtocolReader> reader(new TProtocolReader(rsp->encoded_response()));
            std::vector<TUnversionedRow> rows;
            reader->ReadUnversionedRowset(&rows);

            return TErrorOr<IRowsetPtr>(New<TRowset>(
                std::move(reader),
                std::move(rows)));
        } catch (const std::exception& ex) {
            return TError(ex);
        }
    }

};

IClientPtr CreateClient(IConnectionPtr connection)
{
    YCHECK(connection);

    return New<TClient>(std::move(connection));
}

////////////////////////////////////////////////////////////////////////////////

class TTransaction
    : public ITransaction
{
public:
    TTransaction(
        TClientPtr client,
        NTransactionClient::TTransactionPtr transaction)
        : Client_(std::move(client))
        , Transaction_(std::move(transaction))
        , TransactionStartCollector_(New<TParallelCollector<void>>())
    { }


    virtual const TTransactionId& GetId() override
    {
        return Transaction_->GetId();
    }

    virtual TTimestamp GetStartTimestamp() override
    {
        return Transaction_->GetStartTimestamp();
    }

    virtual TAsyncError Commit() override
    {
        return
            BIND(
                &TTransaction::DoCommit,
                MakeStrong(this))
            .AsyncVia(Client_->Invoker_)
            .Run();
    }

    virtual TAsyncError Abort() override
    {
        return Transaction_->Abort();
    }


    virtual void WriteRow(
        const TYPath& tablePath,
        TUnversionedRow row) override
    {
        WriteRows(
            tablePath,
            std::vector<TUnversionedRow>(1, row));
    }

    virtual void WriteRows(
        const TYPath& tablePath,
        std::vector<TUnversionedRow> rows) override
    {
        Requests_.push_back(std::unique_ptr<TRequestBase>(new TWriteRequest(
            this,
            tablePath,
            std::move(rows))));
    }


    virtual void DeleteRow(
        const TYPath& tablePath,
        TKey key) override
    {
        DeleteRows(
            tablePath,
            std::vector<TKey>(1, key));
    }

    virtual void DeleteRows(
        const TYPath& tablePath,
        std::vector<TKey> keys) override
    {
        Requests_.push_back(std::unique_ptr<TRequestBase>(new TDeleteRequest(
            this,
            tablePath,
            std::move(keys))));
    }


    virtual TFuture<TErrorOr<IRowsetPtr>> LookupRow(
        const TYPath& tablePath,
        TKey key,
        const TLookupOptions& options) override
    {
        TLookupOptions adjustedOptions;
        adjustedOptions.Timestamp = Transaction_->GetStartTimestamp();
        return Client_->Lookup(tablePath, key, adjustedOptions);
    }

private:
    TClientPtr Client_;
    NTransactionClient::TTransactionPtr Transaction_;

    class TRequestBase
    {
    public:
        ~TRequestBase()
        { }

        virtual void Run() = 0;

    protected:
        explicit TRequestBase(
            TTransaction* transaction,
            const TYPath& tablePath)
            : Transaction_(transaction)
            , TablePath_(tablePath)
        { }

        TTransaction* Transaction_;
        TYPath TablePath_;

    };

    class TWriteRequest
        : public TRequestBase
    {
    public:
        TWriteRequest(
            TTransaction* transaction,
            const TYPath& tablePath,
            std::vector<TUnversionedRow> rows)
            : TRequestBase(transaction, tablePath)
            , Rows_(std::move(rows))
        { }

        virtual void Run() override
        {
            const auto& mountInfo = Transaction_->Client_->GetTableMountInfo(TablePath_);
            for (auto row : Rows_) {
                const auto& tabletInfo = Transaction_->Client_->GetTabletInfo(mountInfo, TablePath_, row);
                auto* writer = Transaction_->AddTabletParticipant(tabletInfo);
                writer->WriteCommand(EProtocolCommand::WriteRow);
                writer->WriteUnversionedRow(row);
            }
        }

    private:
        std::vector<TUnversionedRow> Rows_;

    };

    class TDeleteRequest
        : public TRequestBase
    {
    public:
        TDeleteRequest(
            TTransaction* transaction,
            const TYPath& tablePath,
            std::vector<TKey> keys)
            : TRequestBase(transaction, tablePath)
            , Keys_(std::move(keys))
        { }

        virtual void Run() override
        {
            const auto& mountInfo = Transaction_->Client_->GetTableMountInfo(TablePath_);
            for (auto key : Keys_) {
                const auto& tabletInfo = Transaction_->Client_->GetTabletInfo(mountInfo, TablePath_, key);
                auto* writer = Transaction_->AddTabletParticipant(tabletInfo);
                writer->WriteCommand(EProtocolCommand::DeleteRow);
                writer->WriteUnversionedRow(key);
            }
        }

    private:
        std::vector<TUnversionedRow> Keys_;

    };

    std::vector<std::unique_ptr<TRequestBase>> Requests_;

    std::map<const TTabletInfo*, std::unique_ptr<TProtocolWriter>> TabletToWriter_;
    TIntrusivePtr<TParallelCollector<void>> TransactionStartCollector_;


    TProtocolWriter* AddTabletParticipant(const TTabletInfo& tabletInfo)
    {
        auto it = TabletToWriter_.find(&tabletInfo);
        if (it == TabletToWriter_.end()) {
            TransactionStartCollector_->Collect(Transaction_->AddTabletParticipant(tabletInfo.CellId));
            std::unique_ptr<TProtocolWriter> buffer(new TProtocolWriter());
            it = TabletToWriter_.insert(std::make_pair(&tabletInfo, std::move(buffer))).first;
        }
        return it->second.get();
    }

    TError DoCommit()
    {
        try {
            for (const auto& request : Requests_) {
                request->Run();
            }

            auto startResult = WaitFor(TransactionStartCollector_->Complete());
            THROW_ERROR_EXCEPTION_IF_FAILED(startResult);

            auto cellDirectory = Client_->Connection_->GetCellDirectory();

            auto writeCollector = New<TParallelCollector<void>>();

            for (const auto& pair : TabletToWriter_) {
                const auto& tabletInfo = *pair.first;
                auto* writer = pair.second.get();

                auto channel = cellDirectory->GetChannelOrThrow(tabletInfo.CellId);

                TTabletServiceProxy tabletProxy(channel);
                auto writeReq = tabletProxy.Write();
                ToProto(writeReq->mutable_transaction_id(), Transaction_->GetId());
                ToProto(writeReq->mutable_tablet_id(), tabletInfo.TabletId);
                writeReq->set_encoded_request(writer->Finish());

                writeCollector->Collect(
                    writeReq->Invoke().Apply(BIND([] (TTabletServiceProxy::TRspWritePtr rsp) {
                        return rsp->GetError();
                    })));
            }

            auto writeResult = WaitFor(writeCollector->Complete());
            THROW_ERROR_EXCEPTION_IF_FAILED(writeResult);

            auto commitResult = WaitFor(Transaction_->Commit());
            THROW_ERROR_EXCEPTION_IF_FAILED(commitResult);

            return TError();
        } catch (const std::exception& ex) {
            return ex;
        }
    }

};

TFuture<TErrorOr<ITransactionPtr>> TClient::StartTransaction(const TTransactionStartOptions& options)
{
    auto this_ = MakeStrong(this);
    auto transactionManager = Connection_->GetTransactionManager();
    return transactionManager->Start(options).Apply(
        BIND([=] (TErrorOr<NTransactionClient::TTransactionPtr> transactionOrError) -> TErrorOr<ITransactionPtr> {
            if (!transactionOrError.IsOK()) {
                return TError(transactionOrError);
            }
            return TErrorOr<ITransactionPtr>(New<TTransaction>(this_, transactionOrError.GetValue()));
        }));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NApi
} // namespace NYT

