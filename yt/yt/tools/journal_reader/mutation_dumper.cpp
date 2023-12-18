#include "mutation_dumper.h"

#include "private.h"

#include <yt/yt/server/lib/tablet_node/proto/tablet_manager.pb.h>

#include <yt/yt/server/lib/transaction_supervisor/proto/transaction_supervisor.pb.h>

#include <yt/yt/client/table_client/wire_protocol.h>
#include <yt/yt/client/table_client/name_table.h>

#include <yt/yt/client/formats/config.h>

#include <yt/yt/client/transaction_client/public.h>

#include <yt/yt/library/formats/format.h>
#include <yt/yt/library/formats/unversioned_value_yson_writer.h>

#include <yt/yt/core/misc/protobuf_helpers.h>

#include <yt/yt/core/logging/log.h>

#include <yt/yt/core/yson/writer.h>

#include <yt/yt/core/ytree/fluent.h>

#include <yt/yt/core/compression/codec.h>

namespace NYT {

using namespace NTableClient;
using namespace NTabletClient;
using namespace NHydra::NProto;
using namespace NTabletNode::NProto;
using namespace NCompression;
using namespace NYTree;
using namespace NConcurrency;
using namespace NFormats;
using namespace NYson;
using namespace NTransactionClient;

////////////////////////////////////////////////////////////////////////////////

class TWriteRowsDumper
    : public IMutationDumper
{
public:
    TWriteRowsDumper(
        NTabletClient::TTabletId tabletId,
        NTableClient::TTableSchemaPtr schema)
        : TabletId_(tabletId)
        , Schema_(std::move(schema))
        , ValueWriter_(
            TNameTable::FromSchema(*Schema_),
            Schema_,
            {})
        , Consumer_(std::make_unique<TYsonWriter>(
            &Cout,
            EYsonFormat::Pretty,
            EYsonType::ListFragment))
    { }

    void Dump(const TMutationHeader& header, TSharedRef requestData) override
    {
        if (header.mutation_type() == "NYT.NTabletNode.NProto.TReqWriteRows") {
            OnWriteRows(header, requestData);
        } else if (header.mutation_type() == "NYT.NHiveServer.NProto.TReqParticipantCommitTransaction") {
            OnCommitTransaction(header, requestData);
        }
    }

    void OnCommitTransaction(const TMutationHeader& /*header*/, TSharedRef requestData)
    {
        NYT::NTransactionSupervisor::NProto::TReqParticipantCommitTransaction req;
        DeserializeProtoWithEnvelope(&req, requestData);

        Consumer_->OnListItem();
        BuildYsonFluently(Consumer_.get())
            .BeginAttributes()
                .Item("type").Value("commit_transaction")
            .EndAttributes()
            .BeginMap()
                .Item("commit_timestamp").Value(req.commit_timestamp())
                .Item("transaction_id").Value(FromProto<TTransactionId>(req.transaction_id()))
            .EndMap();
    }

    void OnWriteRows(const TMutationHeader& /*header*/, TSharedRef requestData)
    {
        NYT::NTabletNode::NProto::TReqWriteRows req;
        DeserializeProtoWithEnvelope(&req, requestData);

        if (!req.has_tablet_id()) {
            return;
        }

        if (FromProto<NTabletClient::TTabletId>(req.tablet_id()) != TabletId_) {
            return;
        }

        ECodec codecId;
        YT_VERIFY(TryEnumCast<ECodec>(req.codec(), &codecId));
        auto* codec = GetCodec(codecId);
        auto compressedRecordData = TSharedRef::FromString(req.compressed_data());
        auto recordData = codec->Decompress(compressedRecordData);
        auto reader = CreateWireProtocolReader(recordData);

        int rowCount = 0;
        auto transactionId = FromProto<TTransactionId>(req.transaction_id());
        while (!reader->IsFinished()) {
            auto command = reader->ReadCommand();
            if (command != EWireProtocolCommand::WriteRow) {
                YT_LOG_INFO("Unknown wire protocol command (Command: %v)",
                    command);
                return;
            }

            Consumer_->OnListItem();

            Consumer_->OnBeginAttributes();
            Consumer_->OnKeyedItem("transaction_id");
            Consumer_->OnStringScalar(ToString(transactionId));
            Consumer_->OnEndAttributes();

            auto row = reader->ReadUnversionedRow(false);
            WriteUnversionedRow(row);

            ++rowCount;
        }

        Consumer_->Flush();

        YT_LOG_DEBUG("TReqWriteRows dumped (RowCount: %v)",
            rowCount);
    }

private:
    NTabletClient::TTabletId TabletId_;
    NTableClient::TTableSchemaPtr Schema_;

    TUnversionedValueYsonWriter ValueWriter_;
    std::unique_ptr<IFlushableYsonConsumer> Consumer_;

    void WriteUnversionedRow(TUnversionedRow row)
    {
        Consumer_->OnBeginMap();
        for (auto value : row) {
            Consumer_->OnKeyedItem(Schema_->Columns()[value.Id].Name());
            ValueWriter_.WriteValue(value, Consumer_.get());
        }
        Consumer_->OnEndMap();
    }
};

////////////////////////////////////////////////////////////////////////////////

IMutationDumperPtr CreateWriteRowsDumper(TTabletId tabletId, TTableSchemaPtr schema)
{
    return New<TWriteRowsDumper>(tabletId, std::move(schema));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
