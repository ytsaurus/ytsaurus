#include "stdafx.h"
#include "table_commands.h"
#include "config.h"

#include <ytlib/formats/format.h>
#include <ytlib/ytree/yson_parser.h>
#include <ytlib/ytree/tree_visitor.h>

#include <ytlib/transaction_client/transaction_manager.h>
#include <ytlib/table_client/table_reader.h>
#include <ytlib/table_client/table_writer.h>
#include <ytlib/table_client/table_consumer.h>
#include <ytlib/table_client/table_producer.h>

namespace NYT {
namespace NDriver {

using namespace NYTree;
using namespace NFormats;
using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

void TReadCommand::DoExecute()
{
    auto reader = New<TTableReader>(
        Context->GetConfig()->ChunkSequenceReader,
        Context->GetMasterChannel(),
        GetTransaction(false),
        Context->GetBlockCache(),
        Request->Path);
    reader->Open();

    auto driverRequest = Context->GetRequest();
    auto writer = CreateConsumerForFormat(
        driverRequest->OutputFormat,
        EDataType::Tabular,
        driverRequest->OutputStream);
    ProduceYson(reader, ~writer);
}

//////////////////////////////////////////////////////////////////////////////////

void TWriteCommand::DoExecute()
{
    TNullable<TKeyColumns> keyColumns;
    if (Request->Sorted) {
        keyColumns.Assign(Request->KeyColumns);
    }

    auto writer = New<TTableWriter>(
        Context->GetConfig()->ChunkSequenceWriter,
        Context->GetMasterChannel(),
        GetTransaction(false),
        Context->GetTransactionManager(),
        Request->Path,
        keyColumns);

    writer->Open();

    TTableConsumer consumer(writer);

    if (Request->Value) {
        auto value = Request->Value;
        switch (value->GetType()) {
            case ENodeType::List: {
                FOREACH (const auto& child, value->AsList()->GetChildren()) {
                    VisitTree(child, &consumer);
                }
                break;
            }

            case ENodeType::Map: {
                VisitTree(value, &consumer);
                break;
            }

            default:
                YUNREACHABLE();
        }
    } else {
        auto driverRequest = Context->GetRequest();
        auto producer = CreateProducerForFormat(
            driverRequest->InputFormat, 
            EDataType::Tabular, 
            driverRequest->InputStream);
        producer.Run(&consumer);
    }

    writer->Close();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NDriver
} // namespace NYT
