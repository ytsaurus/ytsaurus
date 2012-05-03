#include "stdafx.h"
#include "table_commands.h"
#include "config.h"

#include <ytlib/ytree/yson_parser.h>
#include <ytlib/ytree/tree_visitor.h>

#include <ytlib/table_client/table_reader.h>
#include <ytlib/table_client/table_writer.h>
#include <ytlib/table_client/table_consumer.h>
#include <ytlib/table_client/table_producer.h>

namespace NYT {
namespace NDriver {

using namespace NYTree;
using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

TCommandDescriptor TReadCommand::GetDescriptor()
{
    return TCommandDescriptor(EDataType::Null, EDataType::Table);
}

void TReadCommand::DoExecute(TReadRequestPtr request)
{
    auto stream = Host->GetOutputStream();

    auto reader = New<TTableReader>(
        Host->GetConfig()->ChunkSequenceReader,
        Host->GetMasterChannel(),
        Host->GetTransaction(request),
        Host->GetBlockCache(),
        request->Path);
    reader->Open();

    TYsonWriter writer(
        stream,
        Host->GetConfig()->OutputFormat,
        EYsonType::ListFragment,
        Host->GetConfig()->OutputFormat != EYsonFormat::Binary);
    ProduceYson(reader, &writer);
}

////////////////////////////////////////////////////////////////////////////////

TCommandDescriptor TWriteCommand::GetDescriptor()
{
    return TCommandDescriptor(EDataType::Table, EDataType::Null);
}

void TWriteCommand::DoExecute(TWriteRequestPtr request)
{
    auto writer = New<TTableWriter>(
        Host->GetConfig()->ChunkSequenceWriter,
        Host->GetMasterChannel(),
        Host->GetTransaction(request),
        Host->GetTransactionManager(),
        request->Path,
        Null);

    //ToDo: write sorted data.
    writer->Open();
    TTableConsumer consumer(writer);

    if (request->Value) {
        auto value = request->Value;
        switch (value->GetType()) {
            case ENodeType::List: {
                FOREACH (const auto& child, value->AsList()->GetChildren()) {
                    VisitTree(~child, &consumer);
                }
                break;
            }

            case ENodeType::Map: {
                VisitTree(~value, &consumer);
                break;
            }

            default:
                YUNREACHABLE();
        }
    } else {
        auto stream = Host->GetInputStream();
        ParseYson(stream, &consumer, EYsonType::ListFragment);
    }

    writer->Close();
    Host->ReplySuccess();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NDriver
} // namespace NYT
