#include "stdafx.h"
#include "table_commands.h"

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

void TReadCommand::DoExecute(TReadRequestPtr request)
{
    auto stream = Host->CreateOutputStream();

    auto reader = New<TTableReader>(
        Host->GetConfig()->ChunkSequenceReader,
        Host->GetMasterChannel(),
        Host->GetTransaction(request),
        Host->GetBlockCache(),
        request->Path);

    TYsonWriter writer(
        stream.Get(),
        Host->GetConfig()->OutputFormat,
        EYsonType::Node,
        true);
    ProduceYson(reader, &writer);
}

////////////////////////////////////////////////////////////////////////////////

void TWriteCommand::DoExecute(TWriteRequestPtr request)
{
    auto writer = New<TTableWriter>(
        Host->GetConfig()->ChunkSequenceWriter,
        Host->GetMasterChannel(),
        Host->GetTransaction(request),
        Host->GetTransactionManager(),
        request->Path,
        Null);

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
        auto stream = Host->CreateInputStream();
        ParseYson(stream.Get(), &consumer, EYsonType::ListFragment);
    }

    writer->Close();
    Host->ReplySuccess();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NDriver
} // namespace NYT
