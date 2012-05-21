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

//TCommandDescriptor TReadCommand::GetDescriptor()
//{
//    return TCommandDescriptor(EDataType::Null, EDataType::Table);
//}

//void TReadCommand::DoExecute()
//{
//    auto stream = Context->GetOutputStream();

//    auto reader = New<TTableReader>(
//        Context->GetConfig()->ChunkSequenceReader,
//        Context->GetMasterChannel(),
//        Context->GetTransaction(request),
//        Context->GetBlockCache(),
//        request->Path);
//    reader->Open();

//    TYsonWriter writer(
//        stream,
//        Context->GetConfig()->OutputFormat,
//        EYsonType::ListFragment,
//        Context->GetConfig()->OutputFormat != EYsonFormat::Binary);
//    ProduceYson(reader, &writer);
//}

//////////////////////////////////////////////////////////////////////////////////

////TCommandDescriptor TWriteCommand::GetDescriptor()
////{
////    return TCommandDescriptor(EDataType::Table, EDataType::Null);
////}

//void TWriteCommand::DoExecute()
//{
//    TNullable<TKeyColumns> keyColumns;
//    if (request->Sorted) {
//        keyColumns.Assign(request->KeyColumns);
//    }

//    auto writer = New<TTableWriter>(
//        Context->GetConfig()->ChunkSequenceWriter,
//        Context->GetMasterChannel(),
//        Context->GetTransaction(request),
//        Context->GetTransactionManager(),
//        request->Path,
//        keyColumns);

//    writer->Open();
//    TTableConsumer consumer(writer);

//    if (request->Value) {
//        auto value = request->Value;
//        switch (value->GetType()) {
//            case ENodeType::List: {
//                FOREACH (const auto& child, value->AsList()->GetChildren()) {
//                    VisitTree(~child, &consumer);
//                }
//                break;
//            }

//            case ENodeType::Map: {
//                VisitTree(~value, &consumer);
//                break;
//            }

//            default:
//                YUNREACHABLE();
//        }
//    } else {
//        auto stream = Context->GetInputStream();
//        ParseYson(stream, &consumer, EYsonType::ListFragment);
//    }

//    writer->Close();
//    Context->ReplySuccess();
//}

////////////////////////////////////////////////////////////////////////////////

} // namespace NDriver
} // namespace NYT
