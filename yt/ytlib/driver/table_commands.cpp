#include "stdafx.h"
#include "table_commands.h"

#include <ytlib/ytree/yson_parser.h>
#include <ytlib/ytree/tree_visitor.h>

#include <ytlib/table_client/table_reader.h>
#include <ytlib/table_client/table_writer.h>
#include <ytlib/table_client/yson_table_input.h>
#include <ytlib/table_client/yson_row_consumer.h>

namespace NYT {
namespace NDriver {

using namespace NYTree;
using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

void TReadCommand::DoExecute(TReadRequest* request)
{
    auto stream = DriverImpl->CreateOutputStream();

    auto reader = New<TTableReader>(
        ~DriverImpl->GetConfig()->TableReader,
        DriverImpl->GetMasterChannel(),
        ~DriverImpl->GetTransaction(request),
        DriverImpl->GetBlockCache(),
        request->Path);

    TYsonTableInput input(
        ~reader, 
        DriverImpl->GetConfig()->OutputFormat, 
        stream.Get());

    while (input.ReadRow())
    { }
}

////////////////////////////////////////////////////////////////////////////////

void TWriteCommand::DoExecute(TWriteRequest* request)
{
    auto writer = New<TTableWriter>(
        ~DriverImpl->GetConfig()->TableWriter,
        DriverImpl->GetMasterChannel(),
        ~DriverImpl->GetTransaction(request),
        DriverImpl->GetTransactionManager(),
        request->Path);

    writer->Open();
    TRowConsumer consumer(~writer);

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
        auto stream = DriverImpl->CreateInputStream();
        ParseYson(stream.Get(), &consumer, TYsonParser::EMode::ListFragment);
    }

    writer->Close();
    DriverImpl->ReplySuccess();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NDriver
} // namespace NYT
