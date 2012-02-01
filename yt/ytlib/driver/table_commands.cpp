#include "stdafx.h"
#include "table_commands.h"

#include <ytlib/ytree/yson_reader.h>
#include <ytlib/ytree/yson_writer.h>
#include <ytlib/ytree/tree_visitor.h>

#include <ytlib/table_client/table_reader.h>
#include <ytlib/table_client/table_writer.h>

namespace NYT {
namespace NDriver {

using namespace NYTree;
using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

void TReadCommand::DoExecute(TReadRequest* request)
{
    auto stream = DriverImpl->CreateOutputStream(ToStreamSpec(request->Stream));

    auto reader = New<TTableReader>(
        ~DriverImpl->GetConfig()->TableReader,
        DriverImpl->GetMasterChannel(),
        DriverImpl->GetCurrentTransaction(),
        DriverImpl->GetBlockCache(),
        request->Path);
    reader->Open();

    auto format = DriverImpl->GetConfig()->OutputFormat;

    while (reader->NextRow()) {
        TYsonWriter writer(~stream, format);
        writer.OnBeginMap();
        while (reader->NextColumn()) {
            writer.OnMapItem(reader->GetColumn());
            writer.OnStringScalar(reader->GetValue().ToString());
        }
        writer.OnEndMap();
        stream->Write('\n');
    }
}

////////////////////////////////////////////////////////////////////////////////

class TRowConsumer
    : public IYsonConsumer
{
public:
    TRowConsumer(TTableWriter* writer)
        : TableWriter(writer)
        , RowIndex(0)
        , InsideRow(false)
    { }

private:
    virtual void OnStringScalar(const Stroka& value, bool hasAttributes)
    {
        CheckNoAttributes(hasAttributes);
        CheckInsideRow();

        TableWriter->Write(Column, TValue(value));
    }

    virtual void OnInt64Scalar(i64 value, bool hasAttributes)
    {
        CheckNoAttributes(hasAttributes);
        CheckInsideRow();

        TableWriter->Write(Column, TValue(ToString(value)));
    }

    virtual void OnDoubleScalar(double value, bool hasAttributes)
    {
        CheckNoAttributes(hasAttributes);
        CheckInsideRow();

        TableWriter->Write(Column, TValue(ToString(value)));
    }

    virtual void OnEntity(bool hasAttributes)
    {
        UNUSED(hasAttributes);

        ythrow yexception() << Sprintf("Table value cannot be an entity (RowIndex: %d)", RowIndex);
    }

    virtual void OnBeginList()
    {
        ythrow yexception() << Sprintf("Table value cannot be a list (RowIndex: %d)", RowIndex);
    }

    virtual void OnListItem()
    {
        YUNREACHABLE();
    }

    virtual void OnEndList(bool hasAttributes)
    {
        UNUSED(hasAttributes);
        YUNREACHABLE();
    }

    virtual void OnBeginMap()
    {
        if (InsideRow) {
            ythrow yexception() << Sprintf("Table value cannot be a map (RowIndex: %d)", RowIndex);
        }
        InsideRow = true;
    }

    virtual void OnMapItem(const Stroka& name)
    {
        YASSERT(InsideRow);
        Column = name;
    }

    virtual void OnEndMap(bool hasAttributes)
    {
        CheckNoAttributes(hasAttributes);
        YASSERT(InsideRow);
        TableWriter->EndRow();
        InsideRow = false;
        ++RowIndex;
    }


    virtual void OnBeginAttributes() 
    {
        YUNREACHABLE();
    }

    virtual void OnAttributesItem(const Stroka& name)
    {
        UNUSED(name);
        YUNREACHABLE();
    }

    virtual void OnEndAttributes()
    {
        YUNREACHABLE();
    }


    void CheckNoAttributes(bool hasAttributes)
    {
        if (hasAttributes) {
            ythrow yexception() << Sprintf("Table value cannot have attributes (RowIndex: %d)", RowIndex);
        }
    }

    void CheckInsideRow()
    {
        if (!InsideRow) {
            ythrow yexception() << Sprintf("Invalid row format, map expected (RowIndex: %d)", RowIndex);
        }
    }


    TTableWriter::TPtr TableWriter;
    int RowIndex;
    bool InsideRow;
    TColumn Column;
};

void TWriteCommand::DoExecute(TWriteRequest* request)
{
    auto writer = New<TTableWriter>(
        ~DriverImpl->GetConfig()->TableWriter,
        DriverImpl->GetMasterChannel(),
        DriverImpl->GetCurrentTransaction(false),
        DriverImpl->GetTransactionManager(),
        // TODO: provide proper schema
        TSchema::Empty(),
        request->Path);

    writer->Open();

    TRowConsumer consumer(~writer);

    if (request->Value) {
        auto value = request->Value;
        switch (value->GetType()) {
            case ENodeType::List: {
                FOREACH (const auto& child, value->AsList()->GetChildren()) {
                    TTreeVisitor visitor(&consumer);
                    visitor.Visit(~child);
                }
                break;
            }

            case ENodeType::Map: {
                TTreeVisitor visitor(&consumer);
                visitor.Visit(~value);
                break;
            }

            default:
                YUNREACHABLE();
        }
    } else {
        auto stream = DriverImpl->CreateInputStream(ToStreamSpec(request->Stream));
        TYsonFragmentReader reader(&consumer, ~stream);
        while (reader.HasNext()) {
            reader.ReadNext();
        }
    }

    writer->Close();

    DriverImpl->ReplySuccess();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NDriver
} // namespace NYT
