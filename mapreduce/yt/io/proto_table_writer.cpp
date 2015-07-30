#include "proto_table_writer.h"

#include "node_table_writer.h"
#include "proxy_output.h"

#include <mapreduce/yt/common/node_builder.h>
#include <mapreduce/yt/interface/extension.pb.h>

namespace NYT {

using ::google::protobuf::FieldDescriptor;

namespace {

TNode MakeNodeFromMessage(const Message& row)
{
    TNode node;
    TNodeBuilder builder(&node);
    builder.OnBeginMap();
    auto* descriptor = row.GetDescriptor();
    auto* reflection = row.GetReflection();

    int count = descriptor->field_count();
    for (int i = 0; i < count; ++i) {
        auto* fieldDesc = descriptor->field(i);
        if (!reflection->HasField(row, fieldDesc)) {
            continue;
        }

        const auto& columnName = fieldDesc->options().GetExtension(column_name);
        if (columnName.empty()) {
            ythrow yexception() << "column_name extension must be set for field";
        }

        builder.OnKeyedItem(columnName);

        switch (fieldDesc->type()) {
            case FieldDescriptor::TYPE_STRING:
            case FieldDescriptor::TYPE_BYTES:
                builder.OnStringScalar(reflection->GetString(row, fieldDesc));
                break;
            case FieldDescriptor::TYPE_INT64:
                builder.OnInt64Scalar(reflection->GetInt64(row, fieldDesc));
                break;
            case FieldDescriptor::TYPE_UINT64:
                builder.OnUint64Scalar(reflection->GetUInt64(row, fieldDesc));
                break;
            case FieldDescriptor::TYPE_DOUBLE:
                builder.OnDoubleScalar(reflection->GetDouble(row, fieldDesc));
                break;
            case FieldDescriptor::TYPE_BOOL:
                builder.OnBooleanScalar(reflection->GetBool(row, fieldDesc));
                break;
            default:
                ythrow yexception() << "Invalid field type";
                break;
        }
    }
    builder.OnEndMap();
    return node;
}

}

////////////////////////////////////////////////////////////////////////////////

TProtoTableWriter::TProtoTableWriter(THolder<TProxyOutput> output)
    : NodeWriter_(new TNodeTableWriter(MoveArg(output)))
{ }

TProtoTableWriter::~TProtoTableWriter()
{ }

void TProtoTableWriter::AddRow(const Message& row, size_t tableIndex)
{
    NodeWriter_->AddRow(MakeNodeFromMessage(row), tableIndex);
}

void TProtoTableWriter::Finish()
{
    NodeWriter_->Finish();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
