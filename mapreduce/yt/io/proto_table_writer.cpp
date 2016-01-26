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

        Stroka columnName = fieldDesc->options().GetExtension(column_name);
        if (columnName.empty()) {
            const auto& keyColumnName = fieldDesc->options().GetExtension(key_column_name);
            if (keyColumnName.empty()) {
                ythrow yexception() << "column_name or key_column_name extension must be set for field " << fieldDesc->name();
            }
            columnName = keyColumnName;
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
            case FieldDescriptor::TYPE_INT32:
                builder.OnInt64Scalar(reflection->GetInt32(row, fieldDesc));
                break;
            case FieldDescriptor::TYPE_UINT64:
                builder.OnUint64Scalar(reflection->GetUInt64(row, fieldDesc));
                break;
            case FieldDescriptor::TYPE_UINT32:
                builder.OnUint64Scalar(reflection->GetUInt32(row, fieldDesc));
                break;
            case FieldDescriptor::TYPE_DOUBLE:
                builder.OnDoubleScalar(reflection->GetDouble(row, fieldDesc));
                break;
            case FieldDescriptor::TYPE_BOOL:
                builder.OnBooleanScalar(reflection->GetBool(row, fieldDesc));
                break;
            case FieldDescriptor::TYPE_MESSAGE:
                builder.OnStringScalar(reflection->GetMessage(row, fieldDesc).SerializeAsString());
                break;
            default:
                ythrow yexception() << "Invalid field type for column: " << columnName;
                break;
        }
    }
    builder.OnEndMap();
    return node;
}

}

////////////////////////////////////////////////////////////////////////////////

TProtoTableWriter::TProtoTableWriter(THolder<TProxyOutput> output)
    : NodeWriter_(new TNodeTableWriter(std::move(output)))
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
