#include "proto_table_writer.h"

#include "node_table_writer.h"
#include "proxy_output.h"

#include <mapreduce/yt/common/node_builder.h>
#include <mapreduce/yt/interface/extension.pb.h>

namespace NYT {

using ::google::protobuf::FieldDescriptor;

////////////////////////////////////////////////////////////////////////////////

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

        Stroka columnName = fieldDesc->options().GetExtension(column_name);
        if (columnName.empty()) {
            const auto& keyColumnName = fieldDesc->options().GetExtension(key_column_name);
            columnName = keyColumnName.empty() ? fieldDesc->name() : keyColumnName;
        }

        builder.OnKeyedItem(columnName);

        switch (fieldDesc->type()) {
            case FieldDescriptor::TYPE_STRING:
            case FieldDescriptor::TYPE_BYTES:
                builder.OnStringScalar(reflection->GetString(row, fieldDesc));
                break;
            case FieldDescriptor::TYPE_INT64:
            case FieldDescriptor::TYPE_SINT64:
            case FieldDescriptor::TYPE_SFIXED64:
                builder.OnInt64Scalar(reflection->GetInt64(row, fieldDesc));
                break;
            case FieldDescriptor::TYPE_INT32:
            case FieldDescriptor::TYPE_SINT32:
            case FieldDescriptor::TYPE_SFIXED32:
                builder.OnInt64Scalar(reflection->GetInt32(row, fieldDesc));
                break;
            case FieldDescriptor::TYPE_UINT64:
            case FieldDescriptor::TYPE_FIXED64:
                builder.OnUint64Scalar(reflection->GetUInt64(row, fieldDesc));
                break;
            case FieldDescriptor::TYPE_UINT32:
            case FieldDescriptor::TYPE_FIXED32:
                builder.OnUint64Scalar(reflection->GetUInt32(row, fieldDesc));
                break;
            case FieldDescriptor::TYPE_DOUBLE:
                builder.OnDoubleScalar(reflection->GetDouble(row, fieldDesc));
                break;
            case FieldDescriptor::TYPE_FLOAT:
                builder.OnDoubleScalar(reflection->GetFloat(row, fieldDesc));
                break;
            case FieldDescriptor::TYPE_BOOL:
                builder.OnBooleanScalar(reflection->GetBool(row, fieldDesc));
                break;
            case FieldDescriptor::TYPE_ENUM:
                builder.OnStringScalar(reflection->GetEnum(row, fieldDesc)->name());
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

} // namespace

////////////////////////////////////////////////////////////////////////////////

TProtoTableWriter::TProtoTableWriter(THolder<TProxyOutput> output)
    : NodeWriter_(new TNodeTableWriter(std::move(output)))
{ }

TProtoTableWriter::~TProtoTableWriter()
{ }

size_t TProtoTableWriter::GetStreamCount() const
{
    return NodeWriter_->GetStreamCount();
}

TOutputStream* TProtoTableWriter::GetStream(size_t tableIndex) const
{
    return NodeWriter_->GetStream(tableIndex);
}

void TProtoTableWriter::AddRow(const Message& row, size_t tableIndex)
{
    NodeWriter_->AddRow(MakeNodeFromMessage(row), tableIndex);
}

////////////////////////////////////////////////////////////////////////////////

TLenvalProtoTableWriter::TLenvalProtoTableWriter(THolder<TProxyOutput> output)
    : Output_(std::move(output))
{ }

TLenvalProtoTableWriter::~TLenvalProtoTableWriter()
{ }

size_t TLenvalProtoTableWriter::GetStreamCount() const
{
    return Output_->GetStreamCount();
}

TOutputStream* TLenvalProtoTableWriter::GetStream(size_t tableIndex) const
{
    return Output_->GetStream(tableIndex);
}

void TLenvalProtoTableWriter::AddRow(const Message& row, size_t tableIndex)
{
    auto* stream = GetStream(tableIndex);

    i32 size = row.ByteSize();
    stream->Write(&size, sizeof(size));
    row.SerializeToStream(stream);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
