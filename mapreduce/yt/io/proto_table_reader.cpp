#include "proto_table_reader.h"

#include "node_table_reader.h"
#include "proxy_input.h"

#include <mapreduce/yt/interface/extension.pb.h>

namespace NYT {

using ::google::protobuf::FieldDescriptor;

namespace {

void ReadMessageFromNode(const TNode& node, Message* row)
{
    auto* descriptor = row->GetDescriptor();
    auto* reflection = row->GetReflection();

    int count = descriptor->field_count();
    for (int i = 0; i < count; ++i) {
        auto* fieldDesc = descriptor->field(i);

        Stroka columnName = fieldDesc->options().GetExtension(column_name);
        if (columnName.empty()) {
            const auto& keyColumnName = fieldDesc->options().GetExtension(key_column_name);
            if (keyColumnName.empty()) {
                continue; // cannot be read from table
            }
            columnName = keyColumnName;
        }

        const auto& nodeMap = node.AsMap();
        auto it = nodeMap.find(columnName);
        if (it == nodeMap.end()) {
            continue; // no such column
        }
        auto actualType = it->second.GetType();
        if (actualType == TNode::ENTITY) {
            continue; // null field
        }

        auto checkType = [&columnName] (TNode::EType expected, TNode::EType actual) {
            if (expected != actual) {
                ythrow TNode::TTypeError()
                    << Sprintf("expected node type %s, actual %s for node %s",
                        ~TNode::TypeToString(expected), ~TNode::TypeToString(actual), ~columnName);
            }
        };

        switch (fieldDesc->type()) {
            case FieldDescriptor::TYPE_STRING:
            case FieldDescriptor::TYPE_BYTES:
                checkType(TNode::STRING, actualType);
                reflection->SetString(row, fieldDesc, it->second.AsString());
                break;
            case FieldDescriptor::TYPE_INT64:
                checkType(TNode::INT64, actualType);
                reflection->SetInt64(row, fieldDesc, it->second.AsInt64());
                break;
            case FieldDescriptor::TYPE_INT32:
                checkType(TNode::INT64, actualType);
                reflection->SetInt32(row, fieldDesc, it->second.AsInt64());
                break;
            case FieldDescriptor::TYPE_UINT64:
                checkType(TNode::UINT64, actualType);
                reflection->SetUInt64(row, fieldDesc, it->second.AsUint64());
                break;
            case FieldDescriptor::TYPE_UINT32:
                checkType(TNode::UINT64, actualType);
                reflection->SetUInt32(row, fieldDesc, it->second.AsUint64());
                break;
            case FieldDescriptor::TYPE_DOUBLE:
                checkType(TNode::DOUBLE, actualType);
                reflection->SetDouble(row, fieldDesc, it->second.AsDouble());
                break;
            case FieldDescriptor::TYPE_BOOL:
                checkType(TNode::BOOL, actualType);
                reflection->SetBool(row, fieldDesc, it->second.AsBool());
                break;
            case FieldDescriptor::TYPE_MESSAGE:
            {
                checkType(TNode::STRING, actualType);
                google::protobuf::Message* message = reflection->MutableMessage(row, fieldDesc);
                if (!message->ParseFromArray(~it->second.AsString(), +it->second.AsString())) {
                    ythrow yexception() << "Failed to parse protobuf message";
                }
                break;
            }
            default:
                ythrow yexception() << "Incorrect protobuf type";
        }
    }
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

TProtoTableReader::TProtoTableReader(THolder<TProxyInput> input)
    : NodeReader_(new TNodeTableReader(std::move(input)))
{ }

TProtoTableReader::~TProtoTableReader()
{ }

void TProtoTableReader::ReadRow(Message* row)
{
    const auto& node = NodeReader_->GetRow();
    ReadMessageFromNode(node, row);
}

void TProtoTableReader::SkipRow()
{ }

bool TProtoTableReader::IsValid() const
{
    return NodeReader_->IsValid();
}

void TProtoTableReader::Next()
{
    NodeReader_->Next();
}

ui32 TProtoTableReader::GetTableIndex() const
{
    return NodeReader_->GetTableIndex();
}

ui64 TProtoTableReader::GetRowIndex() const
{
    return NodeReader_->GetRowIndex();
}

void TProtoTableReader::NextKey()
{
    NodeReader_->NextKey();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
