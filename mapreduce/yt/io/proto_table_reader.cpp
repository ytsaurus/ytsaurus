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

        const auto& columnName = fieldDesc->options().GetExtension(column_name);
        if (columnName.empty()) {
            continue; // cannot be read from table
        }

        const auto& nodeMap = node.AsMap();
        auto it = nodeMap.find(columnName);
        if (it == nodeMap.end()) {
            continue; // no such column
        }

        auto checkType = [] (TNode::EType expected, TNode::EType actual) {
            if (expected != actual) {
                ythrow TNode::TTypeError()
                    << Sprintf("expected node type %s, actual %s",
                        ~TNode::TypeToString(expected), ~TNode::TypeToString(actual));
            }
        };

        auto actualType = it->second.GetType();
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
            case FieldDescriptor::TYPE_UINT64:
                checkType(TNode::UINT64, actualType);
                reflection->SetUInt64(row, fieldDesc, it->second.AsUint64());
                break;
            case FieldDescriptor::TYPE_DOUBLE:
                checkType(TNode::DOUBLE, actualType);
                reflection->SetDouble(row, fieldDesc, it->second.AsDouble());
                break;
            case FieldDescriptor::TYPE_BOOL:
                checkType(TNode::BOOL, actualType);
                reflection->SetBool(row, fieldDesc, it->second.AsBool());
                break;
            default:
                ythrow yexception() << "Incorrect protobuf type";
        }
    }
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

TProtoTableReader::TProtoTableReader(THolder<TProxyInput> input)
    : NodeReader_(new TNodeTableReader(MoveArg(input)))
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

size_t TProtoTableReader::GetTableIndex() const
{
    return NodeReader_->GetTableIndex();
}

void TProtoTableReader::NextKey()
{
    NodeReader_->NextKey();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
