#include "proto_table_reader.h"

#include "node_table_reader.h"
#include "proxy_input.h"

#include <mapreduce/yt/interface/extension.pb.h>

#include <util/string/escape.h>

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
            columnName = keyColumnName.empty() ? fieldDesc->name() : keyColumnName;
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
            case FieldDescriptor::TYPE_SINT64:
            case FieldDescriptor::TYPE_SFIXED64:
                checkType(TNode::INT64, actualType);
                reflection->SetInt64(row, fieldDesc, it->second.AsInt64());
                break;
            case FieldDescriptor::TYPE_INT32:
            case FieldDescriptor::TYPE_SINT32:
            case FieldDescriptor::TYPE_SFIXED32:
                checkType(TNode::INT64, actualType);
                reflection->SetInt32(row, fieldDesc, it->second.AsInt64());
                break;
            case FieldDescriptor::TYPE_UINT64:
            case FieldDescriptor::TYPE_FIXED64:
                checkType(TNode::UINT64, actualType);
                reflection->SetUInt64(row, fieldDesc, it->second.AsUint64());
                break;
            case FieldDescriptor::TYPE_UINT32:
            case FieldDescriptor::TYPE_FIXED32:
                checkType(TNode::UINT64, actualType);
                reflection->SetUInt32(row, fieldDesc, it->second.AsUint64());
                break;
            case FieldDescriptor::TYPE_DOUBLE:
                checkType(TNode::DOUBLE, actualType);
                reflection->SetDouble(row, fieldDesc, it->second.AsDouble());
                break;
            case FieldDescriptor::TYPE_FLOAT:
                checkType(TNode::DOUBLE, actualType);
                reflection->SetFloat(row, fieldDesc, it->second.AsDouble());
                break;
            case FieldDescriptor::TYPE_BOOL:
                checkType(TNode::BOOL, actualType);
                reflection->SetBool(row, fieldDesc, it->second.AsBool());
                break;
            case FieldDescriptor::TYPE_ENUM: {
                checkType(TNode::STRING, actualType);
                const auto& v = it->second.AsString();
                if (const auto* const p = fieldDesc->enum_type()->FindValueByName(v)) {
                    reflection->SetEnum(row, fieldDesc, p);
                } else {
                    ythrow yexception() << "Failed to parse \"" << EscapeC(v)
                                        << "\" as " << fieldDesc->enum_type()->full_name();
                }
                break;
            }
            case FieldDescriptor::TYPE_MESSAGE: {
                checkType(TNode::STRING, actualType);
                Message* message = reflection->MutableMessage(row, fieldDesc);
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

TLenvalProtoTableReader::TLenvalProtoTableReader(THolder<TProxyInput> input)
    : TLenvalTableReader(std::move(input))
{
    TLenvalTableReader::Next();
}

TLenvalProtoTableReader::~TLenvalProtoTableReader()
{ }

void TLenvalProtoTableReader::ReadRow(Message* row)
{
    TLengthLimitedInput stream(Input_.Get(), Length_);
    row->ParseFromStream(&stream);
    RowTaken_ = true;
}

bool TLenvalProtoTableReader::IsValid() const
{
    return TLenvalTableReader::IsValid();
}

void TLenvalProtoTableReader::Next()
{
    if (!RowTaken_) {
        Input_->Skip(Length_);
    }
    RowTaken_ = false;
    TLenvalTableReader::Next();
}

ui32 TLenvalProtoTableReader::GetTableIndex() const
{
    return TLenvalTableReader::GetTableIndex();
}

ui64 TLenvalProtoTableReader::GetRowIndex() const
{
    return TLenvalTableReader::GetRowIndex();
}

void TLenvalProtoTableReader::NextKey()
{
    TLenvalTableReader::NextKey();
    RowTaken_ = true;
}

void TLenvalProtoTableReader::OnRowStart()
{
    AtStart_ = false;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
