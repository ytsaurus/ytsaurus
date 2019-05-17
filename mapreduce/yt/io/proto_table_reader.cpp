#include "proto_table_reader.h"

#include "node_table_reader.h"

#include "proto_helpers.h"

#include <mapreduce/yt/interface/protos/extension.pb.h>

#include <util/string/escape.h>
#include <util/string/printf.h>

namespace NYT {

using ::google::protobuf::Descriptor;
using ::google::protobuf::FieldDescriptor;

void ReadMessageFromNode(const TNode& node, Message* row)
{
    auto* descriptor = row->GetDescriptor();
    auto* reflection = row->GetReflection();

    int count = descriptor->field_count();
    for (int i = 0; i < count; ++i) {
        auto* fieldDesc = descriptor->field(i);

        TString columnName = fieldDesc->options().GetExtension(column_name);
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
        if (actualType == TNode::Null) {
            continue; // null field
        }

        auto checkType = [&columnName] (TNode::EType expected, TNode::EType actual) {
            if (expected != actual) {
                ythrow TNode::TTypeError() << "expected node type " << expected
                    << ", actual " << actual << " for node " << columnName.data();
            }
        };

        switch (fieldDesc->type()) {
            case FieldDescriptor::TYPE_STRING:
            case FieldDescriptor::TYPE_BYTES:
                checkType(TNode::String, actualType);
                reflection->SetString(row, fieldDesc, it->second.AsString());
                break;
            case FieldDescriptor::TYPE_INT64:
            case FieldDescriptor::TYPE_SINT64:
            case FieldDescriptor::TYPE_SFIXED64:
                checkType(TNode::Int64, actualType);
                reflection->SetInt64(row, fieldDesc, it->second.AsInt64());
                break;
            case FieldDescriptor::TYPE_INT32:
            case FieldDescriptor::TYPE_SINT32:
            case FieldDescriptor::TYPE_SFIXED32:
                checkType(TNode::Int64, actualType);
                reflection->SetInt32(row, fieldDesc, it->second.AsInt64());
                break;
            case FieldDescriptor::TYPE_UINT64:
            case FieldDescriptor::TYPE_FIXED64:
                checkType(TNode::Uint64, actualType);
                reflection->SetUInt64(row, fieldDesc, it->second.AsUint64());
                break;
            case FieldDescriptor::TYPE_UINT32:
            case FieldDescriptor::TYPE_FIXED32:
                checkType(TNode::Uint64, actualType);
                reflection->SetUInt32(row, fieldDesc, it->second.AsUint64());
                break;
            case FieldDescriptor::TYPE_DOUBLE:
                checkType(TNode::Double, actualType);
                reflection->SetDouble(row, fieldDesc, it->second.AsDouble());
                break;
            case FieldDescriptor::TYPE_FLOAT:
                checkType(TNode::Double, actualType);
                reflection->SetFloat(row, fieldDesc, it->second.AsDouble());
                break;
            case FieldDescriptor::TYPE_BOOL:
                checkType(TNode::Bool, actualType);
                reflection->SetBool(row, fieldDesc, it->second.AsBool());
                break;
            case FieldDescriptor::TYPE_ENUM: {
                checkType(TNode::String, actualType);
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
                checkType(TNode::String, actualType);
                Message* message = reflection->MutableMessage(row, fieldDesc);
                if (!message->ParseFromArray(it->second.AsString().data(), it->second.AsString().size())) {
                    ythrow yexception() << "Failed to parse protobuf message";
                }
                break;
            }
            default:
                ythrow yexception() << "Incorrect protobuf type";
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

TProtoTableReader::TProtoTableReader(
    ::TIntrusivePtr<TRawTableReader> input,
    TVector<const Descriptor*>&& descriptors)
    : NodeReader_(new TNodeTableReader(std::move(input)))
    , Descriptors_(std::move(descriptors))
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

ui32 TProtoTableReader::GetRangeIndex() const
{
    return NodeReader_->GetRangeIndex();
}

ui64 TProtoTableReader::GetRowIndex() const
{
    return NodeReader_->GetRowIndex();
}

void TProtoTableReader::NextKey()
{
    NodeReader_->NextKey();
}

TMaybe<size_t> TProtoTableReader::GetReadByteCount() const
{
    return NodeReader_->GetReadByteCount();
}

////////////////////////////////////////////////////////////////////////////////

TLenvalProtoTableReader::TLenvalProtoTableReader(
    ::TIntrusivePtr<TRawTableReader> input,
    TVector<const Descriptor*>&& descriptors)
    : TLenvalTableReader(std::move(input))
    , Descriptors_(std::move(descriptors))
{ }

TLenvalProtoTableReader::~TLenvalProtoTableReader()
{ }

void TLenvalProtoTableReader::ReadRow(Message* row)
{
    ValidateProtoDescriptor(*row, GetTableIndex(), Descriptors_, true);

    while (true) {
        try {
            ParseFromStream(&Input_, *row, Length_);
            RowTaken_ = true;

            // We successfully parsed one more row from the stream,
            // so reset retry count to their initial value.
            Input_.ResetRetries();

            break;
        } catch (const yexception& ) {
            if (!TLenvalTableReader::Retry()) {
                throw;
            }
        }
    }
}

bool TLenvalProtoTableReader::IsValid() const
{
    return TLenvalTableReader::IsValid();
}

void TLenvalProtoTableReader::Next()
{
    TLenvalTableReader::Next();
}

ui32 TLenvalProtoTableReader::GetTableIndex() const
{
    return TLenvalTableReader::GetTableIndex();
}

ui32 TLenvalProtoTableReader::GetRangeIndex() const
{
    return TLenvalTableReader::GetRangeIndex();
}

ui64 TLenvalProtoTableReader::GetRowIndex() const
{
    return TLenvalTableReader::GetRowIndex();
}

void TLenvalProtoTableReader::NextKey()
{
    TLenvalTableReader::NextKey();
}

TMaybe<size_t> TLenvalProtoTableReader::GetReadByteCount() const
{
    return TLenvalTableReader::GetReadByteCount();
}

void TLenvalProtoTableReader::SkipRow()
{
    while (true) {
        try {
            size_t skipped = Input_.Skip(Length_);
            if (skipped != Length_) {
                ythrow yexception() << "Premature end of stream";
            }
            break;
        } catch (const yexception& ) {
            if (!TLenvalTableReader::Retry()) {
                throw;
            }
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
