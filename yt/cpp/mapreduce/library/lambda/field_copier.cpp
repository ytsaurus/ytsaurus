#include "field_copier.h"

#include <yt/yt_proto/yt/formats/extension.pb.h>

using namespace NProtoBuf;

// ===========================================================================
namespace NYT::NDetail {
// ===========================================================================
void CheckFieldCopierTypes(TStringBuf columnName, const FieldDescriptor* from, const FieldDescriptor* to) {
    if (to && from->type() != to->type()) {
        ythrow yexception() << "Protobuf messages "
            << from->containing_type()->full_name() << " and " << to->containing_type()->full_name()
            << " have different types of fields that match YT column '" << EscapeC(columnName) << "': "
            << from->containing_type()->full_name() << "::" << from->name() << " is " << from->type_name() << ", "
            << to->containing_type()->full_name() << "::" << to->name() << " is " << to->type_name();
    }

    if (to && from->type() == FieldDescriptor::TYPE_ENUM &&
        from->enum_type()->full_name() != to->enum_type()->full_name())
    {
        ythrow yexception() << "Protobuf messages "
            << from->containing_type()->full_name() << " and " << to->containing_type()->full_name()
            << " have different enum types of fields that match YT column '" << EscapeC(columnName) << "': "
            << from->containing_type()->full_name() << "::" << from->name() << " is " << from->enum_type()->full_name() << ", "
            << to->containing_type()->full_name() << "::" << to->name() << " is " << to->enum_type()->full_name();
    }

    auto checkType = [columnName](auto* desc) {
        if (desc->is_repeated()) {
            ythrow yexception() << "Sorry, TFieldCopier does not support repeated fields, but "
                << desc->containing_type()->full_name() << "::" << desc->name()
                << " that matches key column '" << EscapeC(columnName)
                << "' is repeated";
        }

        if (desc->cpp_type() == FieldDescriptor::CPPTYPE_MESSAGE) {
            ythrow yexception() << "Sorry, TFieldCopier does not support fields of Message type, but "
                << desc->containing_type()->full_name() << "::" << desc->name()
                << " that matches key column '" << EscapeC(columnName)
                << "' is of Message type";
        }
    };

    checkType(from);
    if (to) {
        checkType(to);
    }
}

// ===========================================================================
} // namespace NYT::NDetail
// ===========================================================================

// ===========================================================================
namespace NYT {
// ===========================================================================

const FieldDescriptor* DescriptorByColumn(
    const Descriptor* descriptor,
    TStringBuf columnName
) {
    int count = descriptor->field_count();
    for (int i = 0; i < count; ++i) {
        auto* fieldDesc = descriptor->field(i);

        TString curColumnName = fieldDesc->options().GetExtension(column_name);
        if (curColumnName.empty()) {
            const auto& keyColumnName = fieldDesc->options().GetExtension(key_column_name);
            curColumnName = keyColumnName.empty() ? fieldDesc->name() : keyColumnName;
        }
        if (columnName == curColumnName) {
            return fieldDesc;
        }
    }
    ythrow yexception() << "Protobuf message " << descriptor->full_name()
        << " does not have field for YT column '" << EscapeC(columnName) << "'";
}

// ===========================================================================
} // namespace NYT
// ===========================================================================
