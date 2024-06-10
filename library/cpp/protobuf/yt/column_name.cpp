#include "column_name.h"

#include <mapreduce/yt/interface/protos/extension.pb.h>

TString CalcColumnNameWithYtExtention(const google::protobuf::FieldDescriptor& field) {
    const auto& options = field.options();
    const auto columnName = options.GetExtension(NYT::column_name);
    if (!columnName.empty()) {
        return columnName;
    }
    const auto keyColumnName = options.GetExtension(NYT::key_column_name);
    if (!keyColumnName.empty()) {
        return keyColumnName;
    }
    return field.name();
}
