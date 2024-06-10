#pragma once

#include <google/protobuf/descriptor.h>
#include <util/charset/unidata.h>

TString CalcColumnNameWithYtExtention(const google::protobuf::FieldDescriptor& fd);

static TString ToLower(const TString& name) {
    TString result;
    for (const auto c : name) {
        if (IsUpper(c)) {
            if (!result.empty()) {
                result += '_';
            }
            result += ToLower(c);
        } else {
            result += c;
        }
    }
    return result;
}

inline TString CalcColumnName(const google::protobuf::FieldDescriptor& fd, bool useYtExtention, bool useToLower = false) {
    const auto name = useYtExtention ? CalcColumnNameWithYtExtention(fd) : fd.name();
    return useToLower ? ToLower(name) : name;
}
