#pragma once

#include <mapreduce/yt/interface/common.h>

#include <util/generic/string.h>
#include <util/generic/yexception.h>

struct TParseConfig {
    bool GoogleTimestampToInt64 = false;
    bool GoogleTimestampNanos = false;
    bool SkipEmptyOptionalFields = false;
    bool SkipEmptyRepeatedFields = false;
    bool UseYtExtention = false; // Only for column_name
    bool UseToLower = false;
    bool KeepFieldsWithoutExtension = true;
    bool ProtoMapFromList = false;
    bool ProtoMapToList = true;
    bool UseImplicitDefault = false;
    bool EnumCaseInsensitive = false;
    bool CastRobust = false;
    bool SaveEnumsAsString = false;

    NYT::TSortColumns KeyColumns = NYT::TSortColumns();

#define FUNCTION_SET_FLAG(Flag)                         \
    TParseConfig& Set##Flag(const bool value = true) {  \
        Flag = value;                                   \
        return *this;                                   \
    }

    FUNCTION_SET_FLAG(GoogleTimestampToInt64);
    FUNCTION_SET_FLAG(GoogleTimestampNanos);
    FUNCTION_SET_FLAG(SkipEmptyOptionalFields);
    FUNCTION_SET_FLAG(SkipEmptyRepeatedFields);
    FUNCTION_SET_FLAG(UseYtExtention);
    FUNCTION_SET_FLAG(UseToLower);
    FUNCTION_SET_FLAG(KeepFieldsWithoutExtension);
    FUNCTION_SET_FLAG(ProtoMapFromList);
    FUNCTION_SET_FLAG(ProtoMapToList);
    FUNCTION_SET_FLAG(UseImplicitDefault);
    FUNCTION_SET_FLAG(EnumCaseInsensitive);
    FUNCTION_SET_FLAG(CastRobust);
    FUNCTION_SET_FLAG(SaveEnumsAsString);

    TParseConfig& SetKeyColumns(const NYT::TSortColumns& keyColumns) {
        KeyColumns = keyColumns;
        return *this;
    }

    static const TString GOOGLE_PROTOBUF_TIMESTAMP_TYPE;
    static const i32 SECONDS_TO_NANOSECONDS = 1000000000;
};
