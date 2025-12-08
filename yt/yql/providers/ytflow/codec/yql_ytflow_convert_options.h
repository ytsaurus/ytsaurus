#pragma once


namespace NYql::NYtflow::NCodec {

enum class EConvertDirection {
    Invalid = 0,
    YtToYql = 1,
    YqlToYt = 2,
};

class TConvertOptions {
public:
    TConvertOptions() = default;

    TConvertOptions& WithConvertDirection(EConvertDirection value);
    TConvertOptions& WithAllowExtraYtFields(bool value);
    TConvertOptions& WithAllowExtraYqlFields(bool value);

    EConvertDirection GetConvertDirection() const;
    bool GetAllowExtraYtFields() const;
    bool GetAllowExtraYqlFields() const;

private:
    EConvertDirection ConvertDirection = EConvertDirection::Invalid;
    bool AllowExtraYtFields = false;
    bool AllowExtraYqlFields = false;
};

} // namespace NYql::NYtflow::NCodec
