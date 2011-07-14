#pragma once

#include <dict/json/json.h>
#include <util/generic/vector.h>
#include <util/charset/wide.h>
#include <util/string/printf.h>

namespace NYT {

// TOOD: code review pending

////////////////////////////////////////////////////////////////////////////////
// JSON configs helper


//TODO: implement other read functions with defaultValue
inline bool TryRead(
    const TJsonObject* jsonConfig,
    const wchar_t* paramName,
    int* param)
{
    const TJsonObject* value = jsonConfig->Value(paramName);
    if (value != NULL) {
        *param = value->ToInt();
        return true;
    }
    return false;
}

inline bool TryRead(
    const TJsonObject* jsonConfig,
    const wchar_t* paramName,
    double* param)
{
    const TJsonObject* value = jsonConfig->Value(paramName);
    if (value != NULL) {
        *param = value->ToDouble();
        return true;
    }
    return false;
}

inline bool TryRead(
    const TJsonObject* jsonConfig,
    const wchar_t* paramName,
    bool* param)
{
    const TJsonObject* value = jsonConfig->Value(paramName);
    if (value != NULL) {
        *param = value->ToBoolean();
        return true;
    }
    return false;
}

inline bool TryRead(
    const TJsonObject* jsonConfig,
    const wchar_t* paramName,
    yvector<Stroka>* param)
{
    const TJsonObject* value = jsonConfig->Value(paramName);
    if (value != NULL) {
        const TJsonArray* array = static_cast<const TJsonArray*>(value);
        param->resize(array->Length());
        for (i32 i = 0; i < param->ysize(); ++i) {
            (*param)[i] = WideToChar(array->Item(i)->ToString());
        }
        return true;
    }
    return false;
}

template <class T>
inline void ReadEnum(
    const TJsonObject* jsonConfig,
    const wchar_t* paramName,
    T* param)
{
    Stroka value;
    if (TryRead(jsonConfig, paramName, &value)) {
        *param = T::FromString(value);
    } else {
        ythrow yexception() << Sprintf("Error reading enumeration %s", paramName);
    }
}

template <class T>
inline bool ReadEnum(
    const TJsonObject* jsonConfig,
    const wchar_t* paramName,
    T* param,
    T defaultValue)
{
    Stroka value;
    if (TryRead(jsonConfig, paramName, &value)) {
        *param = T::FromString(value);
        return true;
    } else {
        *param = defaultValue;
        return false;
    }
}

inline TJsonObject* GetSubTree(TJsonObject* object, Stroka rootPath) {
    VectorWtrok path;
    TJsonObject* root = object;
    SplitStroku(&path, CharToWide(~rootPath), (wchar16*) ".");
    for (int i = 0; i < path.ysize(); ++i) {
        if (root == NULL) {
            ythrow yexception() <<
                Sprintf("Couldn't procede path %s", ~WideToUTF8(path[i]));
        }
        root = root->Value(path[i]);
    }
    return root;
}

////////////////////////////////////////////////////////////////////////////////

inline bool TryRead(
    const TJsonObject* json,
    const wchar_t* name,
    Stroka* result)
{
    const TJsonObject* value = json->Value(name);
    if (value != NULL) {
        *result = WideToChar(value->ToString());
        return true;
    }
    return false;
}

inline Stroka Read(
    TJsonObject* json,
    const wchar_t* name)
{
    Stroka result;
    if (!TryRead(json, name, &result))
        ythrow yexception() << Sprintf("Error reading string %S", name);
    return result;
}

inline Stroka Read(
    TJsonObject* json,
    const wchar_t* name,
    Stroka defaultValue)
{
    Stroka result = defaultValue;
    TryRead(json, name, &result);
    return result;
}


////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
