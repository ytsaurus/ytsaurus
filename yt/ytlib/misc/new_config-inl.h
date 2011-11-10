#ifndef NEW_CONFIG_INL_H_
#error "Direct inclusion of this file is not allowed, include new_config.h"
#endif
#undef NEW_CONFIG_INL_H_

namespace NYT {
namespace NConfig {

////////////////////////////////////////////////////////////////////////////////

inline void Read(i64* parameter, NYTree::INode* node)
{
    *parameter = node->AsInt64()->GetValue();
}

inline void Read(i32* parameter, NYTree::INode* node)
{
    *parameter = static_cast<i32>(node->AsInt64()->GetValue());
}

inline void Read(ui32* parameter, NYTree::INode* node)
{
    *parameter = static_cast<ui32>(node->AsInt64()->GetValue());
}

inline void Read(double* parameter, NYTree::INode* node)
{
    *parameter = node->AsDouble()->GetValue();
}

inline void Read(Stroka* parameter, NYTree::INode* node)
{
    *parameter = node->AsString()->GetValue();
}

inline void Read(bool* parameter, NYTree::INode* node)
{
    Stroka value = node->AsString()->GetValue();
    if (value == "True") {
        *parameter = true;
    } else if (value == "False") {
        *parameter = false;
    } else {
        throw yexception()
            << "Could not load bool parameter (Value: "
            << (value.length() <= 10
                ? value
                : value.substr(0, 10) + "...")
            << ")";
    }
}

////////////////////////////////////////////////////////////////////////////////

template <class T>
TParameter<T, true>::TParameter(T* parameter)
    : Parameter(parameter)
{ }

template <class T>
void TParameter<T, true>::Load(NYTree::INode* node, Stroka path)
{
    Parameter->Load(node != NULL ? ~node->AsMap() : NULL, path);
}

template <class T>
void TParameter<T, true>::Validate(Stroka path) const
{
    Parameter->Validate(path);   
}

////////////////////////////////////////////////////////////////////////////////

template <class T>
TParameter<T, false>::TParameter(T* parameter)
    : Parameter(parameter)
    , HasDefaultValue(false)
{ }

template <class T>
void TParameter<T, false>::Load(NYTree::INode* node, Stroka path)
{
    if (node != NULL) {
        Read(Parameter, node);
    } else if (HasDefaultValue) {
        *Parameter = DefaultValue;
    } else {
        throw yexception()
            << "Required parameter is missing (Path: " << path << ")";
    }
}

template <class T>
void TParameter<T, false>::Validate(Stroka path) const
{
    FOREACH (auto validator, Validators) {
        try {
            validator->Do(*Parameter);
        } catch (const yexception& ex) {
            throw yexception()
                << Sprintf("Config validation failed (Path: %s, InnerException: %s)",
                    ~path, ex.what());
        }
    }
}

template <class T>
TParameter<T, false>& TParameter<T, false>::Default(const T& defaultValue)
{
    DefaultValue = defaultValue;
    HasDefaultValue = true;
    return *this;
}

template <class T>
TParameter<T, false>& TParameter<T, false>::Check(typename TValidator::TPtr validator)
{
    Validators.push_back(validator);
    return *this;
}

////////////////////////////////////////////////////////////////////////////////

TConfigBase::~TConfigBase()
{ }

void TConfigBase::Load(NYTree::IMapNode* node, Stroka prefix)
{
    FOREACH (auto pair, Parameters) {
        auto name = pair.First();
        Stroka childPath = prefix + "/" + name;
        auto child = node != NULL ? node->FindChild(name) : NULL;
        pair.Second()->Load(~child, childPath);
    }
}

void TConfigBase::Validate(Stroka prefix) const
{
    FOREACH (auto pair, Parameters) {
        pair.Second()->Validate(prefix + "/" + pair.First());
    }
}

template <class T>
TParameter<T>& TConfigBase::Register(Stroka parameterName, T& value)
{
    YASSERT(value != NULL);
    auto parameter = New< TParameter<T> >(value);
    YVERIFY(Parameters.insert(TPair<Stroka, IParameter::TPtr>(parameterName, parameter)).Second());
    return *parameter;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NConfig
} // namespace NYT
