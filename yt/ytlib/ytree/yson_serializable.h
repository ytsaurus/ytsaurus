#pragma once

#include "public.h"

#include <ytlib/misc/mpl.h>
#include <ytlib/misc/property.h>
#include <ytlib/misc/nullable.h>

#include <ytlib/actions/bind.h>
#include <ytlib/actions/callback.h>

namespace NYT {
namespace NConfig {

////////////////////////////////////////////////////////////////////////////////

struct IParameter
    : public TRefCounted
{
    // node can be NULL
    virtual void Load(NYTree::INodePtr node, const NYTree::TYPath& path) = 0;
    virtual void Validate(const NYTree::TYPath& path) const = 0;
    virtual void Save(NYTree::IYsonConsumer* consumer) const = 0;
    virtual bool IsPresent() const = 0;
};

typedef TIntrusivePtr<IParameter> IParameterPtr;

////////////////////////////////////////////////////////////////////////////////

template <class T>
class TParameter
    : public IParameter
{
public:
    /*!
     * \note Must throw exception for incorrect data
     */
    typedef TCallback<void(const T&)> TValidator;
    typedef typename TNullableTraits<T>::TValueType TValueType;

    explicit TParameter(T& parameter);

    virtual void Load(NYTree::INodePtr node, const NYTree::TYPath& path);
    virtual void Validate(const NYTree::TYPath& path) const;
    virtual void Save(NYTree::IYsonConsumer* consumer) const;
    virtual bool IsPresent() const;

public: // for users
    TParameter& Default(const T& defaultValue = T());
    TParameter& Default(T&& defaultValue);
    TParameter& DefaultNew();
    TParameter& CheckThat(TValidator validator);
    TParameter& GreaterThan(TValueType value);
    TParameter& GreaterThanOrEqual(TValueType value);
    TParameter& LessThan(TValueType value);
    TParameter& LessThanOrEqual(TValueType value);
    TParameter& InRange(TValueType lowerBound, TValueType upperBound);
    TParameter& NonEmpty();
    
private:
    T& Parameter;
    bool HasDefaultValue;
    std::vector<TValidator> Validators;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NConfig

////////////////////////////////////////////////////////////////////////////////

class TYsonSerializable
    : public TRefCounted
{
public:
    typedef TIntrusivePtr<TYsonSerializable> TPtr;

    TYsonSerializable();

    virtual void Load(NYTree::INodePtr node, bool validate = true, const NYTree::TYPath& path = "");
    void Validate(const NYTree::TYPath& path = "") const;

    void Save(NYTree::IYsonConsumer* consumer) const;

    DEFINE_BYVAL_RW_PROPERTY(bool, KeepOptions);
    NYTree::IMapNodePtr GetOptions() const;

protected:
    virtual void DoValidate() const;

    template <class T>
    NConfig::TParameter<T>& Register(const Stroka& parameterName, T& value);

private:
    template <class T>
    friend class TParameter;

    typedef yhash_map<Stroka, NConfig::IParameterPtr> TParameterMap;
    
    TParameterMap Parameters;
    NYTree::IMapNodePtr Options;
};

////////////////////////////////////////////////////////////////////////////////

template <class T>
TIntrusivePtr<T> CloneConfigurable(TIntrusivePtr<T> obj);

void Serialize(const TYsonSerializable& value, NYTree::IYsonConsumer* consumer);
void Deserialize(TYsonSerializable& value, NYTree::INodePtr node);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

#define YSON_SERIALIZABLE_INL_H_
#include "yson_serializable-inl.h"
#undef YSON_SERIALIZABLE_INL_H_
