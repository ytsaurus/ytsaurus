#pragma once

#include "mpl.h"
#include "../actions/action.h"
#include "../actions/action_util.h"
#include "../ytree/ytree.h"

namespace NYT {
namespace NConfig {

////////////////////////////////////////////////////////////////////////////////

struct IParameter
    : public TRefCountedBase
{
    typedef TIntrusivePtr<IParameter> TPtr;

    virtual void Load(NYTree::INode* node, Stroka path) = 0;
    virtual void Validate(Stroka path) const = 0;
};

////////////////////////////////////////////////////////////////////////////////

class TConfigBase;

template <class T, bool TIsConfig = NYT::NDetail::TIsConvertible<T, TConfigBase>::Value>
class TParameter;

////////////////////////////////////////////////////////////////////////////////

template <class T>
class TParameter<T, true>
    : public IParameter
{
public:
    explicit TParameter(T* parameter);

    virtual void Load(NYTree::INode* node, Stroka path);
    virtual void Validate(Stroka path) const;

private:
    T* Parameter;
};

////////////////////////////////////////////////////////////////////////////////

template <class T>
class TParameter<T, false>
    : public IParameter
{
public:
    /*!
     * \note Must throw exception for incorrect data
     */
    typedef IParamAction<T> TValidator;

    explicit TParameter(T* parameter);

    virtual void Load(NYTree::INode* node, Stroka path);
    virtual void Validate(Stroka path) const;

public: // for users
    TParameter& Default(T defaultValue = T());
    TParameter& Check(typename TValidator::TPtr validator);
    TParameter& GreaterThan(T value);
    TParameter& GreaterThanOrEqual(T value);
    TParameter& LessThan(T value);
    TParameter& LessThanOrEqual(T value);
    TParameter& InRange(T lowerBound, T upperBound);
    TParameter& NonEmpty();
    
private:
    T* Parameter;
    bool HasDefaultValue;
    T DefaultValue;
    yvector<typename TValidator::TPtr> Validators;
};

////////////////////////////////////////////////////////////////////////////////

class TConfigBase
{
public:
    virtual ~TConfigBase();
    
    virtual void Load(NYTree::IMapNode* node, Stroka prefix = Stroka());
    virtual void Validate(Stroka prefix = Stroka()) const;

protected:
    template <class T>
    TParameter<T>& Register(Stroka parameterName, T& value);

private:
    typedef yhash_map<Stroka, IParameter::TPtr> ParameterMap;

    ParameterMap Parameters;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NConfig
} // namespace NYT

#define NEW_CONFIG_INL_H_
#include "new_config-inl.h"
#undef NEW_CONFIG_INL_H_
