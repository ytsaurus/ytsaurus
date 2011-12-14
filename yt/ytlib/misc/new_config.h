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

    // node can be NULL
    virtual void Load(NYTree::INode* node, const NYTree::TYPath& path) = 0;
    virtual void Validate(const NYTree::TYPath& path) const = 0;
};

////////////////////////////////////////////////////////////////////////////////

template <class T>
class TParameter
    : public IParameter
{
public:
    /*!
     * \note Must throw exception for incorrect data
     */
    typedef IParamAction<const T&> TValidator;

    explicit TParameter(T& parameter);

    virtual void Load(NYTree::INode* node, const NYTree::TYPath& path);
    virtual void Validate(const NYTree::TYPath& path) const;
    
public: // for users
    TParameter& Default(const T& defaultValue = T());
    TParameter& Default(T&& defaultValue);
    TParameter& CheckThat(TValidator* validator);
    TParameter& GreaterThan(T value);
    TParameter& GreaterThanOrEqual(T value);
    TParameter& LessThan(T value);
    TParameter& LessThanOrEqual(T value);
    TParameter& InRange(T lowerBound, T upperBound);
    TParameter& NonEmpty();
    
private:
    T& Parameter;
    bool HasDefaultValue;
    yvector<typename TValidator::TPtr> Validators;
};

////////////////////////////////////////////////////////////////////////////////

class TConfigBase
    : public TRefCountedBase
{
public:
    typedef TIntrusivePtr<TConfigBase> TPtr;

    virtual void Load(NYTree::INode* node, const NYTree::TYPath& path = "");
    virtual void Validate(const NYTree::TYPath& path = "") const;

protected:
    template <class T>
    TParameter<T>& Register(const Stroka& parameterName, T& value);

private:
    template <class T>
    friend class TParameter;

    typedef yhash_map<Stroka, NConfig::IParameter::TPtr> TParameterMap;
    
    TParameterMap Parameters;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NConfig

////////////////////////////////////////////////////////////////////////////////

typedef NConfig::TConfigBase TConfigBase;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

#define NEW_CONFIG_INL_H_
#include "new_config-inl.h"
#undef NEW_CONFIG_INL_H_
