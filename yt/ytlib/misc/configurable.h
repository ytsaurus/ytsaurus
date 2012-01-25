#pragma once

#include "mpl.h"
#include "property.h"

#include <ytlib/actions/action.h>
#include <ytlib/actions/action_util.h>
#include <ytlib/ytree/ytree.h>
#include <ytlib/ytree/ypath_detail.h>
#include <ytlib/ytree/yson_consumer.h>

namespace NYT {
namespace NConfig {

////////////////////////////////////////////////////////////////////////////////

struct IParameter
    : public TRefCounted
{
    typedef TIntrusivePtr<IParameter> TPtr;

    // node can be NULL
    virtual void Load(const NYTree::INode* node, const NYTree::TYPath& path) = 0;
    virtual void Validate(const NYTree::TYPath& path) const = 0;
    virtual void Save(NYTree::IYsonConsumer* consumer) const = 0;
    virtual bool IsPresent() const = 0;
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

    virtual void Load(const NYTree::INode* node, const NYTree::TYPath& path);
    virtual void Validate(const NYTree::TYPath& path) const;
    virtual void Save(NYTree::IYsonConsumer *consumer) const;
    virtual bool IsPresent() const;

public: // for users
    TParameter& Default(const T& defaultValue = T());
    TParameter& Default(T&& defaultValue);
    TParameter& DefaultNew();
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

class TConfigurable
    : public TRefCounted
{
public:
    typedef TIntrusivePtr<TConfigurable> TPtr;

    TConfigurable();

    void LoadAndValidate(const NYTree::INode* node, const NYTree::TYPath& path = NYTree::RootMarker);
    virtual void Load(const NYTree::INode* node, const NYTree::TYPath& path = NYTree::RootMarker);
    void Validate(const NYTree::TYPath& path = NYTree::RootMarker) const;

    void Save(NYTree::IYsonConsumer* consumer) const;

    DEFINE_BYVAL_RO_PROPERTY(NYTree::IMapNode::TPtr, Options);

protected:
    virtual void DoValidate() const;

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

typedef NConfig::TConfigurable TConfigurable;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

#define CONFIGURABLE_INL_H_
#include "configurable-inl.h"
#undef CONFIGURABLE_INL_H_
