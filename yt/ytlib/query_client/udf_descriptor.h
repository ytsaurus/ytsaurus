#pragma once

#include "functions.h"

#include <core/ytree/yson_serializable.h>

namespace NYT {
namespace NQueryClient {

////////////////////////////////////////////////////////////////////////////////

extern const Stroka FunctionDescriptorAttribute;
extern const Stroka AggregateDescriptorAttribute;

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(ECallingConvention,
    (Simple)
    (UnversionedValue)
);

DEFINE_ENUM(ETypeCategory,
    ((TypeArgument) (TType::TagOf<TTypeArgument>()))
    ((UnionType)    (TType::TagOf<TUnionType>()))
    ((ConcreteType) (TType::TagOf<EValueType>()))
);

struct TDescriptorType
{
    TType Type = EValueType::Min;

    // NB(lukyan): For unknown reason Visual C++ does not create default constructor
    // for this class. Moreover it does not create it if TDescriptorType() = default is written
    TDescriptorType()
    { }
};

void Serialize(const TDescriptorType& value, NYson::IYsonConsumer* consumer);
void Deserialize(TDescriptorType& value, NYTree::INodePtr node);

////////////////////////////////////////////////////////////////////////////////

class TCypressFunctionDescriptor
    : public NYTree::TYsonSerializable
{
public:
    Stroka Name;
    std::vector<TDescriptorType> ArgumentTypes;
    TNullable<TDescriptorType> RepeatedArgumentType;
    TDescriptorType ResultType;
    ECallingConvention CallingConvention;

    TCypressFunctionDescriptor()
    {
        RegisterParameter("name", Name)
            .NonEmpty();
        RegisterParameter("argument_types", ArgumentTypes);
        RegisterParameter("result_type", ResultType);
        RegisterParameter("calling_convention", CallingConvention);
        RegisterParameter("repeated_argument_type", RepeatedArgumentType)
            .Default();
    }

    std::vector<TType> GetArgumentsTypes()
    {
        std::vector<TType> argumentTypes;
        for (const auto& type: ArgumentTypes) {
            argumentTypes.push_back(type.Type);
        }
        return argumentTypes;
    }
};

DECLARE_REFCOUNTED_CLASS(TCypressFunctionDescriptor)
DEFINE_REFCOUNTED_TYPE(TCypressFunctionDescriptor)

class TCypressAggregateDescriptor
    : public NYTree::TYsonSerializable
{
public:
    Stroka Name;
    TDescriptorType ArgumentType;
    TDescriptorType StateType;
    TDescriptorType ResultType;
    ECallingConvention CallingConvention;

    TCypressAggregateDescriptor()
    {
        RegisterParameter("name", Name)
            .NonEmpty();
        RegisterParameter("argument_type", ArgumentType);
        RegisterParameter("state_type", StateType);
        RegisterParameter("result_type", ResultType);
        RegisterParameter("calling_convention", CallingConvention);
    }
};

DECLARE_REFCOUNTED_CLASS(TCypressAggregateDescriptor)
DEFINE_REFCOUNTED_TYPE(TCypressAggregateDescriptor)

class TUdfDescriptor
    : public NYTree::TYsonSerializable
{
public:
    Stroka Name;
    TCypressFunctionDescriptorPtr FunctionDescriptor;
    TCypressAggregateDescriptorPtr AggregateDescriptor;

    TUdfDescriptor()
    {
        RegisterParameter("name", Name)
            .NonEmpty();
        RegisterParameter(FunctionDescriptorAttribute, FunctionDescriptor)
            .Default(nullptr);
        RegisterParameter(AggregateDescriptorAttribute, AggregateDescriptor)
            .Default(nullptr);
    }
};

DECLARE_REFCOUNTED_CLASS(TUdfDescriptor)
DEFINE_REFCOUNTED_TYPE(TUdfDescriptor)

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT
