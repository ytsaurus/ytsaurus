#pragma once

#include "common.h"
#include "ytree.h"
#include "yson_events.h"

namespace NYT {
namespace NYTree {

////////////////////////////////////////////////////////////////////////////////

class TTreeParser
{
public:
    TTreeParser(IYsonEvents* events)
        : Events(events)
    { }

    void Parse(INode::TConstPtr root)
    {
        Events->BeginTree();
        ParseAny(root);
        Events->EndTree();
    }

private:
    IYsonEvents* Events;

    void ParseAny(INode::TConstPtr node)
    {
        switch (node->GetType()) {
            case ENodeType::String:
            case ENodeType::Int64:
            case ENodeType::Double:
            case ENodeType::Entity:
                ParseScalar(node);
                break;

            case ENodeType::List:
                ParseList(node->AsList());
                break;

            case ENodeType::Map:
                ParseMap(node->AsMap());
                break;

            default:
                YASSERT(false);
                break;
        }

        auto attributes = node->GetAttributes();
        if (~attributes != NULL) {
            ParseAttributes(attributes);
        }
    }

    void ParseScalar(INode::TConstPtr node)
    {
        switch (node->GetType()) {
            case ENodeType::String:
                Events->StringValue(node->GetValue<Stroka>());
                break;

            case ENodeType::Int64:
                Events->Int64Value(node->GetValue<i64>());
                break;

            case ENodeType::Double:
                Events->DoubleValue(node->GetValue<double>());
                break;

            case ENodeType::Entity:
                Events->EntityValue();
                break;

            default:
                YASSERT(false);
                break;
        }
    }

    void ParseList(IListNode::TConstPtr node)
    {
        Events->BeginList();
        for (int i = 0; i < node->GetChildCount(); ++i) {
            auto child = node->GetChild(i);
            Events->ListItem(i);
            Parse(child);
        }
        Events->EndList();
    }

    void ParseMap(IMapNode::TConstPtr node)
    {
        Events->BeginMap();
        auto childNames = node->GetChildNames();
        FOREACH(const Stroka& name, childNames) {
            auto child = node->GetChild(name);
            Events->MapItem(name);
            Parse(child);
        }
        Events->EndMap();
    }

    void ParseAttributes(IMapNode::TConstPtr node)
    {
        Events->BeginAttributes();
        auto childNames = node->GetChildNames();
        FOREACH(const Stroka& name, childNames) {
            auto child = node->GetChild(name);
            Events->AttributesItem(name);
            Parse(child);
        }
        Events->EndAttributes();
    }

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT
