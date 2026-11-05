#pragma once

#include <yt/yt/core/ytree/public.h>
#include <yt/yt/core/ytree/node.h>

#include <Common/JSONParsers/ElementTypes.h>
#include <Core/Types.h>

namespace NYT::NClickHouseServer {

////////////////////////////////////////////////////////////////////////////////

//! This class can be used as an argument for the template class TFunctionYson.
//! It provides the ability to parse YSONs using the JSON parser's interface.
struct TYsonParserAdapter
{
    class Array;
    class Object;

    //! References an element in a YSON document, representing a boolean, string, number, map or list.
    class Element
    {
    public:
        Element() = default;
        Element(const NYTree::INodePtr& node) // Intentionally implicit.
            : Node_(node)
        { }

        NYTree::INodePtr GetNode() const { return Node_; }

        DB::ElementType type() const
        {
            switch (Node_->GetType()) {
                case NYTree::ENodeType::String:
                    return DB::ElementType::STRING;
                case NYTree::ENodeType::Int64:
                    return DB::ElementType::INT64;
                case NYTree::ENodeType::Uint64:
                    return DB::ElementType::UINT64;
                case NYTree::ENodeType::Double:
                    return DB::ElementType::DOUBLE;
                case NYTree::ENodeType::Boolean:
                    return DB::ElementType::BOOL;
                case NYTree::ENodeType::Map:
                    return DB::ElementType::OBJECT;
                case NYTree::ENodeType::List:
                    return DB::ElementType::ARRAY;
                case NYTree::ENodeType::Entity:
                    return DB::ElementType::NULL_VALUE;
                case NYTree::ENodeType::Composite:
                    // Should never appear during YSON parsing.
                    YT_ABORT();
            }
        }

        bool isInt64() const { return Node_->GetType() == NYTree::ENodeType::Int64; }
        bool isUInt64() const { return Node_->GetType() == NYTree::ENodeType::Uint64; }
        bool isDouble() const { return Node_->GetType() == NYTree::ENodeType::Double; }
        bool isString() const { return Node_->GetType() == NYTree::ENodeType::String; }
        bool isArray() const { return Node_->GetType() == NYTree::ENodeType::List; }
        bool isObject() const { return Node_->GetType() == NYTree::ENodeType::Map; }
        bool isBool() const { return Node_->GetType() == NYTree::ENodeType::Boolean; }
        bool isNull() const { return Node_->GetType() == NYTree::ENodeType::Entity; }

        Int64 getInt64() const { return Node_->AsInt64()->GetValue(); }
        UInt64 getUInt64() const { return Node_->AsUint64()->GetValue(); }
        double getDouble() const { return Node_->AsDouble()->GetValue(); }
        bool getBool() const { return Node_->AsBoolean()->GetValue(); }
        std::string_view getString() const;
        Array getArray() const;
        Object getObject() const;

    private:
        NYTree::INodePtr Node_;
    };

    //! References a list in a YSON document.
    class Array
    {
    public:
        class Iterator
        {
        public:
            Iterator(const NYTree::IListNodePtr& listNode, size_t index)
                : ListNode_(listNode)
                , Index_(index)
            { }

            Element operator*() const { return ListNode_->FindChild(Index_); }
            Iterator& operator++() { ++Index_; return *this; }
            Iterator operator++(int) { auto result = *this; ++Index_; return result; }
            friend bool operator==(const Iterator& lhs, const Iterator& rhs)
            {
                return lhs.Index_ == rhs.Index_ && lhs.ListNode_ == rhs.ListNode_;
            }

        private:
            NYTree::IListNodePtr ListNode_;
            size_t Index_ = 0;
        };

        explicit Array(const NYTree::IListNodePtr& listNode) : ListNode_(listNode) { }
        Iterator begin() const { return {ListNode_, 0}; }
        Iterator end() const { return {ListNode_, size()}; }
        size_t size() const { return ListNode_->GetChildCount(); }
        Element operator[](size_t index) const { return ListNode_->FindChild(index); }

    private:
        NYTree::IListNodePtr ListNode_;
    };

    using KeyValuePair = std::pair<std::string_view, NYTree::INodePtr>;

    //! References a map in a YSON document.
    class Object
    {
    public:
        class Iterator
        {
        public:
            Iterator(
                const std::shared_ptr<std::vector<std::pair<std::string, NYTree::INodePtr>>>& keyValuePairs,
                size_t index)
                : KeyValuePairs_(keyValuePairs)
                , Index_(index)
            { }

            KeyValuePair operator*() const { return (*KeyValuePairs_)[Index_]; }
            Iterator& operator++() { ++Index_; return *this; }
            Iterator operator++(int) { auto result = *this; ++Index_; return result; }
            friend bool operator==(const Iterator& lhs, const Iterator& rhs)
            {
                return lhs.Index_ == rhs.Index_ && lhs.KeyValuePairs_ == rhs.KeyValuePairs_;
            }

        private:
            // Children of the parent's map node.
            // We store them here to lock the order and to return std::string_view on keys in some methods.
            std::shared_ptr<std::vector<std::pair<std::string, NYTree::INodePtr>>> KeyValuePairs_ = nullptr;
            size_t Index_ = 0;
        };

        explicit Object(const NYTree::IMapNodePtr& mapNode) : MapNode_(mapNode) { }
        Iterator begin() const;
        Iterator end() const;
        size_t size() const { return MapNode_->GetChildCount(); }
        bool find(const std::string_view& key, Element& result) const;

        // NB: We do not support get by index for objects since the order in IMapNode is arbitrary.

    private:
        std::shared_ptr<std::vector<std::pair<std::string, NYTree::INodePtr>>> GetKeyValuePairs() const;
        NYTree::IMapNodePtr MapNode_;
        mutable std::shared_ptr<std::vector<std::pair<std::string, NYTree::INodePtr>>> KeyValuePairs_ = nullptr;
    };

    //! Parses a YSON document, returns the reference to its root element if succeeded.
    bool parse(const std::string_view& yson, Element& result);

private:
    NYTree::INodePtr Root_;
};

inline std::string_view TYsonParserAdapter::Element::getString() const
{
    // GetValue() returns const TString&, so it's ok to provide std::string_view outside.
    const auto& result = Node_->AsString()->GetValue();
    return {result.data(), result.size()};
}

inline TYsonParserAdapter::Array TYsonParserAdapter::Element::getArray() const
{
    return Array(Node_->AsList());
}

inline TYsonParserAdapter::Object TYsonParserAdapter::Element::getObject() const
{
    return Object(Node_->AsMap());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
