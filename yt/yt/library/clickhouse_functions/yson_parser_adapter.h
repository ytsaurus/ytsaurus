#pragma once

#include <yt/yt/core/ytree/public.h>
#include <yt/yt/core/ytree/node.h>

#include <Core/Types.h>

namespace NYT::NClickHouseServer {

////////////////////////////////////////////////////////////////////////////////

/// This class can be used as an argument for the template class TFunctionYson.
/// It provides ability to parse YSONs using JSON parser's interface.
struct TYsonParserAdapter
{
    class Array;
    class Object;

    // References an element in an YSON document, representing a boolean, string, number, map or list.
    class Element
    {
    public:
        Element() {}
        Element(const NYTree::INodePtr & node) : Node_(node) {}
        NYTree::INodePtr GetNode() const { return Node_; }

        bool isInt64() const { return Node_->GetType() == NYTree::ENodeType::Int64; }
        bool isUInt64() const { return Node_->GetType() == NYTree::ENodeType::Uint64; }
        bool isDouble() const { return Node_->GetType() == NYTree::ENodeType::Double; }
        bool isString() const { return Node_->GetType() == NYTree::ENodeType::String; }
        bool isArray() const { return Node_->GetType() == NYTree::ENodeType::List; }
        bool isObject() const { return Node_->GetType() == NYTree::ENodeType::Map; }
        bool isBool() const { return Node_->GetType() == NYTree::ENodeType::Boolean; }
        bool isNull() const { return false; /* We do not support nulls. */ }

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

    // References a list in an YSON document.
    class Array
    {
    public:
        class Iterator
        {
        public:
            Iterator(const NYTree::IListNodePtr & list_node, size_t index) : ListNode_(list_node), Index_(index) {}
            Element operator *() const { return ListNode_->FindChild(Index_); }
            Iterator & operator ++() { ++Index_; return *this; }
            Iterator operator ++(int) { auto res = *this; ++Index_; return res; }
            friend bool operator ==(const Iterator & left, const Iterator & right) { return (left.Index_ == right.Index_) && (left.ListNode_ == right.ListNode_); }
            friend bool operator !=(const Iterator & left, const Iterator & right) { return !(left == right); }
        private:
            NYTree::IListNodePtr ListNode_;
            size_t Index_ = 0;
        };

        Array(const NYTree::IListNodePtr & list_node) : ListNode_(list_node) {}
        Iterator begin() const { return {ListNode_, 0}; }
        Iterator end() const { return {ListNode_, size()}; }
        size_t size() const { return ListNode_->GetChildCount(); }
        Element operator [](size_t index) const { return ListNode_->FindChild(index); }

    private:
        NYTree::IListNodePtr ListNode_;
    };

    using KeyValuePair = std::pair<std::string_view, Element>;

    // References a map in an YSON document.
    class Object
    {
    public:
        class Iterator
        {
        public:
            Iterator(const std::shared_ptr<std::vector<std::pair<TString, NYTree::INodePtr>>>& key_value_pairs, size_t index) : KeyValuePairs_(key_value_pairs), Index_(index) {}
            KeyValuePair operator *() const { return (*KeyValuePairs_)[Index_]; }
            Iterator & operator ++() { ++Index_; return *this; }
            Iterator operator ++(int) { auto res = *this; ++Index_; return res; }
            friend bool operator ==(const Iterator & left, const Iterator & right) { return (left.Index_ == right.Index_) && (left.KeyValuePairs_ == right.KeyValuePairs_); }
            friend bool operator !=(const Iterator & left, const Iterator & right) { return !(left == right); }
        private:
            // Children of the parent's map node.
            // We store it here to lock the order and to return std::string_view on keys in some methods.
            std::shared_ptr<std::vector<std::pair<TString, NYTree::INodePtr>>> KeyValuePairs_ = nullptr;
            size_t Index_ = 0;
        };

        Object(const NYTree::IMapNodePtr & map_node) : MapNode_(map_node) {}
        Iterator begin() const;
        Iterator end() const;
        size_t size() const { return MapNode_->GetChildCount(); }
        bool find(const std::string_view & key, Element & result) const;

#if 0
        // Optional: Provides access to an object's element by index.
        // We do not support get by index for objects since the order in IMapNode is arbitrary.
        KeyValuePair operator[](size_t) const { return {}; }
#endif

    private:
        std::shared_ptr<std::vector<std::pair<TString, NYTree::INodePtr>>> GetKeyValuePairs() const;
        NYTree::IMapNodePtr MapNode_;
        mutable std::shared_ptr<std::vector<std::pair<TString, NYTree::INodePtr>>> KeyValuePairs_ = nullptr;
    };

    // Parses an YSON document, returns the reference to its root element if succeeded.
    bool parse(const std::string_view & json, Element & result);

#if 0
    // Optional: Allocates memory to parse documents faster.
    void reserve(size_t max_size);
#endif

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
    return Node_->AsList();
}

inline TYsonParserAdapter::Object TYsonParserAdapter::Element::getObject() const
{
    return Node_->AsMap();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
