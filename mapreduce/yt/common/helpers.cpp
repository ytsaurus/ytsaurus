#include "helpers.h"

#include "config.h"
#include "node_builder.h"
#include "node_visitor.h"

#include <library/yson/parser.h>
#include <library/yson/writer.h>
#include <library/yson/json_writer.h>

#include <library/json/json_reader.h>
#include <library/json/json_value.h>

#include "serialize.h"
#include "fluent.h"

#include <util/stream/input.h>
#include <util/stream/output.h>
#include <util/stream/str.h>

namespace NYT {

static void WalkJsonTree(const NJson::TJsonValue& jsonValue, NJson::TJsonCallbacks* callbacks)
{
    using namespace NJson;
    switch (jsonValue.GetType()) {
        case JSON_NULL:
            callbacks->OnNull();
            return;
        case JSON_BOOLEAN:
            callbacks->OnBoolean(jsonValue.GetBoolean());
            return;
        case JSON_INTEGER:
            callbacks->OnInteger(jsonValue.GetInteger());
            return;
        case JSON_UINTEGER:
            callbacks->OnUInteger(jsonValue.GetUInteger());
            return;
        case JSON_DOUBLE:
            callbacks->OnDouble(jsonValue.GetDouble());
            return;
        case JSON_STRING:
            callbacks->OnString(jsonValue.GetString());
            return;
        case JSON_MAP:
            {
                callbacks->OnOpenMap();
                for (const auto& item : jsonValue.GetMap()) {
                    callbacks->OnMapKey(item.first);
                    WalkJsonTree(item.second, callbacks);
                }
                callbacks->OnCloseMap();
            }
            return;
        case JSON_ARRAY:
            {
                callbacks->OnOpenArray();
                for (const auto& item : jsonValue.GetArray()) {
                    WalkJsonTree(item, callbacks);
                }
                callbacks->OnCloseArray();
            }
            return;
        case JSON_UNDEFINED:
            ythrow yexception() << "cannot consume undefined json value";
            return;
    }
    Y_UNREACHABLE();
}

TNode CreateEmptyNodeByType(EYsonType type)
{
    TNode result;
    switch (type) {
        case YT_LIST_FRAGMENT:
            result = TNode::CreateList();
            break;
        case YT_MAP_FRAGMENT:
            result = TNode::CreateMap();
            break;
        default:
            break;
    }
    return result;
}

TNode NodeFromYsonString(const Stroka& input, EYsonType type)
{
    TStringInput stream(input);

    TNode result = CreateEmptyNodeByType(type);

    TNodeBuilder builder(&result);
    TYsonParser parser(&builder, &stream, type);
    parser.Parse();
    return result;
}

Stroka NodeToYsonString(const TNode& node, EYsonFormat format)
{
    TStringStream stream;
    TYsonWriter writer(&stream, format);
    TNodeVisitor visitor(&writer);
    visitor.Visit(node);
    return stream.Str();
}

TNode NodeFromJsonString(const Stroka& input, EYsonType type)
{
    TStringInput stream(input);

    TNode result = CreateEmptyNodeByType(type);

    TNodeBuilder builder(&result);
    TYson2JsonCallbacksAdapter callbacks(&builder, /*throwException*/ true);
    NJson::ReadJson(&stream, &callbacks);
    return result;
}

TNode NodeFromJsonValue(const NJson::TJsonValue& input)
{
    TNode result;
    TNodeBuilder builder(&result);
    TYson2JsonCallbacksAdapter callbacks(&builder, /*throwException*/ true);
    WalkJsonTree(input, &callbacks);
    return result;
}

Stroka NodeListToYsonString(const TNode::TList& nodes)
{
    TStringStream stream;
    TYsonWriter writer(&stream, YF_BINARY, YT_LIST_FRAGMENT);
    auto list = BuildYsonListFluently(&writer);
    for (const auto& node : nodes) {
        list.Item().Value(node);
    }
    return stream.Str();
}

TNode NodeFromYPath(const TRichYPath& path)
{
    return BuildYsonNodeFluently().BeginMap()
        .Item("path").Value(path)
    .EndMap();
}

Stroka AttributesToYsonString(const TNode& node)
{
    return BuildYsonStringFluently().BeginMap()
        .Item("attributes").Value(node)
    .EndMap();
}

Stroka AttributeFilterToYsonString(const TAttributeFilter& filter)
{
    return BuildYsonStringFluently().BeginMap()
        .Item("attributes").Value(filter)
    .EndMap();
}

TNode NodeFromTableSchema(const TTableSchema& schema)
{
    TNode result;
    TNodeBuilder builder(&result);
    Serialize(schema, &builder);
    return result;
}

void MergeNodes(TNode& dst, const TNode& src)
{
    if (dst.IsMap() && src.IsMap()) {
        auto& dstMap = dst.AsMap();
        const auto& srcMap = src.AsMap();
        for (const auto& srcItem : srcMap) {
            const auto& key = srcItem.first;
            auto dstItem = dstMap.find(key);
            if (dstItem != dstMap.end()) {
                MergeNodes(dstItem->second, srcItem.second);
            } else {
                dstMap[key] = srcItem.second;
            }
        }
    } else {
        if (dst.GetType() == src.GetType() && src.HasAttributes()) {
            auto attributes = dst.GetAttributes();
            MergeNodes(attributes, src.GetAttributes());
            dst = src;
            dst.Attributes() = attributes;
        } else {
            dst = src;
        }
    }
}

TYPath AddPathPrefix(const TYPath& path)
{
    if (path.StartsWith("//")) {
        return path;
    }
    return TConfig::Get()->Prefix + path;
}

Stroka GetWriteTableCommand()
{
    return TConfig::Get()->ApiVersion == "v2" ? "write" : "write_table";
}

Stroka GetReadTableCommand()
{
    return TConfig::Get()->ApiVersion == "v2" ? "read" : "read_table";
}

Stroka GetWriteFileCommand()
{
    return TConfig::Get()->ApiVersion == "v2" ? "upload" : "write_file";
}

Stroka GetReadFileCommand()
{
    return TConfig::Get()->ApiVersion == "v2" ? "download" : "read_file";
}

bool IsTrivial(const TReadLimit& readLimit)
{
    return !readLimit.Key_ && !readLimit.RowIndex_ && !readLimit.Offset_;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
