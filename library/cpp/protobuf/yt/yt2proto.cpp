#include "yt2proto.h"

#include "column_name.h"

#include <mapreduce/yt/interface/node.h>

#include <google/protobuf/message.h>

using namespace NYT;
using namespace ::google::protobuf;

static const EnumValueDescriptor* FindEnumValue(
    const TNode& node,
    const FieldDescriptor* fd,
    const TParseConfig config) {
    if (node.IsString()) {
        const auto& v = node.AsString();
        if (!config.EnumCaseInsensitive) {
            return fd->enum_type()->FindValueByName(v);
        }
        for (int i = 0; i < fd->enum_type()->value_count(); ++i) {
            const auto* evd = fd->enum_type()->value(i);
            if (to_lower(evd->name()) == to_lower(v)) {
                return evd;
            }
        }
    } else {
        return fd->enum_type()->FindValueByNumber(config.CastRobust ? node.ConvertTo<i64>() : node.AsInt64());
    }
    return nullptr;
}

static void NodeToRepeatedField(
    const TNode& node,
    const FieldDescriptor* fd,
    Message& proto,
    const TParseConfig config) {
    if (node.GetType() == TNode::Null) {
        // default
        return;
    }

    auto list = node.AsList();
    auto r = proto.GetReflection();

    for (auto ni = list.begin(); ni != list.end(); ++ni) {
        switch (fd->cpp_type()) {
            case FieldDescriptor::CPPTYPE_INT32: {
                r->AddInt32(&proto, fd, config.CastRobust ? ni->ConvertTo<i32>() : ni->AsInt64());
                break;
            }
            case FieldDescriptor::CPPTYPE_INT64: {
                r->AddInt64(&proto, fd, config.CastRobust ? ni->ConvertTo<i64>() : ni->AsInt64());
                break;
            }
            case FieldDescriptor::CPPTYPE_UINT32: {
                r->AddUInt32(&proto, fd, config.CastRobust ? ni->ConvertTo<ui32>() : ni->AsUint64());
                break;
            }
            case FieldDescriptor::CPPTYPE_UINT64: {
                r->AddUInt64(&proto, fd, config.CastRobust ? ni->ConvertTo<ui64>() : ni->AsUint64());
                break;
            }
            case FieldDescriptor::CPPTYPE_DOUBLE: {
                r->AddDouble(&proto, fd, config.CastRobust ? ni->ConvertTo<double>() : ni->AsDouble());
                break;
            }
            case FieldDescriptor::CPPTYPE_FLOAT: {
                r->AddFloat(&proto, fd, config.CastRobust ? ni->ConvertTo<double>() : ni->AsDouble());
                break;
            }
            case FieldDescriptor::CPPTYPE_BOOL: {
                r->AddBool(&proto, fd, config.CastRobust ? ni->ConvertTo<bool>() : ni->AsBool());
                break;
            }
            case FieldDescriptor::CPPTYPE_STRING: {
                r->AddString(&proto, fd, config.CastRobust ? ni->ConvertTo<TString>() : ni->AsString());
                break;
            }
            case FieldDescriptor::CPPTYPE_ENUM: {
                const auto* vd = FindEnumValue(*ni, fd, config);
                Y_ENSURE(vd != nullptr, "Enum value is not found");
                r->AddEnum(&proto, fd, vd);
                break;
            }
            case FieldDescriptor::CPPTYPE_MESSAGE: {
                YtNodeToProto(*ni, *r->AddMessage(&proto, fd), config);
                break;
            }
        }
    }
}

static void NodeToSingleField(
    const TNode& node,
    const FieldDescriptor* fd,
    Message& proto,
    const TParseConfig config) {
    auto r = proto.GetReflection();

    if (node.GetType() == TNode::Null) {
        // default
        return;
    }

    const bool skipDefault = config.SkipEmptyOptionalFields;

    switch (fd->cpp_type()) {
        case FieldDescriptor::CPPTYPE_INT32: {
            const auto v = config.CastRobust ? node.ConvertTo<i32>() : static_cast<i32>(node.AsInt64());
            if (!skipDefault || fd->is_required() || v != fd->default_value_int32()) {
                r->SetInt32(&proto, fd, v);
            }
            break;
        }
        case FieldDescriptor::CPPTYPE_INT64: {
            const auto v = config.CastRobust ? node.ConvertTo<i64>() : node.AsInt64();
            if (!skipDefault || fd->is_required() || v != fd->default_value_int64()) {
                r->SetInt64(&proto, fd, v);
            }
            break;
        }
        case FieldDescriptor::CPPTYPE_UINT32: {
            const auto v = config.CastRobust ? node.ConvertTo<ui32>() : static_cast<ui32>(node.AsUint64());
            if (!skipDefault || fd->is_required() || v != fd->default_value_uint32()) {
                r->SetUInt32(&proto, fd, v);
            }
            break;
        }
        case FieldDescriptor::CPPTYPE_UINT64: {
            const auto v = config.CastRobust ? node.ConvertTo<ui64>() : node.AsUint64();
            if (!skipDefault || fd->is_required() || v != fd->default_value_uint64()) {
                r->SetUInt64(&proto, fd, v);
            }
            break;
        }
        case FieldDescriptor::CPPTYPE_DOUBLE: {
            const auto v = config.CastRobust ? node.ConvertTo<double>() : node.AsDouble();
            if (!skipDefault || fd->is_required() || v != fd->default_value_double()) {
                r->SetDouble(&proto, fd, v);
            }
            break;
        }
        case FieldDescriptor::CPPTYPE_FLOAT: {
            const auto v = config.CastRobust ? node.ConvertTo<double>() : node.AsDouble();
            if (!skipDefault || fd->is_required() || v != fd->default_value_float()) {
                r->SetFloat(&proto, fd, v);
            }
            break;
        }
        case FieldDescriptor::CPPTYPE_BOOL: {
            const auto v = config.CastRobust ? node.ConvertTo<bool>() : node.AsBool();
            if (!skipDefault || fd->is_required() || v != fd->default_value_bool()) {
                r->SetBool(&proto, fd, v);
            }
            break;
        }
        case FieldDescriptor::CPPTYPE_STRING: {
            const TString& v = config.CastRobust ? node.ConvertTo<TString>() : node.AsString();
            if (!skipDefault || fd->is_required() || v != TStringBuf{fd->default_value_string()}) {
                r->SetString(&proto, fd, v);
            }
            break;
        }
        case FieldDescriptor::CPPTYPE_ENUM: {
            const auto* vd = FindEnumValue(node, fd, config);
            Y_ENSURE(vd != nullptr, "Enum value is not found");
            if (!skipDefault || fd->is_required() || vd->name() != TStringBuf{fd->default_value_enum()->name()}) {
                r->SetEnum(&proto, fd, vd);
            }
            break;
        }
        case FieldDescriptor::CPPTYPE_MESSAGE: {
            YtNodeToProto(node, *r->MutableMessage(&proto, fd), config);
            break;
        }
    }
}

static void YtNodeToGoogleTimestamp(const TNode& node, Message& proto, const TParseConfig config) {
    auto d = proto.GetDescriptor();
    auto r = proto.GetReflection();

    i64 seconds = config.CastRobust ? node.ConvertTo<i64>() :  node.AsInt64();
    i32 nanos = 0;
    if (config.GoogleTimestampNanos) {
        nanos = seconds % TParseConfig::SECONDS_TO_NANOSECONDS;
        seconds /= TParseConfig::SECONDS_TO_NANOSECONDS;
    }

    for (int i = 0, end = d->field_count(); i < end; ++i) {
        const FieldDescriptor* fd = d->field(i);

        const TString columnName = CalcColumnName(*fd, config.UseYtExtention, config.UseToLower);
        if (columnName == TStringBuf("seconds")) {
            r->SetInt64(&proto, fd, seconds);
        } else if (config.GoogleTimestampNanos && columnName == TStringBuf("nanos")) {
            r->SetInt32(&proto, fd, nanos);
        }
    }
}

static void StringToMapKey(const TString& keyStr, Message& proto, const FieldDescriptor* fd) {
    auto r = proto.GetReflection();

    switch (fd->cpp_type()) {
        case FieldDescriptor::CPPTYPE_STRING:
            r->SetString(&proto, fd, keyStr);
            break;

        case FieldDescriptor::CPPTYPE_INT32:
            r->SetInt32(&proto, fd, FromString<i32>(keyStr));
            break;

        case FieldDescriptor::CPPTYPE_INT64:
            r->SetInt64(&proto, fd, FromString<i64>(keyStr));
            break;

        case FieldDescriptor::CPPTYPE_UINT32:
            r->SetUInt32(&proto, fd, FromString<ui32>(keyStr));
            break;

        case FieldDescriptor::CPPTYPE_UINT64:
            r->SetUInt64(&proto, fd, FromString<ui64>(keyStr));
            break;

        case FieldDescriptor::CPPTYPE_DOUBLE:
            r->SetDouble(&proto, fd, FromString<double>(keyStr));
            break;

        case FieldDescriptor::CPPTYPE_FLOAT:
            r->SetFloat(&proto, fd, FromString<float>(keyStr));
            break;

        case FieldDescriptor::CPPTYPE_BOOL:
            r->SetBool(&proto, fd, FromString<bool>(keyStr));
            break;

        default:
            Y_ENSURE(false, "Unimplemented for type");
    }
}

static void NodeToMap(const TNode& node,
                        const FieldDescriptor* fd,
                        Message& proto,
                        const TParseConfig config) {
    if (node.GetType() == TNode::Null) {
        return;
    }

    auto r = proto.GetReflection();

    for (const auto& [keyStr, valueNode] : node.AsMap()) {
        Message& mapEntry = *r->AddMessage(&proto, fd);
        const auto d = mapEntry.GetDescriptor();
        Y_ENSURE(d->field_count() == 2);
        const auto keyDescriptor = d->field(0);
        StringToMapKey(keyStr, mapEntry, keyDescriptor);
        const auto valueDescriptor = d->field(1);
        NodeToSingleField(valueNode, valueDescriptor, mapEntry, config);
    }
}

void YtNodeToProto(const TNode& node, Message& proto, const TParseConfig config) {
    proto.Clear();

    if (node.GetType() == TNode::Undefined || node.GetType() == TNode::Null) {
        return;
    } else if (config.GoogleTimestampToInt64 && proto.GetTypeName() == TParseConfig::GOOGLE_PROTOBUF_TIMESTAMP_TYPE) {
        YtNodeToGoogleTimestamp(node, proto, config);
        return;
    } else {
        Y_ENSURE(node.GetType() == TNode::Map, "expected map node");
    }

    auto d = proto.GetDescriptor();

    for (int i = 0, end = d->field_count(); i < end; ++i) {
        const FieldDescriptor* fd = d->field(i);

        const TString columnName = CalcColumnName(*fd, config.UseYtExtention, config.UseToLower);
        if (node.HasKey(columnName)) {
            if (fd->is_repeated()) {
                if (fd->is_map() && !config.ProtoMapFromList) {
                    NodeToMap(node[columnName], fd, proto, config);
                } else {
                    NodeToRepeatedField(node[columnName], fd, proto, config);
                }
            } else {
                NodeToSingleField(node[columnName], fd, proto, config);
            }
        } else {
            // default
        }
    }
}
