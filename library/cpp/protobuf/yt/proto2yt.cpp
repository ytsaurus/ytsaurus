#include "proto2yt.h"

#include "column_name.h"

#include <google/protobuf/message.h>

#include <library/cpp/yson/node/node_io.h>

#include <mapreduce/yt/interface/node.h>


using namespace NYT;
using namespace ::google::protobuf;

static TNode CreateDefaultValue(
    const FieldDescriptor* fd,
    const bool saveEnumsAsString = false) {
#define DEFAULT_TO_NODE(EProtoCppType, ValueGet) \
    case FieldDescriptor::EProtoCppType: {       \
        return TNode(fd->ValueGet());            \
        break;                                   \
    }

    switch (fd->cpp_type()) {
        DEFAULT_TO_NODE(CPPTYPE_INT32, default_value_int32);
        DEFAULT_TO_NODE(CPPTYPE_INT64, default_value_int64);
        DEFAULT_TO_NODE(CPPTYPE_UINT32, default_value_uint32);
        DEFAULT_TO_NODE(CPPTYPE_UINT64, default_value_uint64);
        DEFAULT_TO_NODE(CPPTYPE_DOUBLE, default_value_double);
        DEFAULT_TO_NODE(CPPTYPE_FLOAT, default_value_float);
        DEFAULT_TO_NODE(CPPTYPE_BOOL, default_value_bool);

        case FieldDescriptor::CPPTYPE_ENUM:
            if (saveEnumsAsString) {
                return TNode(fd->default_value_enum()->name());
            } else {
                return TNode((i32)fd->default_value_enum()->number());
            }

        case FieldDescriptor::CPPTYPE_STRING:
            return TNode(fd->default_value_string());

        case FieldDescriptor::CPPTYPE_MESSAGE:
            return TNode::CreateEntity();
    }
#undef DEFAULT_TO_NODE

    return TNode();
}

static TNode GoogleTimestampToYtNode(const NProtoBuf::Message& proto, const TParseConfig config) {
    auto d = proto.GetDescriptor();
    auto r = proto.GetReflection();

    i64 seconds = 0;
    i32 nanos = 0;

    for (int i = 0, end = d->field_count(); i < end; ++i) {
        const FieldDescriptor* fd = d->field(i);

        if (r->HasField(proto, fd)) {
            const TString columnName = CalcColumnName(*fd, config.UseYtExtention, config.UseToLower);
            if (columnName == TStringBuf("seconds")) {
                seconds = r->GetInt64(proto, fd);
            } else if (columnName == TStringBuf("nanos")) {
                nanos = r->GetInt32(proto, fd);
            }
        }
    }
    if (config.GoogleTimestampNanos) {
        return TNode(seconds * TParseConfig::SECONDS_TO_NANOSECONDS + nanos);
    } else {
        return TNode(seconds);
    }
}

static TNode CreateRepeatedField(
    const Message& proto,
    const FieldDescriptor* fd,
    const TParseConfig config) {
    auto r = proto.GetReflection();
    auto node = TNode::CreateList();

#define REPEATED_FIELD_TO_NODE(EProtoCppType, ProtoGet)                     \
    case FieldDescriptor::EProtoCppType: {                                  \
        for (size_t i = 0, endI = r->FieldSize(proto, fd); i < endI; ++i) { \
            node.Add(TNode(r->ProtoGet(proto, fd, i)));                     \
        }                                                                   \
        return node;                                                        \
    }

    switch (fd->cpp_type()) {
        REPEATED_FIELD_TO_NODE(CPPTYPE_INT32, GetRepeatedInt32);
        REPEATED_FIELD_TO_NODE(CPPTYPE_INT64, GetRepeatedInt64);
        REPEATED_FIELD_TO_NODE(CPPTYPE_UINT32, GetRepeatedUInt32);
        REPEATED_FIELD_TO_NODE(CPPTYPE_UINT64, GetRepeatedUInt64);
        REPEATED_FIELD_TO_NODE(CPPTYPE_DOUBLE, GetRepeatedDouble);
        REPEATED_FIELD_TO_NODE(CPPTYPE_FLOAT, GetRepeatedFloat);
        REPEATED_FIELD_TO_NODE(CPPTYPE_BOOL, GetRepeatedBool);

        case FieldDescriptor::CPPTYPE_ENUM:
            for (size_t i = 0, endI = r->FieldSize(proto, fd); i < endI; ++i) {
                if (config.SaveEnumsAsString) {
                    node.Add(TNode(r->GetRepeatedEnum(proto, fd, i)->name()));
                } else {
                    node.Add(TNode((i32)r->GetRepeatedEnum(proto, fd, i)->number()));
                }
            }
            return node;

        case FieldDescriptor::CPPTYPE_STRING:
            for (size_t i = 0, endI = r->FieldSize(proto, fd); i < endI; ++i) {
                node.Add(TNode(r->GetRepeatedString(proto, fd, i)));
            }
            return node;

        case FieldDescriptor::CPPTYPE_MESSAGE:
            for (size_t i = 0, endI = r->FieldSize(proto, fd); i < endI; ++i) {
                node.Add(TNode(ProtoToYtNode(r->GetRepeatedMessage(proto, fd, i), config)));
            }

            if (fd->is_map() && config.ProtoMapToList) {
                auto getKey = [](const TNode& a) {
                    for (const auto& key : {"key", "Key"}) {
                        if (a.AsMap().contains(key)) {
                            return NodeToYsonString(a.AsMap().at(key));
                        }
                    }
                    return TString();
                };

                Sort(node.AsList(), [&](const TNode& a, const TNode& b) {
                    return getKey(a) < getKey(b);
                });
            }
            return node;
    }
#undef REPEATED_FIELD_TO_NODE

    return TNode();
}

static TNode CreateSingleValue(
    const Message& proto,
    const FieldDescriptor* fd,
    const TParseConfig config) {
    auto r = proto.GetReflection();

#define FIELD_TO_NODE(EProtoCppType, ProtoGet) \
    case FieldDescriptor::EProtoCppType: {     \
        return TNode(r->ProtoGet(proto, fd));  \
    }

    switch (fd->cpp_type()) {
        FIELD_TO_NODE(CPPTYPE_INT32, GetInt32);
        FIELD_TO_NODE(CPPTYPE_INT64, GetInt64);
        FIELD_TO_NODE(CPPTYPE_UINT32, GetUInt32);
        FIELD_TO_NODE(CPPTYPE_UINT64, GetUInt64);
        FIELD_TO_NODE(CPPTYPE_DOUBLE, GetDouble);
        FIELD_TO_NODE(CPPTYPE_FLOAT, GetFloat);
        FIELD_TO_NODE(CPPTYPE_BOOL, GetBool);

        case FieldDescriptor::CPPTYPE_ENUM:
            if (config.SaveEnumsAsString) {
                return TNode(r->GetEnum(proto, fd)->name());
            } else {
                return TNode((i64)r->GetEnum(proto, fd)->number());
            }
        case FieldDescriptor::CPPTYPE_STRING:
            return TNode(r->GetString(proto, fd));

        case FieldDescriptor::CPPTYPE_MESSAGE:
            return ProtoToYtNode(r->GetMessage(proto, fd), config);
    }

#undef FIELD_TO_NODE

    return TNode();
}

TNode DeduceSchema(const NProtoBuf::Message& proto, const bool inferAnyFromRepeated, const bool useYtExtention, const bool useLower, const bool enumsAsString) {
    TNode node;

    auto d = proto.GetDescriptor();

    for (int i = 0, end = d->field_count(); i < end; ++i) {
        const FieldDescriptor* fd = d->field(i);
        const TString columnName = CalcColumnName(*fd, useYtExtention, useLower);

        if (inferAnyFromRepeated && fd->is_repeated()) {
            node.Add()("name", columnName)("type", "any");
            continue;
        }

        switch (fd->cpp_type()) {
            case FieldDescriptor::CPPTYPE_INT32:
            case FieldDescriptor::CPPTYPE_INT64:
                node.Add()("name", columnName)("type", "int64");
                break;

            case FieldDescriptor::CPPTYPE_UINT32:
            case FieldDescriptor::CPPTYPE_UINT64:
                node.Add()("name", columnName)("type", "uint64");
                break;

            case FieldDescriptor::CPPTYPE_DOUBLE:
            case FieldDescriptor::CPPTYPE_FLOAT:
                node.Add()("name", columnName)("type", "double");
                break;

            case FieldDescriptor::CPPTYPE_BOOL:
                node.Add()("name", columnName)("type", "boolean");
                break;

            case FieldDescriptor::CPPTYPE_ENUM:
                if (enumsAsString) {
                    node.Add()("name", columnName)("type", "string");
                } else {
                    node.Add()("name", columnName)("type", "int64");
                }
                break;

            case FieldDescriptor::CPPTYPE_STRING:
                node.Add()("name", columnName)("type", "string");
                break;

            case FieldDescriptor::CPPTYPE_MESSAGE:
                node.Add()("name", columnName)("type", "any");
                break;
        }
    }

    return node;
}

TNode ProtoToYtNode(const NProtoBuf::Message& proto, const TParseConfig config) {
    if (config.GoogleTimestampToInt64 && proto.GetTypeName() == TParseConfig::GOOGLE_PROTOBUF_TIMESTAMP_TYPE) {
        return GoogleTimestampToYtNode(proto, config);
    }

    auto node = TNode::CreateMap();

    auto d = proto.GetDescriptor();
    auto r = proto.GetReflection();

    for (int i = 0, end = d->field_count(); i < end; ++i) {
        const FieldDescriptor* fd = d->field(i);

        const TString columnName = CalcColumnName(*fd, config.UseYtExtention, config.UseToLower);

        if (fd->is_map() && !config.ProtoMapToList) {
            Y_ABORT("TODO proto map to map");
        }

        if (fd->is_optional()) {
            if (r->HasField(proto, fd)) {
                node(columnName, CreateSingleValue(proto, fd, config));
            } else if (!config.SkipEmptyOptionalFields && (config.UseImplicitDefault || fd->has_default_value())) {
                node(columnName, CreateDefaultValue(fd, config.SaveEnumsAsString));
            }
        } else if (fd->is_repeated()) {
            if (!config.SkipEmptyRepeatedFields || r->FieldSize(proto, fd) > 0) {
                node(columnName, CreateRepeatedField(proto, fd, config));
            }
        } else if (fd->is_required()) {
            node(columnName, CreateSingleValue(proto, fd, config));
        }
    }

    return node;
}
