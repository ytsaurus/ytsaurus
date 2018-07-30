#include "yql_type_scheme.h"

#include <yql/ast/yql_expr.h>
#include <yql/ast/yql_expr_types.h>

#include <mapreduce/yt/common/helpers.h>
#include <mapreduce/yt/common/node_builder.h>

#include <library/yson/parser.h>
#include <library/yson/writer.h>

#include <util/generic/map.h>
#include <util/stream/str.h>

namespace NYql {

template <typename TDerived>
class TYqlTypeYsonSaverImpl {
    typedef TYqlTypeYsonSaverImpl<TDerived> TSelf;

public:
    typedef NYT::TYsonConsumerBase TConsumer;

    TYqlTypeYsonSaverImpl(TConsumer& writer)
        : Writer(writer)
    {
    }

    template <typename TType>
    void Save(TType* type) {
        static_cast<TDerived*>(this)->Save(type);
    }

protected:
    void SaveTypeHeader(TStringBuf name) {
        Writer.OnBeginList();
        Writer.OnListItem();
        Writer.OnStringScalar(name);
    }

#define SAVE_TYPE_IMPL(type) \
    void Save ## type() { \
        SaveTypeHeader(#type); \
        Writer.OnEndList(); \
    }

        SAVE_TYPE_IMPL(Type)
        SAVE_TYPE_IMPL(VoidType)
        SAVE_TYPE_IMPL(NullType)

#undef SAVE_TYPE_IMPL

    void SaveDataType(const TStringBuf& dataType) {
        SaveTypeHeader("DataType");
        Writer.OnListItem();
        Writer.OnStringScalar(dataType);
        Writer.OnEndList();
    }

    void SaveResourceType(const TStringBuf& tag) {
        SaveTypeHeader("ResourceType");
        Writer.OnListItem();
        Writer.OnStringScalar(tag);
        Writer.OnEndList();
    }

    void SaveTaggedType(const TTaggedExprType& taggedType) {
        SaveTypeHeader("TaggedType");
        Writer.OnListItem();
        Writer.OnStringScalar(taggedType.GetTag());
        Writer.OnListItem();
        TSelf baseType(Writer);
        baseType.Save(taggedType.GetBaseType());
        Writer.OnEndList();
    }

    void SaveErrorType(const TErrorExprType& errorType) {
        SaveTypeHeader("ErrorType");
        auto err = errorType.GetError();
        Writer.OnListItem();
        Writer.OnInt64Scalar(err.Position.Row);
        Writer.OnListItem();
        Writer.OnInt64Scalar(err.Position.Column);
        Writer.OnListItem();
        Writer.OnStringScalar(err.Message);
        Writer.OnEndList();
    }

    template <typename TStructType>
    void SaveStructType(const TStructType& structType) {
        SaveTypeHeader("StructType");
        Writer.OnListItem();
        Writer.OnBeginList();
        for (ui32 i = 0, e = structType.GetMembersCount(); i < e; ++i) {
            Writer.OnListItem();
            Writer.OnBeginList();
            Writer.OnListItem();
            Writer.OnStringScalar(structType.GetMemberName(i));
            Writer.OnListItem();
            TSelf value(Writer);
            value.Save(structType.GetMemberType(i));
            Writer.OnEndList();
        }
        Writer.OnEndList();
        Writer.OnEndList();
    }

    template <typename TListType>
    void SaveListType(const TListType& listType) {
        SaveTypeHeader("ListType");
        Writer.OnListItem();
        TSelf item(Writer);
        item.Save(listType.GetItemType());
        Writer.OnEndList();
    }

    template <typename TStreamType>
    void SaveStreamType(const TStreamType& streamType) {
        SaveTypeHeader("StreamType");
        Writer.OnListItem();
        TSelf item(Writer);
        item.Save(streamType.GetItemType());
        Writer.OnEndList();
    }

    template <typename TOptionalType>
    void SaveOptionalType(const TOptionalType& optionalType) {
        SaveTypeHeader("OptionalType");
        Writer.OnListItem();
        TSelf item(Writer);
        item.Save(optionalType.GetItemType());
        Writer.OnEndList();
    }

    template <typename TDictType>
    void SaveDictType(const TDictType& dictType) {
        SaveTypeHeader("DictType");
        Writer.OnListItem();
        TSelf key(Writer);
        key.Save(dictType.GetKeyType());
        Writer.OnListItem();
        TSelf val(Writer);
        val.Save(dictType.GetPayloadType());
        Writer.OnEndList();
    }

    template <typename TTupleType>
    void SaveTupleType(const TTupleType& tupleType) {
        SaveTypeHeader("TupleType");
        Writer.OnListItem();
        Writer.OnBeginList();
        for (ui32 i = 0, e = tupleType.GetElementsCount(); i < e; ++i) {
            Writer.OnListItem();
            TSelf element(Writer);
            element.Save(tupleType.GetElementType(i));
        }
        Writer.OnEndList();
        Writer.OnEndList();
    }

    template <typename TCallableType>
    void SaveCallableType(const TCallableType& callableType) {
        SaveTypeHeader("CallableType");
        Writer.OnListItem();
        // main settings
        Writer.OnBeginList();
        if (callableType.GetOptionalArgsCount() > 0 || !callableType.GetPayload().Empty()) {
            Writer.OnListItem();
            Writer.OnUint64Scalar(callableType.GetOptionalArgsCount());
        }

        if (!callableType.GetPayload().Empty()) {
            Writer.OnListItem();
            Writer.OnStringScalar(callableType.GetPayload());
        }

        Writer.OnEndList();
        // ret
        Writer.OnListItem();
        Writer.OnBeginList();
        Writer.OnListItem();
        TSelf ret(Writer);
        ret.Save(callableType.GetReturnType());
        Writer.OnEndList();
        // args
        Writer.OnListItem();
        Writer.OnBeginList();
        for (ui32 i = 0, e = callableType.GetArgumentsCount(); i < e; ++i) {
            Writer.OnListItem();
            Writer.OnBeginList();
            Writer.OnListItem();
            TSelf arg(Writer);
            arg.Save(callableType.GetArgumentType(i));
            if (!callableType.GetArgumentName(i).Empty()) {
                Writer.OnListItem();
                Writer.OnStringScalar(callableType.GetArgumentName(i));
            }

            if (callableType.GetArgumentFlags(i) != 0) {
                Writer.OnListItem();
                Writer.OnUint64Scalar(callableType.GetArgumentFlags(i));
            }

            Writer.OnEndList();
        }

        Writer.OnEndList();
        Writer.OnEndList();
    }

    template <typename TVariantType>
    void SaveVariantType(const TVariantType& variantType) {
        SaveTypeHeader("VariantType");
        Writer.OnListItem();
        TSelf item(Writer);
        item.Save(variantType.GetUnderlyingType());
        Writer.OnEndList();
    }

protected:
    NYT::TYsonConsumerBase& Writer;
};

template <template<typename> class TSaver>
class TExprTypeSaver: public TSaver<TExprTypeSaver<TSaver>> {
    typedef TSaver<TExprTypeSaver<TSaver>> TBase;

    struct TStructAdaptor {
        const TStructExprType* Type;

        TStructAdaptor(const TStructExprType* type)
            : Type(type)
        {
        }

        ui32 GetMembersCount() const {
            return Type->GetItems().size();
        }

        const TStringBuf& GetMemberName(ui32 idx) const {
            return Type->GetItems()[idx]->GetName();
        }

        const TTypeAnnotationNode* GetMemberType(ui32 idx) const {
            return Type->GetItems()[idx]->GetItemType();
        }
    };

    struct TOrderedStructAdaptor {
        TVector<const TItemExprType*> Members;

        TOrderedStructAdaptor(const TVector<TString>& columns, const TStructExprType* type)
        {
            TMap<TStringBuf, const TItemExprType*> members;
            for (auto& item: type->GetItems()) {
                members[item->GetName()] = item;
            }
            for (auto& col: columns) {
                auto item = members.FindPtr(col);
                if (item) {
                    Members.push_back(*item);
                }
            }
        }

        ui32 GetMembersCount() const {
            return Members.size();
        }

        const TStringBuf& GetMemberName(ui32 idx) const {
            return Members[idx]->GetName();
        }

        const TTypeAnnotationNode* GetMemberType(ui32 idx) const {
            return Members[idx]->GetItemType();
        }
    };

    struct TTupleAdaptor {
        const TTupleExprType* Type;

        TTupleAdaptor(const TTupleExprType* type)
            : Type(type)
        {
        }

        ui32 GetElementsCount() const {
            return Type->GetItems().size();
        }

        const TTypeAnnotationNode* GetElementType(ui32 idx) const {
            return Type->GetItems()[idx];
        }
    };

    struct TCallableAdaptor {
        const TCallableExprType* Type;

        TCallableAdaptor(const TCallableExprType* type)
            : Type(type)
        {
        }

        size_t GetOptionalArgsCount() const {
            return Type->GetOptionalArgumentsCount();
        }

        TStringBuf GetPayload() const {
            return Type->GetPayload();
        }

        const TTypeAnnotationNode* GetReturnType() const {
            return Type->GetReturnType();
        }

        size_t GetArgumentsCount() const {
            return Type->GetArgumentsSize();
        }

        TStringBuf GetArgumentName(size_t i) const {
            return Type->GetArguments().at(i).Name;
        }

        ui64 GetArgumentFlags(size_t i) const {
            return Type->GetArguments().at(i).Flags;
        }

        const TTypeAnnotationNode* GetArgumentType(size_t i) const {
            return Type->GetArguments().at(i).Type;
        }
    };

public:
    TExprTypeSaver(typename TBase::TConsumer& consumer)
        : TBase(consumer)
    {
    }

    void Save(const TTypeAnnotationNode* type) {
        switch (type->GetKind()) {
            case ETypeAnnotationKind::Data:
                TBase::SaveDataType(type->Cast<TDataExprType>()->GetDataType());
                break;
            case ETypeAnnotationKind::Struct:
                TBase::SaveStructType(TStructAdaptor(type->Cast<TStructExprType>()));
                break;
            case ETypeAnnotationKind::List:
                TBase::SaveListType(*type->Cast<TListExprType>());
                break;
            case ETypeAnnotationKind::Optional:
                TBase::SaveOptionalType(*type->Cast<TOptionalExprType>());
                break;
            case ETypeAnnotationKind::Tuple:
                TBase::SaveTupleType(TTupleAdaptor(type->Cast<TTupleExprType>()));
                break;
            case ETypeAnnotationKind::Dict:
                TBase::SaveDictType(*type->Cast<TDictExprType>());
                break;
            case ETypeAnnotationKind::Type:
                TBase::SaveType();
                break;
            case ETypeAnnotationKind::Void:
                TBase::SaveVoidType();
                break;
            case ETypeAnnotationKind::Null:
                TBase::SaveNullType();
                break;
            case ETypeAnnotationKind::Resource:
                TBase::SaveResourceType(type->Cast<TResourceExprType>()->GetTag());
                break;
            case ETypeAnnotationKind::Tagged:
                TBase::SaveTaggedType(*type->Cast<TTaggedExprType>());
                break;
            case ETypeAnnotationKind::Error:
                TBase::SaveErrorType(*type->Cast<TErrorExprType>());
                break;
            case ETypeAnnotationKind::Callable:
                TBase::SaveCallableType(TCallableAdaptor(type->Cast<TCallableExprType>()));
                break;
            case ETypeAnnotationKind::Variant:
                TBase::SaveVariantType(*type->Cast<TVariantExprType>());
                break;
            case ETypeAnnotationKind::Stream:
                TBase::SaveStreamType(*type->Cast<TStreamExprType>());
                break;
            default:
                YQL_ENSURE(false, "Unsupported type annotation kind");
        }
    }

    void SaveStructType(const TStructExprType* type, const TVector<TString>* columns) {
        if (columns) {
            TBase::SaveStructType(TOrderedStructAdaptor(*columns, type));
        } else {
            Save(type);
        }
    }
};

void SaveStructTypeToYson(NYT::TYsonConsumerBase& writer, const TStructExprType* type, const TVector<TString>* columns) {
    TExprTypeSaver<TYqlTypeYsonSaverImpl> saver(writer);
    saver.SaveStructType(type, columns);
}

void WriteTypeToYson(NYT::TYsonConsumerBase& writer, const TTypeAnnotationNode* type) {
    TExprTypeSaver<TYqlTypeYsonSaverImpl> saver(writer);
    saver.Save(type);
}

NYT::TNode TypeToYsonNode(const TTypeAnnotationNode* type) {
    NYT::TNode res;
    NYT::TNodeBuilder builder(&res);
    WriteTypeToYson(builder, type);
    return res;
}

TString WriteTypeToYson(const TTypeAnnotationNode* type) {
    TStringStream stream;
    NYT::TYsonWriter writer(&stream);
    WriteTypeToYson(writer, type);
    return stream.Str();
}

template <typename TLoader>
static typename TLoader::TType* DoLoadTypeFromYson(TLoader& loader, const NYT::TNode& node) {
    if (!node.IsList() || node.Size() < 1 || !node[0].IsString()) {
        loader.Error("Invalid type scheme");
        return nullptr;
    }
    auto typeName = node[0].AsString();
    if (typeName == "VoidType") {
        return loader.LoadVoidType();
    } else if (typeName == "NullType") {
        return loader.LoadNullType();
    } else if (typeName == "DataType") {
        if (node.Size() != 2 || !node[1].IsString()) {
            loader.Error("Invalid data type scheme");
            return nullptr;
        }
        return loader.LoadDataType(node[1].AsString());
    } else if (typeName == "ResourceType") {
        if (node.Size() != 2 || !node[1].IsString()) {
            loader.Error("Invalid resource type scheme");
            return nullptr;
        }
        return loader.LoadResourceType(node[1].AsString());
    } else if (typeName == "TaggedType") {
        if (node.Size() != 3 || !node[1].IsString()) {
            loader.Error("Invalid tagged type scheme");
            return nullptr;
        }
        auto baseType = DoLoadTypeFromYson(loader, node[2]);
        if (!baseType) {
            return nullptr;
        }
        return loader.LoadTaggedType(baseType, node[1].AsString());
    } else if (typeName == "ErrorType") {
        if (node.Size() != 4 || !node[1].IsInt64() || !node[2].IsInt64() || !node[3].IsString()) {
            loader.Error("Invalid error type scheme");
            return nullptr;
        }
        return loader.LoadErrorType(node[1].AsInt64(), node[2].AsInt64(), node[3].AsString());
    } else if (typeName == "StructType") {
        if (node.Size() != 2 || !node[1].IsList()) {
            loader.Error("Invalid struct type scheme");
            return nullptr;
        }
        TVector<std::pair<TString, typename TLoader::TType*>> members;
        for (auto& member : node[1].AsList()) {
            if (!member.IsList() || member.Size() != 2 || !member[0].IsString()) {
                loader.Error("Invalid struct type scheme");
                return nullptr;
            }

            auto name = member[0].AsString();
            auto memberType = DoLoadTypeFromYson(loader, member[1]);
            if (!memberType) {
                return nullptr;
            }
            members.push_back(std::make_pair(name, memberType));
        }
        return loader.LoadStructType(members);
    } else if (typeName == "ListType") {
        if (node.Size() != 2) {
            loader.Error("Invalid list type scheme");
            return nullptr;
        }
        auto itemType = DoLoadTypeFromYson(loader, node[1]);
        if (!itemType) {
            return nullptr;
        }
        return loader.LoadListType(itemType);
    } else if (typeName == "StreamType") {
        if (node.Size() != 2) {
            loader.Error("Invalid list type scheme");
            return nullptr;
        }
        auto itemType = DoLoadTypeFromYson(loader, node[1]);
        if (!itemType) {
            return nullptr;
        }
        return loader.LoadStreamType(itemType);

    } else if (typeName == "OptionalType") {
        if (node.Size() != 2) {
            loader.Error("Invalid optional type scheme");
            return nullptr;
        }
        auto itemType = DoLoadTypeFromYson(loader, node[1]);
        if (!itemType) {
            return nullptr;
        }
        return loader.LoadOptionalType(itemType);
    } else if (typeName == "TupleType") {
        if (node.Size() != 2 || !node[1].IsList()) {
            loader.Error("Invalid tuple type scheme");
            return nullptr;
        }
        TVector<typename TLoader::TType*> elements;
        for (auto& item: node[1].AsList()) {
            elements.push_back(DoLoadTypeFromYson(loader, item));
            if (!elements.back()) {
                return nullptr;
            }
        }
        return loader.LoadTupleType(elements);
    } else if (typeName == "DictType") {
        if (node.Size() != 3) {
            loader.Error("Invalid dict type scheme");
            return nullptr;
        }
        auto keyType = DoLoadTypeFromYson(loader, node[1]);
        auto valType = DoLoadTypeFromYson(loader, node[2]);
        if (!keyType || !valType) {
            return nullptr;
        }
        return loader.LoadDictType(keyType, valType);
    } else if (typeName == "CallableType") {
        if (node.Size() != 4 || !node[1].IsList() || !node[2].IsList() || !node[3].IsList()) {
            loader.Error("Invalid callable type scheme");
            return nullptr;
        }
        ui32 optionalCount = 0;
        TString payload;
        if (!node[1].AsList().empty()) {
            auto& list = node[1].AsList();
            if (!list[0].IsUint64()) {
                loader.Error("Invalid callable type scheme");
                return nullptr;
            }
            optionalCount = list[0].AsUint64();
            if (list.size() > 1) {
                if (!list[1].IsString()) {
                    loader.Error("Invalid callable type scheme");
                    return nullptr;
                }
                payload = list[1].AsString();
            }
            if (list.size() > 2) {
                loader.Error("Invalid callable type scheme");
                return nullptr;
            }
        }

        if (node[2].AsList().size() != 1) {
            loader.Error("Invalid callable type scheme");
            return nullptr;
        }
        auto returnType = DoLoadTypeFromYson(loader, node[2].AsList()[0]);
        if (!returnType) {
            return nullptr;
        }

        TVector<typename TLoader::TType*> argTypes;
        TVector<TString> argNames;
        TVector<ui64> argFlags;
        for (auto& item: node[3].AsList()) {
            if (!item.IsList() || item.AsList().size() < 1 || item.AsList().size() > 3) {
                loader.Error("Invalid callable type scheme");
                return nullptr;
            }

            argTypes.push_back(DoLoadTypeFromYson(loader, item.AsList()[0]));
            if (!argTypes.back()) {
                return nullptr;
            }
            if (item.AsList().size() > 1 && item.AsList()[1].IsString()) {
                argNames.push_back(item.AsList()[1].AsString());
            } else {
                argNames.emplace_back();
            }
            if (item.AsList().size() > 1 && item.AsList()[1].IsUint64()) {
                argFlags.push_back(item.AsList()[1].AsUint64());
            } else if (item.AsList().size() > 2) {
                if (!item.AsList()[2].IsUint64()) {
                    loader.Error("Invalid callable type scheme");
                    return nullptr;
                }
                argFlags.push_back(item.AsList()[2].AsUint64());
            } else {
                argFlags.emplace_back();
            }
        }

        return loader.LoadCallableType(returnType, argTypes, argNames, argFlags, optionalCount, payload);
    } else if (typeName == "VariantType") {
        if (node.Size() != 2) {
            loader.Error("Invalid variant type scheme");
            return nullptr;
        }
        auto underlyingType = DoLoadTypeFromYson(loader, node[1]);
        if (!underlyingType) {
            return nullptr;
        }
        return loader.LoadVariantType(underlyingType);
    }
    loader.Error("unsupported type: " + typeName);
    return nullptr;
}

struct TExprTypeLoader {
    typedef const TTypeAnnotationNode TType;

    TExprContext& Ctx;
    TPosition Pos;

    TExprTypeLoader(TExprContext& ctx, const TPosition& pos = TPosition())
        : Ctx(ctx)
        , Pos(pos)
    {
    }
    TType* LoadVoidType() {
        return Ctx.MakeType<TVoidExprType>();
    }
    TType* LoadNullType() {
        return Ctx.MakeType<TNullExprType>();
    }
    TType* LoadDataType(const TString& dataType) {
        return Ctx.MakeType<TDataExprType>(dataType);
    }
    TType* LoadResourceType(const TString& tag) {
        return Ctx.MakeType<TResourceExprType>(tag);
    }
    TType* LoadTaggedType(TType* baseType, const TString& tag) {
        return Ctx.MakeType<TTaggedExprType>(baseType, tag);
    }
    TType* LoadErrorType(ui32 row, ui32 column, const TString& msg) {
        return Ctx.MakeType<TErrorExprType>(TIssue(TPosition(column, row), msg));
    }
    TType* LoadStructType(const TVector<std::pair<TString, TType*>>& members) {
        TVector<const TItemExprType*> items;
        for (auto& member: members) {
            items.push_back(Ctx.MakeType<TItemExprType>(member.first, member.second));
        }
        return Ctx.MakeType<TStructExprType>(items);
    }
    TType* LoadListType(TType* itemType) {
        return Ctx.MakeType<TListExprType>(itemType);
    }
    TType* LoadStreamType(TType* itemType) {
        return Ctx.MakeType<TStreamExprType>(itemType);
    }
    TType* LoadOptionalType(TType* itemType) {
        return Ctx.MakeType<TOptionalExprType>(itemType);
    }
    TType* LoadTupleType(const TVector<TType*> elements) {
        return Ctx.MakeType<TTupleExprType>(elements);
    }
    TType* LoadDictType(TType* keyType, TType* valType) {
        return Ctx.MakeType<TDictExprType>(keyType, valType);
    }
    TType* LoadCallableType(TType* returnType, const TVector<TType*>& argTypes, const TVector<TString>& argNames,
        const TVector<ui64>& argFlags, size_t optionalCount, const TString& payload) {
        YQL_ENSURE(argTypes.size() == argNames.size() && argTypes.size() == argFlags.size());
        TVector<TCallableExprType::TArgumentInfo> args;
        for (size_t i = 0; i < argTypes.size(); ++i) {
            args.emplace_back();
            args.back().Type = argTypes[i];
            args.back().Name = Ctx.AppendString(argNames[i]);
            args.back().Flags = argFlags[i];
        }
        return Ctx.MakeType<TCallableExprType>(returnType, args, optionalCount, Ctx.AppendString(payload));
    }
    TType* LoadVariantType(TType* underlyingType) {
        return Ctx.MakeType<TVariantExprType>(underlyingType);
    }
    void Error(const TString& info) {
        Ctx.AddError(TIssue(Pos, info));
    }
};

const TTypeAnnotationNode* ParseTypeFromYson(const TString& yson, TExprContext& ctx, const TPosition& pos) {
    NYT::TNode node;
    TStringStream err;
    if (!ParseYson(node, yson, err)) {
        ctx.AddError(TIssue(pos, err.Str()));
        return nullptr;
    }

    return ParseTypeFromYson(node, ctx, pos);
}

const TTypeAnnotationNode* ParseTypeFromYson(const NYT::TNode& node, TExprContext& ctx, const TPosition& pos) {
    TExprTypeLoader loader(ctx, pos);
    return DoLoadTypeFromYson(loader, node);
}

bool ParseYson(NYT::TNode& res, const TString& yson, IOutputStream& err) {
    try {
        res = NYT::NodeFromYsonString(yson);
    }
    catch (const yexception& e) {
        err << "Failed to parse scheme from YSON: " << e.what();
        return false;
    }
    return true;
}

}
