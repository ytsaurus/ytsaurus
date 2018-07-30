#include "yql_row_spec.h"

#include "yql_names.h"
#include "yql_type_scheme.h"

#include <mapreduce/yt/node/node_io.h>

#include <util/generic/cast.h>
#include <util/generic/xrange.h>
#include <util/string/builder.h>

namespace NYql {

bool TYqlRowSpecInfo::Parse(const TString& yson, TExprContext& ctx, const TPosition& pos) {
    try {
        return Parse(NYT::NodeFromYsonString(yson), ctx, pos);
    } catch (const yexception& e) {
        ctx.AddError(TIssue(pos, TStringBuilder() << "Failed to parse row spec from YSON: " << e.what()));
        return false;
    }
}

bool TYqlRowSpecInfo::Parse(const NYT::TNode& attrs, TExprContext& ctx, const TPosition& pos) {
    *this = {};
    try {
        if (!attrs.HasKey(RowSpecAttrType)) {
            ctx.AddError(TIssue(pos, TStringBuilder() << "Row spec doesn't have mandatory Type attribute"));
            return false;
        }
        auto type = ParseTypeFromYson(attrs[RowSpecAttrType], ctx);
        if (!type) {
            return false;
        }
        if (type->GetKind() != ETypeAnnotationKind::Struct) {
            ctx.AddError(TIssue(pos, TStringBuilder() << "Row spec defines not a struct type"));
            return false;
        }

        Type = type->Cast<TStructExprType>();

        if (attrs.HasKey(RowSpecAttrStrictSchema)) {
            // Backward compatible parse. Old code saves 'StrictSchema' as Int64
            StrictSchema = attrs[RowSpecAttrStrictSchema].IsInt64()
                ? attrs[RowSpecAttrStrictSchema].AsInt64() != 0
                : NYT::GetBool(attrs[RowSpecAttrStrictSchema]);
            if (!StrictSchema) {
                auto items = Type->GetItems();
                auto dictType = ctx.MakeType<TDictExprType>(
                    ctx.MakeType<TDataExprType>(AsStringBuf("String")),
                    ctx.MakeType<TDataExprType>(AsStringBuf("String")));
                items.push_back(ctx.MakeType<TItemExprType>(YqlOthersColumnName, dictType));
                Type = ctx.MakeType<TStructExprType>(items);
            }
        }

        if (attrs.HasKey(RowSpecAttrSortDirections)) {
            for (auto& item: attrs[RowSpecAttrSortDirections].AsList()) {
                SortDirections.push_back(item.AsInt64() != 0);
            }
        }

        auto loadColumnList = [&] (TStringBuf name, TVector<TString>& columns) {
            if (attrs.HasKey(name)) {
                auto& list = attrs[name].AsList();
                for (const auto& item : list) {
                    columns.push_back(item.AsString());
                }
            }
        };

        loadColumnList(RowSpecAttrSortMembers, SortMembers);
        loadColumnList(RowSpecAttrSortedBy, SortedBy);

        if (attrs.HasKey(RowSpecAttrSortedByTypes)) {
            auto& list = attrs[RowSpecAttrSortedByTypes].AsList();
            for (auto& type : list) {
                SortedByTypes.push_back(ParseTypeFromYson(type, ctx));
            }
        }

        if (attrs.HasKey(RowSpecAttrUniqueKeys)) {
            UniqueKeys = NYT::GetBool(attrs[RowSpecAttrUniqueKeys]);
        }

        if (attrs.HasKey(RowSpecAttrDefaultValues)) {
            for (auto& value : attrs[RowSpecAttrDefaultValues].AsMap()) {
                DefaultValues[value.first] = NYT::NodeFromYsonString(value.second.AsString()).AsString();
            }
        }

    } catch (const yexception& e) {
        ctx.AddError(TIssue(pos, TStringBuilder() << "Failed to parse row spec from YSON: " << e.what()));
        return false;
    }

    return true;
}

TString TYqlRowSpecInfo::ToYsonString() const {
    NYT::TNode attrs = NYT::TNode::CreateMap();
    FillNode(attrs[YqlRowSpecAttribute]);
    return NYT::NodeToYsonString(attrs);
}

void TYqlRowSpecInfo::FillNode(NYT::TNode& attrs) const {
    attrs[RowSpecAttrType] = TypeToYsonNode(Type);
    attrs[RowSpecAttrStrictSchema] = StrictSchema;
    if (!SortedBy.empty()) {
        attrs[RowSpecAttrUniqueKeys] = UniqueKeys;
    }

    if (!SortDirections.empty()) {
        auto list = NYT::TNode::CreateList();
        for (bool dir: SortDirections) {
            list.Add(dir ? 1 : 0);
        }
        attrs[RowSpecAttrSortDirections] = list;
    }

    auto saveColumnList = [&attrs] (TStringBuf name, const TVector<TString>& columns) {
        if (!columns.empty()) {
            auto list = NYT::TNode::CreateList();
            for (const auto& item : columns) {
                list.Add(item);
            }

            attrs[name] = list;
        }
    };

    saveColumnList(RowSpecAttrSortMembers, SortMembers);
    saveColumnList(RowSpecAttrSortedBy, SortedBy);

    if (!SortedByTypes.empty()) {
        auto list = NYT::TNode::CreateList();
        for (auto type: SortedByTypes) {
            list.Add(TypeToYsonNode(type));
        }
        attrs[RowSpecAttrSortedByTypes] = list;
    }

    if (!DefaultValues.empty()) {
        auto map = NYT::TNode::CreateMap();
        for (const auto& val: DefaultValues) {
            map[val.first] = NYT::NodeToYsonString(NYT::TNode(val.second));
        }
        attrs[RowSpecAttrDefaultValues] = map;
    }
}

bool TYqlRowSpecInfo::HasAuxColumns() const {
    for (auto& x: SortedBy) {
        if (!Type->FindItem(x)) {
            return true;
        }
    }
    return false;
}

void TYqlRowSpecInfo::CopySortness(const TYqlRowSpecInfo& from) {
    SortDirections = from.SortDirections;
    SortMembers = from.SortMembers;
    SortedBy = from.SortedBy;
    SortedByTypes = from.SortedByTypes;
}

}
