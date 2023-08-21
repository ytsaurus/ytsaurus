#pragma once

#include <google/protobuf/message.h>

#include <yt/cpp/mapreduce/interface/common.h>

#include <util/string/escape.h>

// ===========================================================================
namespace NYT {
// ===========================================================================
const NProtoBuf::FieldDescriptor* DescriptorByColumn(const NProtoBuf::Descriptor*, TStringBuf columnName);

template <class TProtoType>
const NProtoBuf::FieldDescriptor* DescriptorByColumn(TStringBuf columnName) {
    return DescriptorByColumn(TProtoType::descriptor(), columnName);
}

namespace NDetail {
void CheckFieldCopierTypes(TStringBuf columnName, const NProtoBuf::FieldDescriptor* from, const NProtoBuf::FieldDescriptor* to = nullptr);
} // namespace NDetail

template <class MsgFrom, class MsgTo,
        bool FromProto = std::is_base_of<NProtoBuf::Message, MsgFrom>::value,
        bool ToProto = std::is_base_of<NProtoBuf::Message, MsgTo>::value>
class TFieldCopier {
public:
    TFieldCopier(const TSortColumns& columns) {
        for (auto& keyColumn : columns.Parts_) {
            auto descFrom = DescriptorByColumn<MsgFrom>(keyColumn.EnsureAscending().Name());
            auto descTo = DescriptorByColumn<MsgTo>(keyColumn.EnsureAscending().Name());

            NDetail::CheckFieldCopierTypes(keyColumn.EnsureAscending().Name(), descFrom, descTo);
            CopyDesc_.emplace_back(descFrom, descTo);
        }
    }

    void operator()(const MsgFrom& from, MsgTo& to) const {
        using namespace NProtoBuf;
        auto reflFrom = from.GetReflection();
        auto reflTo = to.GetReflection();
        // NOTE: code below could be moved to cpp file if that would not hurt performance
        for (auto& desc : CopyDesc_) {
            // NOTE: right now, declaring [descFrom, descTo] inside FOR loop will cause
            // incorrect clang 5 warning: "unused variable '' [-Werror,-Wunused-variable]"
            auto [descFrom, descTo] = desc;
            switch(descFrom->cpp_type()) {
            case FieldDescriptor::CPPTYPE_INT32:
                reflTo->SetInt32(&to, descTo, reflFrom->GetInt32(from, descFrom));
                break;
            case FieldDescriptor::CPPTYPE_INT64:
                reflTo->SetInt64(&to, descTo, reflFrom->GetInt64(from, descFrom));
                break;
            case FieldDescriptor::CPPTYPE_UINT32:
                reflTo->SetUInt32(&to, descTo, reflFrom->GetUInt32(from, descFrom));
                break;
            case FieldDescriptor::CPPTYPE_UINT64:
                reflTo->SetUInt64(&to, descTo, reflFrom->GetUInt64(from, descFrom));
                break;
            case FieldDescriptor::CPPTYPE_DOUBLE:
                reflTo->SetDouble(&to, descTo, reflFrom->GetDouble(from, descFrom));
                break;
            case FieldDescriptor::CPPTYPE_FLOAT:
                reflTo->SetFloat(&to, descTo, reflFrom->GetFloat(from, descFrom));
                break;
            case FieldDescriptor::CPPTYPE_BOOL:
                reflTo->SetBool(&to, descTo, reflFrom->GetBool(from, descFrom));
                break;
            case FieldDescriptor::CPPTYPE_ENUM:
                reflTo->SetEnum(&to, descTo, reflFrom->GetEnum(from, descFrom));
                break;
            case FieldDescriptor::CPPTYPE_STRING:
                reflTo->SetString(&to, descTo, reflFrom->GetString(from, descFrom));
                break;
            case FieldDescriptor::CPPTYPE_MESSAGE:
                throw yexception() << "not implemented"; // excluded in CheckFieldCopierTypes
                break;
            }
        }
    }

private:
    TVector<std::pair<const NProtoBuf::FieldDescriptor*, const NProtoBuf::FieldDescriptor*>> CopyDesc_;
};

/**
 * Note that this class does not require MsgFrom & MsgTo to be TNode.
 */
template <class MsgFrom, class MsgTo>
class TFieldCopier<MsgFrom, MsgTo, false, false> {
public:
    TFieldCopier(const TSortColumns& columns) : Columns_(columns) {}

    void operator()(const MsgFrom& from, MsgTo& to) const {
        for (auto& keyColumn : Columns_.Parts_) {
            to[keyColumn.EnsureAscending().Name()] = from[keyColumn.EnsureAscending().Name()];
        }
    }

private:
    // TODO(levysotsky): Replace with TColumnNames.
    TSortColumns Columns_;
};

/**
 * Note that this class basically assumes TNode as MsgFrom.
 */
template <class MsgFrom, class MsgTo>
class TFieldCopier<MsgFrom, MsgTo, false, true> {
public:
    TFieldCopier(const TSortColumns& columns) {
        for (auto& keyColumn : columns.Parts_) {
            const auto& columnName = keyColumn.EnsureAscending().Name();
            auto descTo = DescriptorByColumn<MsgTo>(columnName);
            NDetail::CheckFieldCopierTypes(columnName, descTo);
            CopyDesc_.emplace_back(columnName, descTo);
        }
    }

    // NOTE: see also io/proto_table_reader.cpp
    void operator()(const MsgFrom& from, MsgTo& to) const {
        using namespace NProtoBuf;
        auto reflTo = to.GetReflection();
        // NOTE: code below could be moved to cpp file if that would not hurt performance
        for (auto& desc : CopyDesc_) {
            // NOTE: right now, declaring [columnName, descTo] inside FOR loop will cause
            // incorrect clang 5 warning: "unused variable '' [-Werror,-Wunused-variable]"
            auto& [columnName, descTo] = desc;
            auto& column = from[columnName];
            switch(descTo->cpp_type()) {
            case FieldDescriptor::CPPTYPE_INT32:
                reflTo->SetInt32(&to, descTo, column.AsInt64());
                break;
            case FieldDescriptor::CPPTYPE_INT64:
                reflTo->SetInt64(&to, descTo, column.AsInt64());
                break;
            case FieldDescriptor::CPPTYPE_UINT32:
                reflTo->SetUInt32(&to, descTo, column.AsUint64());
                break;
            case FieldDescriptor::CPPTYPE_UINT64:
                reflTo->SetUInt64(&to, descTo, column.AsUint64());
                break;
            case FieldDescriptor::CPPTYPE_DOUBLE:
                reflTo->SetDouble(&to, descTo, column.AsDouble());
                break;
            case FieldDescriptor::CPPTYPE_FLOAT:
                reflTo->SetFloat(&to, descTo, column.AsDouble());
                break;
            case FieldDescriptor::CPPTYPE_BOOL:
                reflTo->SetBool(&to, descTo, column.AsBool());
                break;
            case FieldDescriptor::CPPTYPE_ENUM:
                if (auto* p = descTo->enum_type()->FindValueByName(column.AsString())) {
                    reflTo->SetEnum(&to, descTo, p);
                } else {
                    ythrow yexception() << "Failed to parse \"" << EscapeC(column.AsString())
                                        << "\" as " << descTo->enum_type()->full_name();
                }
                break;
            case FieldDescriptor::CPPTYPE_STRING:
                reflTo->SetString(&to, descTo, from[columnName].AsString());
                break;
            case FieldDescriptor::CPPTYPE_MESSAGE:
                throw yexception() << "not implemented"; // excluded in CheckFieldCopierTypes
                break;
            }
        }
    }

private:
    TVector<std::pair<TString, const NProtoBuf::FieldDescriptor*>> CopyDesc_;
};

/**
 * Note that this class does not clearly assume that MsgTo is TNode.
 */
template <class MsgFrom, class MsgTo>
class TFieldCopier<MsgFrom, MsgTo, true, false> {
public:
    TFieldCopier(const TSortColumns& columns) {
        for (auto& keyColumn : columns.Parts_) {
            const auto& columnName = keyColumn.EnsureAscending().Name();
            auto descFrom = DescriptorByColumn<MsgFrom>(columnName);
            NDetail::CheckFieldCopierTypes(columnName, descFrom);
            CopyDesc_.emplace_back(descFrom, columnName);
        }
    }

    void operator()(const MsgFrom& from, MsgTo& to) const {
        using namespace NProtoBuf;
        auto reflFrom = from.GetReflection();
        // NOTE: code below could be moved to cpp file if that would not hurt performance
        //       and we restrict that MsgTo to TNode
        for (auto& desc : CopyDesc_) {
            // NOTE: right now, declaring [descFrom, columnName] inside FOR loop will cause
            // incorrect clang 5 warning: "unused variable '' [-Werror,-Wunused-variable]"
            auto& [descFrom, columnName] = desc;
            switch(descFrom->cpp_type()) {
            case FieldDescriptor::CPPTYPE_INT32:
                to[columnName] = reflFrom->GetInt32(from, descFrom);
                break;
            case FieldDescriptor::CPPTYPE_INT64:
                to[columnName] = reflFrom->GetInt64(from, descFrom);
                break;
            case FieldDescriptor::CPPTYPE_UINT32:
                to[columnName] = reflFrom->GetUInt32(from, descFrom);
                break;
            case FieldDescriptor::CPPTYPE_UINT64:
                to[columnName] = reflFrom->GetUInt64(from, descFrom);
                break;
            case FieldDescriptor::CPPTYPE_DOUBLE:
                to[columnName] = reflFrom->GetDouble(from, descFrom);
                break;
            case FieldDescriptor::CPPTYPE_FLOAT:
                to[columnName] = reflFrom->GetFloat(from, descFrom);
                break;
            case FieldDescriptor::CPPTYPE_BOOL:
                to[columnName] = reflFrom->GetBool(from, descFrom);
                break;
            case FieldDescriptor::CPPTYPE_ENUM:
                to[columnName] = reflFrom->GetEnum(from, descFrom)->name();
                break;
            case FieldDescriptor::CPPTYPE_STRING:
                to[columnName] = reflFrom->GetString(from, descFrom);
                break;
            case FieldDescriptor::CPPTYPE_MESSAGE:
                throw yexception() << "not implemented"; // excluded in CheckFieldCopierTypes
                break;
            }
        }
    }

private:
    TVector<std::pair<const NProtoBuf::FieldDescriptor*, TString>> CopyDesc_;
};

/* NOTE: we can restrict MsgTo in above class to TNode and uncomment this:
    template <class MsgFrom, class MsgTo>
    class TFieldCopier<MsgFrom, MsgTo, true, false> {
        TFieldCopier(const TSortColumns&) = delete; // not implemented yet
    };
*/

// ===========================================================================
} // namespace NYT
// ===========================================================================
