#include "serialize.h"

#include "node_visitor.h"

#include "helpers.h"
#include "fluent.h"
#include <library/yson/consumer.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

void Serialize(const Stroka& value, IYsonConsumer* consumer)
{
    consumer->OnStringScalar(value);
}

void Serialize(const TStringBuf& value, IYsonConsumer* consumer)
{
    consumer->OnStringScalar(value);
}

void Serialize(const char* value, IYsonConsumer* consumer)
{
    consumer->OnStringScalar(value);
}

#define SERIALIZE_SIGNED(type) \
void Serialize(type value, IYsonConsumer* consumer) \
{ \
    consumer->OnInt64Scalar(static_cast<i64>(value)); \
}

#define SERIALIZE_UNSIGNED(type) \
void Serialize(type value, IYsonConsumer* consumer) \
{ \
    consumer->OnUint64Scalar(static_cast<ui64>(value)); \
}

SERIALIZE_SIGNED(signed char);
SERIALIZE_SIGNED(short);
SERIALIZE_SIGNED(int);
SERIALIZE_SIGNED(long);
SERIALIZE_SIGNED(long long);

SERIALIZE_UNSIGNED(unsigned char);
SERIALIZE_UNSIGNED(unsigned short);
SERIALIZE_UNSIGNED(unsigned int);
SERIALIZE_UNSIGNED(unsigned long);
SERIALIZE_UNSIGNED(unsigned long long);

#undef SERIALIZE_SIGNED
#undef SERIALIZE_UNSIGNED

void Serialize(double value, IYsonConsumer* consumer)
{
    consumer->OnDoubleScalar(value);
}

void Serialize(bool value, IYsonConsumer* consumer)
{
    // TODO: yson boolean
    consumer->OnStringScalar(value ? "true" : "false");
}

void Serialize(const TNode& node, IYsonConsumer* consumer)
{
    TNodeVisitor visitor(consumer);
    visitor.Visit(node);
}

void Serialize(const TKey& key, IYsonConsumer* consumer)
{
    BuildYsonFluently(consumer).List(key.Parts_);
}

void Serialize(const TKeyColumns& keyColumns, IYsonConsumer* consumer)
{
    BuildYsonFluently(consumer).List(keyColumns.Parts_);
}

void Serialize(const TReadLimit& readLimit, IYsonConsumer* consumer)
{
    BuildYsonFluently(consumer).BeginMap()
        .DoIf(readLimit.Key_.Defined(), [&] (TFluentMap fluent) {
            fluent.Item("key").Value(*readLimit.Key_);
        })
        .DoIf(readLimit.RowIndex_.Defined(), [&] (TFluentMap fluent) {
            fluent.Item("row_index").Value(*readLimit.RowIndex_);
        })
        .DoIf(readLimit.Offset_.Defined(), [&] (TFluentMap fluent) {
            fluent.Item("offset").Value(*readLimit.Offset_);
        })
    .EndMap();
}

bool IsTrivial(const TReadLimit& readLimit)
{
    return !readLimit.Key_ && !readLimit.RowIndex_ && !readLimit.Offset_;
}

void Serialize(const TReadRange& readRange, IYsonConsumer* consumer)
{
    BuildYsonFluently(consumer).BeginMap()
        .DoIf(!IsTrivial(readRange.LowerLimit_), [&] (TFluentMap fluent) {
            fluent.Item("lower_limit").Value(readRange.LowerLimit_);
        })
        .DoIf(!IsTrivial(readRange.UpperLimit_), [&] (TFluentMap fluent) {
            fluent.Item("upper_limit").Value(readRange.UpperLimit_);
        })
        .DoIf(!IsTrivial(readRange.Exact_), [&] (TFluentMap fluent) {
            fluent.Item("exact").Value(readRange.Exact_);
        })
    .EndMap();
}

void Serialize(const TRichYPath& path, IYsonConsumer* consumer)
{
    BuildYsonFluently(consumer).BeginAttributes()
        .DoIf(!path.Ranges_.empty(), [&] (TFluentAttributes fluent) {
            fluent.Item("ranges").List(path.Ranges_);
        })
        .DoIf(!path.Columns_.Parts_.empty(), [&] (TFluentAttributes fluent) {
            fluent.Item("columns").Value(path.Columns_);
        })
        .DoIf(path.Append_.Defined(), [&] (TFluentAttributes fluent) {
            fluent.Item("append").Value(path.Append_.GetRef());
        })
        .DoIf(!path.SortedBy_.Parts_.empty(), [&] (TFluentAttributes fluent) {
            fluent.Item("sorted_by").Value(path.SortedBy_);
        })
        .DoIf(path.Teleport_.Defined(), [&] (TFluentAttributes fluent) {
            fluent.Item("teleport").Value(path.Teleport_.GetRef());
        })
        .DoIf(path.Primary_.Defined(), [&] (TFluentAttributes fluent) {
            fluent.Item("primary").Value(path.Primary_.GetRef());
        })
        .DoIf(path.Foreign_.Defined(), [&] (TFluentAttributes fluent) {
            fluent.Item("foreign").Value(path.Foreign_.GetRef());
        })
        .DoIf(path.RowCountLimit_.Defined(), [&] (TFluentAttributes fluent) {
            fluent.Item("row_count_limit").Value(path.RowCountLimit_.GetRef());
        })
        .DoIf(path.FileName_.Defined(), [&] (TFluentAttributes fluent) {
            fluent.Item("file_name").Value(path.FileName_.GetRef());
        })
        .DoIf(path.Executable_.Defined(), [&] (TFluentAttributes fluent) {
            fluent.Item("executable").Value(path.Executable_.GetRef());
        })
        .DoIf(path.Format_.Defined(), [&] (TFluentAttributes fluent) {
            fluent.Item("format").Value(path.Format_.GetRef());
        })
        .DoIf(path.Schema_.Defined(), [&] (TFluentAttributes fluent) {
            fluent.Item("schema").Value(path.Schema_.GetRef());
        })
    .EndAttributes()
    .Value(path.Path_);
}

void Serialize(const TAttributeFilter& filter, IYsonConsumer* consumer)
{
    BuildYsonFluently(consumer).List(filter.Attributes_);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
