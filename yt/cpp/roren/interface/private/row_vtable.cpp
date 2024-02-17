#include "row_vtable.h"

#include <util/ysaveload.h>

namespace NRoren::NPrivate {

////////////////////////////////////////////////////////////////////////////////

IRawCoderPtr CrashingCoderFactory()
{
    Y_ABORT("Internal error: CrashingCoderFactory is called");
}

TRowVtable CrashingGetVtableFactory()
{
    Y_ABORT("Internal error: CrashingGetVtableFactory is called");
}

////////////////////////////////////////////////////////////////////////////////


NYT::TNode SaveToNode(const std::vector<TRowVtable>& rowVtables)
{
    auto list = NYT::TNode::CreateList();
    for (const auto& rowVtable : rowVtables) {
        list.Add(SaveToNode(rowVtable));
    }
    return list;
}

NYT::TNode SaveToNode(const TRowVtable& rowVtable)
{
    TStringStream out;
    Save(&out, rowVtable);
    return out.Str();
}

std::vector<TRowVtable> LoadVtablesFromNode(const NYT::TNode& node) {
    std::vector<TRowVtable> rowVtables;

    auto list = node.AsList();
    rowVtables.reserve(list.size());

    for (const auto& item : list) {
        rowVtables.push_back(LoadVtableFromNode(item));
    }
    return rowVtables;
}

TRowVtable LoadVtableFromNode(const NYT::TNode& node)
{
    TStringInput in(node.AsString());
    TRowVtable result;
    Load(&in, result);
    return result;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NRoren::NPrivate

void TSerializer<NRoren::NPrivate::TRowVtable>::Save(IOutputStream* output, const NRoren::NPrivate::TRowVtable& rowVtable)
{
    ::Save(output, rowVtable.TypeName);
    ::Save(output, rowVtable.TypeHash);
    ::Save(output, static_cast<ui64>(rowVtable.DataSize));
    ::Save(output, reinterpret_cast<ui64>(rowVtable.DefaultConstructor));
    ::Save(output, reinterpret_cast<ui64>(rowVtable.Destructor));
    ::Save(output, reinterpret_cast<ui64>(rowVtable.CopyConstructor));
    ::Save(output, reinterpret_cast<ui64>(rowVtable.RawCoderFactory));
    ::Save(output, reinterpret_cast<ui64>(rowVtable.CopyToUniquePtr));
    ::Save(output, static_cast<ui64>(rowVtable.KeyOffset));
    ::Save(output, static_cast<ui64>(rowVtable.ValueOffset));
    ::Save(output, reinterpret_cast<ui64>(rowVtable.KeyVtableFactory));
    ::Save(output, reinterpret_cast<ui64>(rowVtable.ValueVtableFactory));
}

void TSerializer<NRoren::NPrivate::TRowVtable>::Load(
    IInputStream* input,
    NRoren::NPrivate::TRowVtable& rowVtable)
{
    using namespace NRoren::NPrivate;
    i64 dataSize, keyOffset, valueOffset;
    ui64 defaultConstructor, destructor, copyConstructor, rawCoderFactory, copyToUniquePtr, keyVtableFactory, valueVtableFactory;

    ::Load(input, rowVtable.TypeName);
    ::Load(input, rowVtable.TypeHash);
    ::Load(input, dataSize);
    ::Load(input, defaultConstructor);
    ::Load(input, destructor);
    ::Load(input, copyConstructor);
    ::Load(input, rawCoderFactory);
    ::Load(input, copyToUniquePtr);
    ::Load(input, keyOffset);
    ::Load(input, valueOffset);
    ::Load(input, keyVtableFactory);
    ::Load(input, valueVtableFactory);

    rowVtable.DataSize = dataSize;
    rowVtable.DefaultConstructor = reinterpret_cast<TRowVtable::TUniDataFunction>(defaultConstructor);
    rowVtable.Destructor = reinterpret_cast<TRowVtable::TUniDataFunction>(destructor);
    rowVtable.CopyConstructor = reinterpret_cast<TRowVtable::TCopyDataFunction>(copyConstructor);
    rowVtable.RawCoderFactory = reinterpret_cast<TRowVtable::TRawCoderFactoryFunction>(rawCoderFactory);
    rowVtable.CopyToUniquePtr = reinterpret_cast<TRowVtable::TCopyToUniquePtrFunction>(copyToUniquePtr);
    rowVtable.KeyOffset = keyOffset;
    rowVtable.ValueOffset = valueOffset;
    rowVtable.KeyVtableFactory = reinterpret_cast<TRowVtable::TRowVtableFactoryFunction>(keyVtableFactory);
    rowVtable.ValueVtableFactory = reinterpret_cast<TRowVtable::TRowVtableFactoryFunction>(valueVtableFactory);
}

////////////////////////////////////////////////////////////////////////////////
