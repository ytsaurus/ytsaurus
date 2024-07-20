#include "transforms.h"

namespace NRoren::NPrivate {

////////////////////////////////////////////////////////////////////////////////

class TAddTableIndexDoFn
    : public IDoFn<NYT::TNode, NYT::TNode>
{
public:
    TAddTableIndexDoFn() = default;

    TAddTableIndexDoFn(ssize_t tableIndex)
        : TableIndex_(tableIndex)
    {
        Y_ABORT_UNLESS(tableIndex >= 0);
    }

    void Do(const NYT::TNode& row, TOutput<NYT::TNode>& output) override
    {
        auto result = row;
        result["table_index"] = TableIndex_;
        output.Add(result);
    }

private:
    ssize_t TableIndex_ = 0;
    Y_SAVELOAD_DEFINE_OVERRIDE(TableIndex_);
};

class TAddTableIndexToProtoDoFn
    : public IDoFn<TKVProto, TKVProto>
{
public:
    TAddTableIndexToProtoDoFn() = default;

    TAddTableIndexToProtoDoFn(ssize_t tableIndex)
        : TableIndex_(tableIndex)
    {
        Y_ABORT_UNLESS(tableIndex >= 0);
    }

    void Do(const TKVProto& row, TOutput<TKVProto>& output) override
    {
        auto result = row;
        result.SetTableIndex(TableIndex_);
        output.Add(result);
    }

private:
    ssize_t TableIndex_ = 0;
    Y_SAVELOAD_DEFINE_OVERRIDE(TableIndex_);
};

////////////////////////////////////////////////////////////////////////////////

IRawParDoPtr CreateReadImpulseParDo(const std::vector<TTableNode*>& inputTables)
{
    Y_ENSURE(!inputTables.empty(), "Expected 'inputTables' to be nonempty");

    std::vector<TRowVtable> vtables;
    auto format = inputTables[0]->GetTableFormat();
    for (const auto& table : inputTables) {
        Y_ENSURE(table->GetTableFormat() == format, "Format of input tables is different");

        if (format == ETableFormat::Proto) {
            vtables.emplace_back(table->Vtable);
        }
    }

    if (format == ETableFormat::TNode) {
        return CreateReadNodeImpulseParDo(std::ssize(inputTables));
    } else {
        return CreateReadProtoImpulseParDo(std::move(vtables));
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NRoren::NPrivate

namespace NRoren {

////////////////////////////////////////////////////////////////////////////////

NPrivate::IRawParDoPtr CreateAddTableIndexProtoParDo(ssize_t index) {
    return NPrivate::MakeRawParDo(::MakeIntrusive<NPrivate::TAddTableIndexToProtoDoFn>(index));
}

NPrivate::IRawParDoPtr CreateAddTableIndexParDo(ssize_t index) {
    return NPrivate::MakeRawParDo(::MakeIntrusive<NPrivate::TAddTableIndexDoFn>(index));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NRoren
