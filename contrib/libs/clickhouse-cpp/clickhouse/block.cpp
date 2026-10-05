#include "block.h"

#include "exceptions.h"

#include <stdexcept>

namespace clickhouse {

Block::Iterator::Iterator(const Block& block)
    : block_(block)
    , idx_(0)
{
}

Block::Iterator::Iterator(const Block& block, Block::Iterator::ConstructAtEndTag /*at_end*/)
    : block_(block)
    , idx_(block.GetColumnCount())
{}

const std::string& Block::Iterator::Name() const {
    return block_.columns_[idx_].name;
}

TypeRef Block::Iterator::Type() const {
    return block_.columns_[idx_].column->Type();
}

ColumnRef Block::Iterator::Column() const {
    return block_.columns_[idx_].column;
}

bool Block::Iterator::Next() {
    ++idx_;
    return IsValid();
}

bool Block::Iterator::IsValid() const {
    return idx_ < block_.columns_.size();
}


Block::Block()
    : rows_(0)
{
}

Block::Block(size_t cols, size_t rows)
    : rows_(rows)
{
    columns_.reserve(cols);
}

Block::~Block() = default;

void Block::AppendColumn(const std::string& name, const ColumnRef& col) {
    if (columns_.empty()) {
        rows_ = col->Size();
    } else if (col->Size() != rows_) {
        throw ValidationError("all columns in block must have same count of rows. Name: ["+name+"], rows: ["+std::to_string(rows_)+"], columns: [" + std::to_string(col->Size())+"]");
    }

    columns_.push_back(ColumnItem{name, col});
}

/// Count of columns in the block.
size_t Block::GetColumnCount() const {
    return columns_.size();
}

const BlockInfo& Block::Info() const {
    return info_;
}

/// Set block info
void Block::SetInfo(BlockInfo info) {
    info_ = std::move(info);
}

/// Count of rows in the block.
size_t Block::GetRowCount() const {
    return rows_;
}

size_t Block::RefreshRowCount() {
    size_t rows = 0UL;

    for (size_t idx = 0UL; idx < columns_.size(); ++idx)
    {
       const std::string& name = columns_[idx].name;
       const ColumnRef& col = columns_[idx].column;

       if (idx == 0UL)
           rows = col->Size();
       else if (rows != col->Size())
           throw ValidationError("all columns in block must have same count of rows. Name: ["+name+"], rows: ["+std::to_string(rows)+"], columns: [" + std::to_string(col->Size())+"]");
    }

    rows_ = rows;
    return rows_;
}

void Block::Clear() {
    for (auto & c : columns_) {
        c.column->Clear();
    }

    RefreshRowCount();
}

void Block::Reserve(size_t new_cap) {
    for (auto & c : columns_) {
        c.column->Reserve(new_cap);
    }
}



ColumnRef Block::At(size_t idx) const {
    if (idx < columns_.size()) {
        return columns_[idx].column;
    }

    throw std::out_of_range("column index is out of range. Index: ["+std::to_string(idx)+"], columns: [" + std::to_string(columns_.size())+"]");
}

Block::Iterator Block::begin() const {
    return Iterator(*this);
}

Block::Iterator Block::end() const {
    return Iterator(*this, Iterator::ConstructAtEndTag{});
}

}
