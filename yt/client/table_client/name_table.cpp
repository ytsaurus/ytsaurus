#include "name_table.h"
#include "schema.h"

#include <yt/client/table_client/proto/chunk_meta.pb.h>

namespace NYT {
namespace NTableClient {

////////////////////////////////////////////////////////////////////////////////

TNameTablePtr TNameTable::FromSchema(const TTableSchema& schema)
{
    auto nameTable = New<TNameTable>();
    for (const auto& column : schema.Columns()) {
        nameTable->RegisterName(column.Name());
    }
    return nameTable;
}

TNameTablePtr TNameTable::SafeFromSchema(const TTableSchema& schema)
{
    ValidateColumnUniqueness(schema);

    auto nameTable = New<TNameTable>();
    for (const auto& column : schema.Columns()) {
        nameTable->RegisterName(column.Name());
    }
    return nameTable;
}

TNameTablePtr TNameTable::FromKeyColumns(const TKeyColumns& keyColumns)
{
    auto nameTable = New<TNameTable>();
    for (const auto& name : keyColumns) {
        nameTable->RegisterName(name);
    }
    return nameTable;
}

int TNameTable::GetSize() const
{
    TGuard<TSpinLock> guard(SpinLock_);
    return IdToName_.size();
}

i64 TNameTable::GetByteSize() const
{
    TGuard<TSpinLock> guard(SpinLock_);
    return ByteSize_;
}

void TNameTable::SetEnableColumnNameValidation()
{
    TGuard<TSpinLock> guard(SpinLock_);
    EnableColumnNameValidation_ = true;
}

std::optional<int> TNameTable::FindId(TStringBuf name) const
{
    TGuard<TSpinLock> guard(SpinLock_);
    auto it = NameToId_.find(name);
    if (it == NameToId_.end()) {
        return std::nullopt;
    } else {
        return std::make_optional(it->second);
    }
}

int TNameTable::GetIdOrThrow(TStringBuf name) const
{
    auto optionalId = FindId(name);
    if (!optionalId) {
        THROW_ERROR_EXCEPTION("No such column %Qv", name);
    }
    return *optionalId;
}

int TNameTable::GetId(TStringBuf name) const
{
    auto index = FindId(name);
    YCHECK(index);
    return *index;
}

TStringBuf TNameTable::GetName(int id) const
{
    TGuard<TSpinLock> guard(SpinLock_);
    YCHECK(id >= 0 && id < IdToName_.size());
    return IdToName_[id];
}

int TNameTable::RegisterName(TStringBuf name)
{
    TGuard<TSpinLock> guard(SpinLock_);
    return DoRegisterName(name);
}

int TNameTable::RegisterNameOrThrow(TStringBuf name)
{
    TGuard<TSpinLock> guard(SpinLock_);
    auto optionalId = NameToId_.find(name);
    if (optionalId != NameToId_.end()) {
        THROW_ERROR_EXCEPTION("Cannot register column %Qv: column already exists", name);
    }
    return DoRegisterName(name);
}

int TNameTable::GetIdOrRegisterName(TStringBuf name)
{
    TGuard<TSpinLock> guard(SpinLock_);
    auto it = NameToId_.find(name);
    if (it == NameToId_.end()) {
        return DoRegisterName(name);
    } else {
        return it->second;
    }
}

int TNameTable::DoRegisterName(TStringBuf name)
{
    int id = IdToName_.size();

    if (id >= MaxColumnId) {
        THROW_ERROR_EXCEPTION(
            EErrorCode::CorruptedNameTable,
            "Cannot register column %Qv: column limit exceeded",
            name)
            << TErrorAttribute("max_column_id", MaxColumnId);
    }

    if (EnableColumnNameValidation_ && name.length() > MaxColumnNameLength) {
        THROW_ERROR_EXCEPTION(
            EErrorCode::CorruptedNameTable,
            "Cannot register column %Qv: column name is too long",
            name)
            << TErrorAttribute("max_column_name_length", MaxColumnNameLength);
    }

    IdToName_.emplace_back(name);
    const auto& savedName = IdToName_.back();
    YCHECK(NameToId_.insert(std::make_pair(savedName, id)).second);
    ByteSize_ += savedName.length();
    return id;
}

////////////////////////////////////////////////////////////////////////////////

TNameTableReader::TNameTableReader(TNameTablePtr nameTable)
    : NameTable_(std::move(nameTable))
{
    Fill();
}

bool TNameTableReader::TryGetName(int id, TStringBuf& name) const
{
    if (id < 0) {
        return false;
    }

    if (id >= IdToNameCache_.size()) {
        Fill();

        if (id >= IdToNameCache_.size()) {
            return false;
        }
    }

    name = IdToNameCache_[id];

    return true;
}

TStringBuf TNameTableReader::GetName(int id) const
{
    Y_ASSERT(id >= 0);
    if (id >= IdToNameCache_.size()) {
        Fill();
    }

    Y_ASSERT(id < IdToNameCache_.size());
    return IdToNameCache_[id];
}

int TNameTableReader::GetSize() const
{
    Fill();
    return static_cast<int>(IdToNameCache_.size());
}

void TNameTableReader::Fill() const
{
    int thisSize = static_cast<int>(IdToNameCache_.size());
    int underlyingSize = NameTable_->GetSize();
    for (int id = thisSize; id < underlyingSize; ++id) {
        IdToNameCache_.push_back(TString(NameTable_->GetName(id)));
    }
}

////////////////////////////////////////////////////////////////////////////////

TNameTableWriter::TNameTableWriter(TNameTablePtr nameTable)
    : NameTable_(std::move(nameTable))
{ }

std::optional<int> TNameTableWriter::FindId(TStringBuf name) const
{
    auto it = NameToId_.find(name);
    if (it != NameToId_.end()) {
        return it->second;
    }

    auto optionalId = NameTable_->FindId(name);
    if (optionalId) {
        Names_.push_back(TString(name));
        YCHECK(NameToId_.insert(std::make_pair(Names_.back(), *optionalId)).second);
    }
    return optionalId;
}

int TNameTableWriter::GetIdOrThrow(TStringBuf name) const
{
    auto optionalId = FindId(name);
    if (!optionalId) {
        THROW_ERROR_EXCEPTION("No such column %Qv", name);
    }
    return *optionalId;
}

int TNameTableWriter::GetIdOrRegisterName(TStringBuf name)
{
    auto it = NameToId_.find(name);
    if (it != NameToId_.end()) {
        return it->second;
    }

    auto id = NameTable_->GetIdOrRegisterName(name);
    Names_.push_back(TString(name));
    YCHECK(NameToId_.insert(std::make_pair(Names_.back(), id)).second);
    return id;
}

////////////////////////////////////////////////////////////////////////////////

void ToProto(NProto::TNameTableExt* protoNameTable, const TNameTablePtr& nameTable)
{
    protoNameTable->clear_names();
    for (int id = 0; id < nameTable->GetSize(); ++id) {
        auto name = nameTable->GetName(id);
        protoNameTable->add_names(name.data(), name.length());
    }
}

void FromProto(TNameTablePtr* nameTable, const NProto::TNameTableExt& protoNameTable)
{
    *nameTable = New<TNameTable>();
    for (const auto& name : protoNameTable.names()) {
        (*nameTable)->RegisterName(name);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT

