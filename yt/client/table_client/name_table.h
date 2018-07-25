#pragma once

#include "public.h"

#include <yt/core/misc/nullable.h>

namespace NYT {
namespace NTableClient {

////////////////////////////////////////////////////////////////////////////////

//! A thread-safe id-to-name mapping.
class TNameTable
    : public virtual TRefCounted
{
public:
    static TNameTablePtr FromSchema(const TTableSchema& schema);
    static TNameTablePtr FromKeyColumns(const TKeyColumns& keyColumns);

    int GetSize() const;
    i64 GetByteSize() const;

    void SetEnableColumnNameValidation();

    TNullable<int> FindId(TStringBuf name) const;
    int GetIdOrThrow(TStringBuf name) const;
    int GetId(TStringBuf name) const;
    int RegisterName(TStringBuf name);
    int GetIdOrRegisterName(TStringBuf name);

    TStringBuf GetName(int id) const;

private:
    TSpinLock SpinLock_;

    bool EnableColumnNameValidation_ = false;

    std::vector<TString> IdToName_;
    THashMap<TStringBuf, int> NameToId_; // String values are owned by IdToName_.
    i64 ByteSize_ = 0;

    int DoRegisterName(TStringBuf name);

};

DEFINE_REFCOUNTED_TYPE(TNameTable)

////////////////////////////////////////////////////////////////////////////////

//! A non thread-safe read-only wrapper for TNameTable.
class TNameTableReader
    : private TNonCopyable
{
public:
    explicit TNameTableReader(TNameTablePtr nameTable);

    bool TryGetName(int id, TStringBuf& name) const;
    TStringBuf GetName(int id) const;
    int GetSize() const;

private:
    const TNameTablePtr NameTable_;

    mutable std::vector<TString> IdToNameCache_;

    void Fill() const;

};

////////////////////////////////////////////////////////////////////////////////

//! A non thread-safe read-write wrapper for TNameTable.
class TNameTableWriter
{
public:
    explicit TNameTableWriter(TNameTablePtr nameTable);

    TNullable<int> FindId(TStringBuf name) const;
    int GetIdOrThrow(TStringBuf name) const;
    int GetIdOrRegisterName(TStringBuf name);

private:
    const TNameTablePtr NameTable_;

    mutable std::vector<TString> Names_;
    mutable THashMap<TStringBuf, int> NameToId_; // String values are owned by Names_.

};

////////////////////////////////////////////////////////////////////////////////

void ToProto(NProto::TNameTableExt* protoNameTable, const TNameTablePtr& nameTable);
void FromProto(TNameTablePtr* nameTable, const NProto::TNameTableExt& protoNameTable);

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
