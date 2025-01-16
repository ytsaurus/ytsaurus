#pragma once

#include "public.h"

#include <library/cpp/yt/logging/logger.h>

namespace NYT::NQueryClient {

////////////////////////////////////////////////////////////////////////////////

void CheckStackDepth();

////////////////////////////////////////////////////////////////////////////////

NLogging::TLogger MakeQueryLogger(TGuid queryId);
NLogging::TLogger MakeQueryLogger(TConstBaseQueryPtr query);

////////////////////////////////////////////////////////////////////////////////

void ThrowTypeMismatchError(
    EValueType lhsType,
    EValueType rhsType,
    TStringBuf source,
    TStringBuf lhsSource,
    TStringBuf rhsSource);

////////////////////////////////////////////////////////////////////////////////

//! Computes key index for a given column name.
int ColumnNameToKeyPartIndex(const TKeyColumns& keyColumns, const std::string& columnName);

//! Derives type of reference expression based on table column type.
//!
//! For historical reasons reference expressions used to have `wire type` of column i.e.
//! if column had `Int16` type its reference would have `Int64` type.
//! `DeriveReferenceType` keeps this behaviour for V1 types, but for V3 types actual type is returned.
NTableClient::TLogicalTypePtr ToQLType(const NTableClient::TLogicalTypePtr& columnType);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryClient
