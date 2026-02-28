#pragma once

#include "schemaless_block_reader.h"

#include <library/cpp/yt/misc/enum.h>

namespace NYT::NTableClient {

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(ENameTableAffinity,
    (Reader)
    (Chunk)
);

//! In the most general form, unversioned row looks like this:
//! +------------+-------+--------------+------------------+------------------+--------------+
//! |    type    | keys  | widened keys | schemaful values | schemaless values| extra values |
//! +------------+-------+--------------+------------------+------------------+--------------+
//! | name table | chunk |    reader    |      chunk       |      reader      |    reader    |
//! +------------+-------+--------------+------------------+------------------+--------------+
//! This class aims to tell you which name table should be used for a particular value
//! (or, in context of the reader, should id be remapped).
//!
//! NB(coteeq): There is no distinction between schemaless values and extra values
//! in this class. Mostly because the border between them is hard to extract from
//! THorizontalBlockReader with the current interface and I am not really ready
//! to refactor that. This does not affect any logic because the have the same
//! name table affinity anyway.
class TUnversionedRowLayout
{
public:
    TUnversionedRowLayout(
        const TKeyWideningOptions& keyWideningOptions,
        int totalColumnCount);

    //! Prefix may not obey chunk's schema if there is non-trivial key widening.
    bool IsPrefixSchemaful() const;
    ENameTableAffinity GetNameTableAffinity(int columnIndex) const;

    std::pair<int, int> GetKeyWideningRange() const;

private:
    int KeyWideningStart_ = 0;
    int KeyWideningEnd_ = 0;
    int ExtraValuesOffset_ = 0;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
