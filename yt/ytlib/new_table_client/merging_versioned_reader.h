#pragma once

#include "new_interfaces.h"

#include <core/misc/ref.h>

namespace NYT {
namespace NVersionedTableClient {

////////////////////////////////////////////////////////////////////////////////

IReaderPtr CreateMergingVersionedReader(
    const TSchema& schema,
    TKeyColumns keyColumns,
    TNameTablePtr nameTable,
    const std::vector<IReaderPtr>& readers);

/*
class TMergingMvccReader
    : public virtual IReader
{
public: 
    TMergingMvccReader()
};
*/

////////////////////////////////////////////////////////////////////////////////

} // namespace NVersionedTableClient
} // namespace NYT
