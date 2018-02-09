#include "operation_archive_schema.h"

namespace NYT {
namespace NApi {

////////////////////////////////////////////////////////////////////////////////

TJobTableDescriptor::TJobTableDescriptor()
    : NameTable(New<TNameTable>())
    , Ids(NameTable)
{ }

TJobTableDescriptor::TIndex::TIndex(const TNameTablePtr& n)
    : OperationIdHi(n->RegisterName("operation_id_hi"))
    , OperationIdLo(n->RegisterName("operation_id_lo"))
    , JobIdHi(n->RegisterName("job_id_hi"))
    , JobIdLo(n->RegisterName("job_id_lo"))
    , Type(n->RegisterName("type"))
    , State(n->RegisterName("state"))
    , TransientState(n->RegisterName("transient_state"))
    , StartTime(n->RegisterName("start_time"))
    , FinishTime(n->RegisterName("finish_time"))
    , Address(n->RegisterName("address"))
    , Error(n->RegisterName("error"))
    , Statistics(n->RegisterName("statistics"))
    , Events(n->RegisterName("events"))
{ }

////////////////////////////////////////////////////////////////////////////////

TJobSpecTableDescriptor::TJobSpecTableDescriptor()
    : NameTable(New<TNameTable>())
    , Ids(NameTable)
{ }

TJobSpecTableDescriptor::TIndex::TIndex(const NTableClient::TNameTablePtr& n)
    : JobIdHi(n->RegisterName("job_id_hi"))
    , JobIdLo(n->RegisterName("job_id_lo"))
    , Spec(n->RegisterName("spec"))
    , SpecVersion(n->RegisterName("spec_version"))
    , Type(n->RegisterName("type"))
{ }

////////////////////////////////////////////////////////////////////////////////

} // namespace NApi
} // namespace NYT
