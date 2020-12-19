#include "operation_archive_schema.h"

namespace NYT::NApi {

using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

TOrderedByIdTableDescriptor::TOrderedByIdTableDescriptor()
    : NameTable(New<TNameTable>())
    , Index(NameTable)
{ }

TOrderedByIdTableDescriptor::TIndex::TIndex(const TNameTablePtr& nameTable)
    : IdHash(nameTable->RegisterName("id_hash"))
    , IdHi(nameTable->RegisterName("id_hi"))
    , IdLo(nameTable->RegisterName("id_lo"))
    , State(nameTable->RegisterName("state"))
    , AuthenticatedUser(nameTable->RegisterName("authenticated_user"))
    , OperationType(nameTable->RegisterName("operation_type"))
    , Progress(nameTable->RegisterName("progress"))
    , Spec(nameTable->RegisterName("spec"))
    , BriefProgress(nameTable->RegisterName("brief_progress"))
    , BriefSpec(nameTable->RegisterName("brief_spec"))
    , StartTime(nameTable->RegisterName("start_time"))
    , FinishTime(nameTable->RegisterName("finish_time"))
    , FilterFactors(nameTable->RegisterName("filter_factors"))
    , Result(nameTable->RegisterName("result"))
    , Events(nameTable->RegisterName("events"))
    , Alerts(nameTable->RegisterName("alerts"))
    , SlotIndex(nameTable->RegisterName("slot_index"))
    , UnrecognizedSpec(nameTable->RegisterName("unrecognized_spec"))
    , FullSpec(nameTable->RegisterName("full_spec"))
    , RuntimeParameters(nameTable->RegisterName("runtime_parameters"))
    , SlotIndexPerPoolTree(nameTable->RegisterName("slot_index_per_pool_tree"))
    , TaskNames(nameTable->RegisterName("task_names"))
{ }

////////////////////////////////////////////////////////////////////////////////

TOrderedByStartTimeTableDescriptor::TOrderedByStartTimeTableDescriptor()
    : NameTable(New<TNameTable>())
    , Index(NameTable)
{ }

TOrderedByStartTimeTableDescriptor::TIndex::TIndex(const TNameTablePtr& nameTable)
    : StartTime(nameTable->RegisterName("start_time"))
    , IdHi(nameTable->RegisterName("id_hi"))
    , IdLo(nameTable->RegisterName("id_lo"))
    , OperationType(nameTable->RegisterName("operation_type"))
    , State(nameTable->RegisterName("state"))
    , AuthenticatedUser(nameTable->RegisterName("authenticated_user"))
    , FilterFactors(nameTable->RegisterName("filter_factors"))
    , Pool(nameTable->RegisterName("pool"))
    , Pools(nameTable->RegisterName("pools"))
    , HasFailedJobs(nameTable->RegisterName("has_failed_jobs"))
    , Acl(nameTable->RegisterName("acl"))
{ }

////////////////////////////////////////////////////////////////////////////////

TJobTableDescriptor::TJobTableDescriptor()
    : NameTable(New<TNameTable>())
    , Index(NameTable)
{ }

TJobTableDescriptor::TIndex::TIndex(const TNameTablePtr& nameTable)
    : OperationIdHi(nameTable->RegisterName("operation_id_hi"))
    , OperationIdLo(nameTable->RegisterName("operation_id_lo"))
    , JobIdHi(nameTable->RegisterName("job_id_hi"))
    , JobIdLo(nameTable->RegisterName("job_id_lo"))
    , Type(nameTable->RegisterName("type"))
    , State(nameTable->RegisterName("state"))
    , TransientState(nameTable->RegisterName("transient_state"))
    , StartTime(nameTable->RegisterName("start_time"))
    , FinishTime(nameTable->RegisterName("finish_time"))
    , UpdateTime(nameTable->RegisterName("update_time"))
    , Address(nameTable->RegisterName("address"))
    , Error(nameTable->RegisterName("error"))
    , Statistics(nameTable->RegisterName("statistics"))
    , BriefStatistics(nameTable->RegisterName("brief_statistics"))
    , StatisticsLz4(nameTable->RegisterName("statistics_lz4"))
    , Events(nameTable->RegisterName("events"))
    , StderrSize(nameTable->RegisterName("stderr_size"))
    , HasSpec(nameTable->RegisterName("has_spec"))
    , HasFailContext(nameTable->RegisterName("has_fail_context"))
    , FailContextSize(nameTable->RegisterName("fail_context_size"))
    , CoreInfos(nameTable->RegisterName("core_infos"))
    , JobCompetitionId(nameTable->RegisterName("job_competition_id"))
    , HasCompetitors(nameTable->RegisterName("has_competitors"))
    , ExecAttributes(nameTable->RegisterName("exec_attributes"))
    , TaskName(nameTable->RegisterName("task_name"))
    , PoolTree(nameTable->RegisterName("pool_tree"))
    , MonitoringDescriptor(nameTable->RegisterName("monitoring_descriptor"))
{ }

////////////////////////////////////////////////////////////////////////////////

TOperationIdTableDescriptor::TOperationIdTableDescriptor()
    : NameTable(New<TNameTable>())
    , Index(NameTable)
{ }

TOperationIdTableDescriptor::TIndex::TIndex(const TNameTablePtr& nameTable)
    : JobIdHi(nameTable->RegisterName("job_id_hi"))
    , JobIdLo(nameTable->RegisterName("job_id_lo"))
    , OperationIdHi(nameTable->RegisterName("operation_id_hi"))
    , OperationIdLo(nameTable->RegisterName("operation_id_lo"))
{}

////////////////////////////////////////////////////////////////////////////////

TJobSpecTableDescriptor::TJobSpecTableDescriptor()
    : NameTable(New<TNameTable>())
    , Index(NameTable)
{ }

TJobSpecTableDescriptor::TIndex::TIndex(const TNameTablePtr& nameTable)
    : JobIdHi(nameTable->RegisterName("job_id_hi"))
    , JobIdLo(nameTable->RegisterName("job_id_lo"))
    , Spec(nameTable->RegisterName("spec"))
    , SpecVersion(nameTable->RegisterName("spec_version"))
    , Type(nameTable->RegisterName("type"))
{ }

////////////////////////////////////////////////////////////////////////////////

TJobStderrTableDescriptor::TJobStderrTableDescriptor()
    : NameTable(New<TNameTable>())
    , Index(NameTable)
{ }

TJobStderrTableDescriptor::TIndex::TIndex(const TNameTablePtr& nameTable)
    : OperationIdHi(nameTable->RegisterName("operation_id_hi"))
    , OperationIdLo(nameTable->RegisterName("operation_id_lo"))
    , JobIdHi(nameTable->RegisterName("job_id_hi"))
    , JobIdLo(nameTable->RegisterName("job_id_lo"))
    , Stderr(nameTable->RegisterName("stderr"))
{ }

////////////////////////////////////////////////////////////////////////////////

TJobProfileTableDescriptor::TJobProfileTableDescriptor()
    : NameTable(New<TNameTable>())
    , Index(NameTable)
{ }

TJobProfileTableDescriptor::TIndex::TIndex(const TNameTablePtr& nameTable)
    : OperationIdHi(nameTable->RegisterName("operation_id_hi"))
    , OperationIdLo(nameTable->RegisterName("operation_id_lo"))
    , JobIdHi(nameTable->RegisterName("job_id_hi"))
    , JobIdLo(nameTable->RegisterName("job_id_lo"))
    , PartIndex(nameTable->RegisterName("part_index"))
    , ProfileType(nameTable->RegisterName("profile_type"))
    , ProfileBlob(nameTable->RegisterName("profile_blob"))
{ }

////////////////////////////////////////////////////////////////////////////////

TJobFailContextTableDescriptor::TJobFailContextTableDescriptor()
    : NameTable(New<TNameTable>())
    , Index(NameTable)
{ }

TJobFailContextTableDescriptor::TIndex::TIndex(const TNameTablePtr& nameTable)
    : OperationIdHi(nameTable->RegisterName("operation_id_hi"))
    , OperationIdLo(nameTable->RegisterName("operation_id_lo"))
    , JobIdHi(nameTable->RegisterName("job_id_hi"))
    , JobIdLo(nameTable->RegisterName("job_id_lo"))
    , FailContext(nameTable->RegisterName("fail_context"))
{ }

////////////////////////////////////////////////////////////////////////////////

TOperationAliasesTableDescriptor::TOperationAliasesTableDescriptor()
    : NameTable(New<TNameTable>())
    , Index(NameTable)
{ }

TOperationAliasesTableDescriptor::TIndex::TIndex(const TNameTablePtr& nameTable)
    : AliasHash(nameTable->RegisterName("alias_hash"))
    , Alias(nameTable->RegisterName("alias"))
    , OperationIdHi(nameTable->RegisterName("operation_id_hi"))
    , OperationIdLo(nameTable->RegisterName("operation_id_lo"))
{ }

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi
