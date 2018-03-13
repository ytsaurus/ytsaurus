#pragma once

#include <yt/ytlib/table_client/name_table.h>

namespace NYT {
namespace NApi {

using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

struct TOrderedByIdTableDescriptor
{
    explicit TOrderedByIdTableDescriptor(const TNameTablePtr& nameTable)
        : IdHash(nameTable->RegisterName("id_hash"))
        , IdHi(nameTable->RegisterName("id_hi"))
        , IdLo(nameTable->RegisterName("id_lo"))
        , State(nameTable->RegisterName("state"))
        , AuthenticatedUser(nameTable->RegisterName("authenticated_user"))
        , OperationType(nameTable->RegisterName("operation_type"))
        , Progress(nameTable->RegisterName("progress"))
        , Spec(nameTable->RegisterName("spec"))
        , FullSpec(nameTable->RegisterName("full_spec"))
        , UnrecognizedSpec(nameTable->RegisterName("unrecognized_spec"))
        , BriefProgress(nameTable->RegisterName("brief_progress"))
        , BriefSpec(nameTable->RegisterName("brief_spec"))
        , StartTime(nameTable->RegisterName("start_time"))
        , FinishTime(nameTable->RegisterName("finish_time"))
        , FilterFactors(nameTable->RegisterName("filter_factors"))
        , Result(nameTable->RegisterName("result"))
        , Events(nameTable->RegisterName("events"))
    { }

    const int IdHash;
    const int IdHi;
    const int IdLo;
    const int State;
    const int AuthenticatedUser;
    const int OperationType;
    const int Progress;
    const int Spec;
    const int FullSpec;
    const int UnrecognizedSpec;
    const int BriefProgress;
    const int BriefSpec;
    const int StartTime;
    const int FinishTime;
    const int FilterFactors;
    const int Result;
    const int Events;
};

////////////////////////////////////////////////////////////////////////////////

struct TStderrArchiveIds
{
    explicit TStderrArchiveIds(const TNameTablePtr& nameTable)
        : OperationIdHi(nameTable->RegisterName("operation_id_hi"))
        , OperationIdLo(nameTable->RegisterName("operation_id_lo"))
        , JobIdHi(nameTable->RegisterName("job_id_hi"))
        , JobIdLo(nameTable->RegisterName("job_id_lo"))
        , Stderr(nameTable->RegisterName("stderr"))
    { }

    const int OperationIdHi;
    const int OperationIdLo;
    const int JobIdHi;
    const int JobIdLo;
    const int Stderr;
};

////////////////////////////////////////////////////////////////////////////////

struct TJobTableDescriptor
{
    TJobTableDescriptor();

    struct TIndex
    {
        explicit TIndex(const NTableClient::TNameTablePtr& n);

        const int OperationIdHi;
        const int OperationIdLo;
        const int JobIdHi;
        const int JobIdLo;
        const int Type;
        const int State;
        const int TransientState;
        const int StartTime;
        const int FinishTime;
        const int UpdateTime;
        const int Address;
        const int Error;
        const int Statistics;
        const int Events;
    };

    const NTableClient::TNameTablePtr NameTable;
    const TIndex Ids;
};

struct TJobSpecTableDescriptor
{
    TJobSpecTableDescriptor();

    struct TIndex
    {
        explicit TIndex(const NTableClient::TNameTablePtr& n);

        const int JobIdHi;
        const int JobIdLo;
        const int Spec;
        const int SpecVersion;
        const int Type;
    };

    const NTableClient::TNameTablePtr NameTable;
    const TIndex Ids;
};


////////////////////////////////////////////////////////////////////////////////

} //namespace NAPI
} //namespace NYT
