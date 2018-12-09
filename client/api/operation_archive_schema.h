#pragma once

#include <yt/client/table_client/name_table.h>

namespace NYT {
namespace NApi {

////////////////////////////////////////////////////////////////////////////////

struct TOrderedByIdTableDescriptor
{
    TOrderedByIdTableDescriptor();

    struct TIndex
    {
        explicit TIndex(const NTableClient::TNameTablePtr& nameTable);

        const int IdHash;
        const int IdHi;
        const int IdLo;
        const int State;
        const int AuthenticatedUser;
        const int OperationType;
        const int Progress;
        const int Spec;
        const int BriefProgress;
        const int BriefSpec;
        const int StartTime;
        const int FinishTime;
        const int FilterFactors;
        const int Result;
        const int Events;
        const int Alerts;
        const int SlotIndex; // TODO(renadeen): delete this column when version with this comment will be on every cluster
        const int UnrecognizedSpec;
        const int FullSpec;
        const int RuntimeParameters;
        const int SlotIndexPerPoolTree;
    };

    const NTableClient::TNameTablePtr NameTable;
    const TIndex Index;
};

////////////////////////////////////////////////////////////////////////////////

struct TOrderedByStartTimeTableDescriptor
{
    TOrderedByStartTimeTableDescriptor();

    struct TIndex
    {
        explicit TIndex(const NTableClient::TNameTablePtr& nameTable);

        const int StartTime;
        const int IdHi;
        const int IdLo;
        const int OperationType;
        const int State;
        const int AuthenticatedUser;
        const int FilterFactors;
        const int Pool;
        const int Pools;
        const int HasFailedJobs;
    };

    const NTableClient::TNameTablePtr NameTable;
    const TIndex Index;
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
        const int StderrSize;
        const int HasSpec;
        const int HasFailContext;
        const int FailContextSize;
    };

    const NTableClient::TNameTablePtr NameTable;
    const TIndex Index;
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
    const TIndex Index;
};

struct TJobStderrTableDescriptor
{
    TJobStderrTableDescriptor();

    struct TIndex
    {
        explicit TIndex(const NTableClient::TNameTablePtr& n);

        const int OperationIdHi;
        const int OperationIdLo;
        const int JobIdHi;
        const int JobIdLo;
        const int Stderr;
    };

    const NTableClient::TNameTablePtr NameTable;
    const TIndex Index;
};

struct TJobFailContextTableDescriptor
{
    TJobFailContextTableDescriptor();

    struct TIndex
    {
        explicit TIndex(const NTableClient::TNameTablePtr& n);

        const int OperationIdHi;
        const int OperationIdLo;
        const int JobIdHi;
        const int JobIdLo;
        const int FailContext;
    };

    const NTableClient::TNameTablePtr NameTable;
    const TIndex Index;
};

struct TOperationAliasesTableDescriptor
{
    TOperationAliasesTableDescriptor();

    struct TIndex
    {
        explicit TIndex(const NTableClient::TNameTablePtr& n);

        const int AliasHash;
        const int Alias;
        const int OperationIdHi;
        const int OperationIdLo;
    };

    const NTableClient::TNameTablePtr NameTable;
    const TIndex Index;
};

////////////////////////////////////////////////////////////////////////////////

} //namespace NAPI
} //namespace NYT
