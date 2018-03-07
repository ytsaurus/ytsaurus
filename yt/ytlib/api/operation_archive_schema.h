#pragma once

#include <yt/ytlib/table_client/name_table.h>

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
        const int SlotIndex;
        const int UnrecognizedSpec;
        const int FullSpec;
    };

    const NTableClient::TNameTablePtr NameTable;
    const TIndex Ids;
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
    };

    const NTableClient::TNameTablePtr NameTable;
    const TIndex Ids;
};

////////////////////////////////////////////////////////////////////////////////

struct TStderrsTableDescriptor
{
    TStderrsTableDescriptor();

    struct TIndex
    {
        explicit TIndex(const NTableClient::TNameTablePtr& nameTable);

        const int OperationIdHi;
        const int OperationIdLo;
        const int JobIdHi;
        const int JobIdLo;
        const int Stderr;
    };

    const NTableClient::TNameTablePtr NameTable;
    const TIndex Ids;
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
