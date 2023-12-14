#pragma once

#include <library/cpp/yson/node/node.h>
#include <library/cpp/yt/memory/range.h>

#include <yt/systest/proto/operation.pb.h>
#include <yt/systest/proto/run_spec.pb.h>
#include <yt/systest/table.h>

#include <optional>
#include <random>

namespace NYT::NTest {

struct TCallState
{
    std::optional<std::mt19937> RandomEngine;
};

////////////////////////////////////////////////////////////////////////////////

class IOperation
{
public:
    explicit IOperation(const TTable& input);
    virtual ~IOperation();

    const TTable& InputTable() const;

    virtual TRange<int> InputColumns() const = 0;
    virtual TRange<TDataColumn> OutputColumns() const = 0;

protected:
    const TTable& Input_;
};

////////////////////////////////////////////////////////////////////////////////

class IRowMapper : public IOperation
{
public:
    explicit IRowMapper(const TTable& inputTable);

    virtual std::vector<TNode> Run(TCallState* state, TRange<TNode> input) const = 0;
    virtual void ToProto(NProto::TRowMapper* proto) const = 0;
};

////////////////////////////////////////////////////////////////////////////////

class IMultiMapper : public IOperation
{
public:
    explicit IMultiMapper(const TTable& table);

    virtual std::vector<std::vector<TNode>> Run(TCallState* state, TRange<TNode> input) const = 0;
    virtual void ToProto(NProto::TMultiMapper* proto) const = 0;
};

////////////////////////////////////////////////////////////////////////////////

class IReducer : public IOperation
{
public:
    explicit IReducer(const TTable& table);

    virtual std::vector<std::vector<TNode>> Run(TCallState* state, TRange<TRange<TNode>> input) const = 0;
    virtual void ToProto(NProto::TReducer* proto) const = 0;
};

////////////////////////////////////////////////////////////////////////////////

struct TSortOperation
{
    std::vector<TString> SortBy;
};

////////////////////////////////////////////////////////////////////////////////

struct TReduceOperation
{
    std::unique_ptr<IReducer> Reducer;
    std::vector<TString> ReduceBy;
};

////////////////////////////////////////////////////////////////////////////////

void ToProto(NProto::TSortRunSpec *proto, const TTable& table, const TSortOperation& operation);
void FromProto(TTable* table, TSortOperation* operation, const NProto::TSortRunSpec& proto);

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<IRowMapper> CreateFromProto(
    const TTable& input,
    const NProto::TRowMapper& operationProto);

std::unique_ptr<IMultiMapper> CreateFromProto(
    const TTable& input,
    const NProto::TMultiMapper& operationProto);

std::unique_ptr<IReducer> CreateFromProto(
    const TTable& input,
    const NProto::TReducer& operationProto);

}  // namespace NYT::NTest
