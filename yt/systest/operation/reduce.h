#pragma once

#include <yt/systest/operation.h>

namespace NYT::NTest {

class TMapperReducer : public IReducer
{
public:
    TMapperReducer(
        const TTable& input,
        std::unique_ptr<IMultiMapper> inner);

    TMapperReducer(
        const TTable& input,
        const TMapperReducer& proto);

    virtual TRange<int> InputColumns() const override;
    virtual TRange<TDataColumn> OutputColumns() const override;

    virtual std::vector<std::vector<TNode>> Run(TCallState* state, TRange<TRange<TNode>> input) const override;
    virtual void ToProto(NProto::TReducer* proto) const override;
};

////////////////////////////////////////////////////////////////////////////////

class TSequenceReducer : public IReducer
{
public:
    TSequenceReducer(
        const TTable& input,
        std::vector<std::unique_ptr<IReducer>> operations);

    TSequenceReducer(
        const TTable& input,
        const NProto::TSequenceReducer& proto);

    virtual TRange<int> InputColumns() const override;
    virtual TRange<TDataColumn> OutputColumns() const override;

    virtual std::vector<std::vector<TNode>> Run(TCallState* state, TRange<TRange<TNode>> input) const override;
    virtual void ToProto(NProto::TReducer* proto) const override;
};

////////////////////////////////////////////////////////////////////////////////

class TSumReducer : public IReducer
{
public:
    TSumReducer(const TTable& input, int columnIndex, TDataColumn outputColumn);
    TSumReducer(const TTable& input, const NProto::TSumReducer& proto);

    virtual TRange<int> InputColumns() const override;
    virtual TRange<TDataColumn> OutputColumns() const override;

    virtual std::vector<std::vector<TNode>> Run(TCallState* state, TRange<TRange<TNode>> input) const override;
    virtual void ToProto(NProto::TReducer* proto) const override;
};

////////////////////////////////////////////////////////////////////////////////

class TConcatenateColumnsReducer : public IReducer
{
public:
    TConcatenateColumnsReducer(const TTable& input, std::vector<std::unique_ptr<IReducer>> operations);

    TConcatenateColumnsReducer(const TTable& input, const NProto::TConcatenateColumnsReducer& proto);

    virtual TRange<int> InputColumns() const override;
    virtual TRange<TDataColumn> OutputColumns() const override;

    virtual std::vector<std::vector<TNode>> Run(TCallState* state, TRange<TRange<TNode>> input) const override;
    virtual void ToProto(NProto::TReducer* proto) const override;
};

}  // namespace NYT::NTest
