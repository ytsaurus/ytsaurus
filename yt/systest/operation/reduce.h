#pragma once

#include <yt/systest/operation.h>

namespace NYT::NTest {

class TSumReducer : public IReducer
{
public:
    TSumReducer(const TTable& input, int columnIndex, TDataColumn outputColumn);
    TSumReducer(const TTable& input, const NProto::TSumReducer& proto);

    virtual TRange<int> InputColumns() const override;
    virtual TRange<TDataColumn> OutputColumns() const override;

    virtual void StartRange(TCallState* state, TRange<TNode> key) override;
    virtual void ProcessRow(TCallState* state, TRange<TNode> input) override;
    virtual std::vector<std::vector<TNode>> FinishRange(TCallState* state) override;
    virtual void ToProto(NProto::TReducer* proto) const override;

private:
    int InputColumnIndex_[1];
    TDataColumn OutputColumns_[1];
    int64_t Result_;
};

////////////////////////////////////////////////////////////////////////////////

class TSumHashReducer : public IReducer
{
public:
    TSumHashReducer(const TTable& input, std::vector<int> indices, TDataColumn outputColumn);
    TSumHashReducer(const TTable& input, const NProto::TSumHashReducer& proto);

    virtual TRange<int> InputColumns() const override;
    virtual TRange<TDataColumn> OutputColumns() const override;

    virtual void StartRange(TCallState* state, TRange<TNode> key) override;
    virtual void ProcessRow(TCallState* state, TRange<TNode> input) override;
    virtual std::vector<std::vector<TNode>> FinishRange(TCallState* state) override;
    virtual void ToProto(NProto::TReducer* proto) const override;

private:
    std::vector<int> InputColumns_;
    TDataColumn OutputColumns_[1];
    int64_t Result_;
};

////////////////////////////////////////////////////////////////////////////////

class TConcatenateColumnsReducer : public IReducer
{
public:
    TConcatenateColumnsReducer(const TTable& input, std::vector<std::unique_ptr<IReducer>> operations);

    TConcatenateColumnsReducer(const TTable& input, const NProto::TConcatenateColumnsReducer& proto);

    virtual TRange<int> InputColumns() const override;
    virtual TRange<TDataColumn> OutputColumns() const override;

    virtual void StartRange(TCallState* state, TRange<TNode> key) override;
    virtual void ProcessRow(TCallState* state, TRange<TNode> input) override;
    virtual std::vector<std::vector<TNode>> FinishRange(TCallState* state) override;
    virtual void ToProto(NProto::TReducer* proto) const override;

private:
    std::vector<std::unique_ptr<IReducer>> Operations_;
    std::vector<TDataColumn> OutputColumns_;
    std::vector<int> InputColumns_;
};

}  // namespace NYT::NTest
