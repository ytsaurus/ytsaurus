#pragma once

#include <yt/cpp/roren/interface/type_tag.h>

#include <bigrt/lib/writer/base/factory.h>
#include <bigrt/lib/writer/base/writer.h>

namespace NRoren::NPrivate {

////////////////////////////////////////////////////////////////////////////////

class TCompositeBigRtWriter
    : public NBigRT::IWriter
{
public:
    using TWriterMap = THashMap<std::pair<std::type_index, TString>, NBigRT::IWriterPtr>;

public:
    explicit TCompositeBigRtWriter(TWriterMap writerMap);

    template <typename T>
        requires std::is_base_of_v<NBigRT::IWriter, T>
    T& GetWriter(TTypeTag<T> tag) const
    {
        auto it = WriterMap_.find(std::pair{std::type_index(typeid(T)), tag.GetDescription()});
        if (it == WriterMap_.end()) {
            Y_FAIL("key %s is not registered", ToString(tag).c_str());
        }
        return dynamic_cast<T&>(*it->second);
    }

    NBigRT::IWriterPtr Clone() override;

    NYT::TFuture<TTransactionWriter> ExtractAsyncWriter() override;

private:
    const TWriterMap WriterMap_;
};

////////////////////////////////////////////////////////////////////////////////

class TCompositeBigRtWriterFactory
    : public NBigRT::IWriterFactory
{
public:
    template <typename T>
        requires std::is_base_of_v<NBigRT::IWriter, T>
    void Add(TTypeTag<T> tag, NBigRT::IWriterFactoryPtr factory)
    {
        auto result = FactoryMap_.emplace(std::pair{std::type_index(typeid(T)), tag.GetDescription()}, factory);
        if (!result.second) {
            Y_FAIL("key %s is already registered", ToString(tag).c_str());
        }
    }

    NBigRT::IWriterPtr Make(uint64_t shard, NSFStats::TSolomonContext solomonContext) override;

public:
    THashMap<std::pair<std::type_index, TString>, NBigRT::IWriterFactoryPtr> FactoryMap_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NRoren::NPrivate
