#include "name_service.h"

namespace NSQLComplete {

    namespace {

        class TNameService: public INameService {
        public:
            explicit TNameService(ISchema::TPtr schema)
                : Schema_(std::move(schema))
            {
            }

            NThreading::TFuture<TNameResponse> Lookup(TNameRequest request) override {
                TNameResponse response;

                if (request.Constraints.Table) {
                    TListRequest listRequest = {
                        .System = "",
                        .Path = request.Prefix,
                        .Filter = {
                            .Types = MakeMaybe<THashSet<TObjectType>>({
                                ObjectType.Folder,
                                ObjectType.Table,
                            }),
                        },
                        .Limit = request.Limit,
                    };

                    // TODO(YQL-19747): Waiting without a timeout and error checking
                    TListResponse listResponse = Schema_->List(listRequest).GetValueSync();

                    for (auto& entry : listResponse.Entries) {
                        if (entry.Type == ObjectType.Folder) {
                            TFolderName name;
                            name.Indentifier = std::move(entry.Name);
                            response.RankedNames.emplace_back(std::move(name));
                        } else if (entry.Type == ObjectType.Table) {
                            TTableName name;
                            name.Indentifier = std::move(entry.Name);
                            response.RankedNames.emplace_back(std::move(name));
                        }
                    }
                }

                return NThreading::MakeFuture(std::move(response));
            }

        private:
            ISchema::TPtr Schema_;
        };

    } // namespace

    INameService::TPtr MakeSchemaNameService(ISchema::TPtr schema) {
        return INameService::TPtr(new TNameService(std::move(schema)));
    }

} // namespace NSQLComplete
