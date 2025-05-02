#include "name_service.h"

namespace NSQLComplete {

    namespace {

        class TNameService: public INameService {
        public:
            explicit TNameService(ISchemaGateway::TPtr schema)
                : Schema_(std::move(schema))
            {
            }

            NThreading::TFuture<TNameResponse> Lookup(TNameRequest request) const override {
                if (!request.Constraints.Folder &&
                    !request.Constraints.Table) {
                    return NThreading::MakeFuture<TNameResponse>({});
                }

                return Schema_
                    ->List(ToListRequest(std::move(request)))
                    .Apply(ToNameResponse);
            }

        private:
            static TListRequest ToListRequest(TNameRequest request) {
                return {
                    .Cluster = "", // TODO(YQL-19747)
                    .Path = request.Prefix,
                    .Filter = ToListFilter(request.Constraints),
                    .Limit = request.Limit,
                };
            }

            static TListFilter ToListFilter(const TNameConstraints& constraints) {
                TListFilter filter;
                filter.Types = THashSet<TString>();
                if (constraints.Folder) {
                    filter.Types->emplace(TFolderEntry::Folder);
                }
                if (constraints.Table) {
                    filter.Types->emplace(TFolderEntry::Table);
                }
                return filter;
            }

            static TNameResponse ToNameResponse(TFuture<TListResponse> f) {
                TListResponse list = f.ExtractValue();

                TNameResponse response;
                for (auto& entry : list.Entries) {
                    response.RankedNames.emplace_back(ToGenericName(std::move(entry)));
                }
                response.NameHintLength = list.NameHintLength;
                return response;
            }

            static TGenericName ToGenericName(TFolderEntry entry) {
                TGenericName name;
                if (entry.Type == TFolderEntry::Folder) {
                    TFolderName local;
                    local.Indentifier = std::move(entry.Name);
                    name = std::move(local);
                } else if (entry.Type == TFolderEntry::Table) {
                    TTableName local;
                    local.Indentifier = std::move(entry.Name);
                    name = std::move(local);
                } else {
                    TUnkownName local;
                    local.Content = std::move(entry.Name);
                    local.Type = std::move(entry.Type);
                    name = std::move(local);
                }
                return name;
            }

            ISchemaGateway::TPtr Schema_;
        };

    } // namespace

    INameService::TPtr MakeSchemaNameService(ISchemaGateway::TPtr schema) {
        return new TNameService(std::move(schema));
    }

} // namespace NSQLComplete
