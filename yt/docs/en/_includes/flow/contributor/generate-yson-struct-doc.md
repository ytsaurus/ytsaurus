# YSON structure documentation generator for {{product-name}} Flow

Part of the {{product-name}} Flow user documentation is generated directly from the source code. This lets you keep the description of YSON structures in sync with the C++ definitions without manual maintenance.

The script collects the markdown description of the YSON structures that you use to configure {{product-name}} Flow. In addition to the consolidated [Configuration Reference](../../../flow/generated_docs/all_yson_structs.md), the generator creates a separate file for each root and nested structure (as well as for any encountered enums and types like `TDuration`). All these files are stored together in the `generated_docs/` directory.

Data sources:
- Metadata from the C++ code (parameter names and types, default values, and so on), extracted directly from the YSON structures.
- Markdown descriptions of the structures and their fields, written manually by developers and technical writers in `description_ru.yaml`.

## How to edit the generated documentation {#edit}

Edit `description_ru.yaml`, run the generator, and commit the changes.

## Running the generator {#run}

After you make changes to the YSON structure code or to `description_ru.yaml`:

```bash
cd $(arc root)/yt/yt/flow/tools/generate_yson_struct_doc
./generate.sh
```

The script automatically performs the necessary `arc add` operations for the generated documentation. After that, you can commit the changes.

## Adding new root YSON structures {#new-roots}

The generator takes several root YSON structures (`TPipelineSpec`, `TFlowNodeConfig`, parameters for computations, sources, sinks, and so on), recursively traverses them through nested fields, and extracts all the schemas it finds.

If a new structure isn’t accessible from the current root structures, you need to explicitly add it to `./bindings/module.cpp`. Otherwise, the generator won’t see it.