# {{product-name}} Flow Style guide

You can find the general rules in the [{{product-name}} C++ Style Guide]({{source-root}}/yt/styleguide/cpp.md).

You can use clang-format for auto-formatting, but you need to review the result and find a balance between the zero-diff linter and common sense.

To run it:

`ya style --cpp-yt <your_file/directory>`

{% if audience == "internal" %}You can submit cases of strange formatting to the queue: https://nda.ya.ru/t/I1-g5T7K7gKZHP{% endif %}

{% if audience == "internal" %}

### How to configure vscode

1. Create a proxy binary to call `ya tool ads-clang-format`:

    ```bash
    sudo echo -e "#! /bin/bash\nya tool ads-clang-format \$@\n" | sudo tee /usr/bin/ads-clang-format > /dev/null && sudo chmod +x /usr/bin/ads-clang-format
    ```

2. Install the [extension](https://marketplace.visualstudio.com/items?itemName=xaver.clang-format).

3. Configure vscode:

    3.1 Set `clang-format.executable` to `/usr/bin/ads-clang-format` (or Clang-format|Executable in the UI).

    3.2 Set `editor.defaultFormatter` to `xaver.clang-format` (or Editor|Default Formatter in Clang-format in the UI).

After that, you can use the `Format Document` command in the editor's context menu.

{% endif %}