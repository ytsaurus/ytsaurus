# {{product-name}} Flow Style guide

Общие правила описаны в [{{product-name}} C++ Style Guide]({{source-root}}/yt/styleguide/cpp.md).

Можно использовать clang-format для автоформатирования, но за ним необходимо оценивать результат и находить компромисс между zero-diff линтера и здравым смыслом.

Как запустить:

`ya style --cpp-yt <your_file/directory>`

{% if audience == "internal" %}Кейсы странного форматирования можно завозить в очередь: https://nda.ya.ru/t/I1-g5T7K7gKZHP{% endif %}

{% if audience == "internal" %}

### Как настроить vscode

1. Создать прокси-бинарь для вызова `ya tool ads-clang-format`: 

    ```bash
    sudo echo -e "#! /bin/bash\nya tool ads-clang-format \$@\n" | sudo tee /usr/bin/ads-clang-format > /dev/null && sudo chmod +x /usr/bin/ads-clang-format
    ```

2. Установить [расширение](https://marketplace.visualstudio.com/items?itemName=xaver.clang-format).

3. Настроить vscode:

    4.1 `clang-format.executable` = `/usr/bin/ads-clang-format` (или Clang-format|Executable в UI).

    4.2 `editor.defaultFormatter` = `xaver.clang-format` (или Editor|Default Formatter в Clang-format в UI)

После этого можно пользоваться командой `Format Document` в контекстом меню редактора.

{% endif %}
