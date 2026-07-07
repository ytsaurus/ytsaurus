# Аутентификация и пароли

В {{product-name}} пользователь может аутентифицироваться в системе с помощью пароля. Его может изменить администратор или сам пользователь — через страницу изменения пароля или команду `set-user-password` в {{product-name}} CLI.

## Установка и смена пароля {#setting-and-changing-password}

Администратор устанавливает первый пароль для свежесозданного пользователя.

**Администратору** не требуется вводить текущий пароль ни при первой установке пароля, ни при последующих его изменениях. **Пользователю** требуется ввести текущий пароль для его изменения.

### Как работает команда set-user-password {#how-set-user-password-works}

#|
||
Администратор создаёт пользователя `oleg` и устанавливает ему пароль `cone`.
|
```bash
$ yt create user --attr '{name=oleg}'
$ yt set-user-password oleg
New password: <interactive typing>
Retype new password: <interactive typing>
```
||
||
Пользователь может сменить пароль на `cube` командой `set-user-password`.
|
```bash
$ yt set-user-password oleg
Current password for oleg: <interactive typing>
New password: <interactive typing>
Retype new password: <interactive typing>
```
||
|#

{% note info %}

Все пароли запрашиваются интерактивно без отображения в терминале для обеспечения безопасности. Его надо ввести повторно для подтверждения.

{% endnote %}

## Управление токенами {#token-management}

Для работы с {{product-name}} через CLI или через API пользователю нужны токены. Для управления токенами в CLI используйте команды `issue-token`, `revoke-token` и `list-user-tokens`.

{% note warning "Внимание" %}

Управление токенами требует пароля пользователя, но изменение пароля пользователя не приводит к отзыву его токенов. При смене пароля можно изменять токены постепенно. 

Если пароль пользователя скомпрометирован, стоит не только изменить пароль, но и отозвать все токены.

{% endnote %}

### issue-token

Команда `issue-token` выпускает новый токен для пользователя. В отличие от пароля, у пользователя может быть несколько активных токенов для плавного процесса замены одного на другой.

Пользователю `oleg` выпустили токен `ytct-2c59-56daecdff8dd45d2561a8679acf5`. Как и в случае команды `set-user-password`, пользователю нужно ввести пароль (запрашивается интерактивно), администратор же пароль пользователя вводить не должен.

```bash
$ yt issue-token oleg
Current password for oleg: <interactive typing>
ytct-2c59-56daecdff8dd45d2561a8679acf5
```

### list-user-tokens


Командой `list-user-tokens` можно посмотреть информацию об активных токенах пользователя — она вернет sha256-хеши токенов, {{product-name}} не сохраняет токены пользователей.

```bash
$ yt list-user-tokens oleg
Current password for oleg: <interactive typing>
["87a5d9406ccf6a42cca510d86e43b20e2943aa7ade7e9129f4f4f947e1b02574"]

$ echo -n 'ytct-2c59-56daecdff8dd45d2561a8679acf5' | sha256sum
87a5d9406ccf6a42cca510d86e43b20e2943aa7ade7e9129f4f4f947e1b02574  -
```

### revoke-token

Команда `revoke-token` отзывает токен пользователя. 

Чтобы отозвать токен, укажите его sha256-хеш флагом `--token-sha256`. 
Если флаг не указан, значение токена будет запрошено интерактивно.
 
Пароль всегда запрашивается интерактивно. `--token-sha256` позволяет использовать результат команды `list-user-tokens` для отзыва конкретных токенов.

```bash
$ yt revoke-token oleg --token-sha256 87a5d9406ccf6a42cca510d86e43b20e2943aa7ade7e9129f4f4f947e1b02574
Current password for oleg: <interactive typing>
$ yt revoke-token oleg
Current password for oleg: <interactive typing>
Token to revoke: <interactive typing>
$ yt list-user-tokens oleg
Current password for oleg: <interactive typing>
[]
```

<!--
Команда `issue-token` выпускает новый токен для пользователя. В отличие от пароля, у пользователя может быть несколько активных токенов для плавного процесса замены одного на другой.

```bash
$ yt issue-token oleg
Current password for oleg: <interactive typing>
ytct-2c59-56daecdff8dd45d2561a8679acf5
```

Пользователю `oleg` был выпущен токен `ytct-2c59-56daecdff8dd45d2561a8679acf5`. Как и в случае команды `set-user-password` пользователю необходимо ввести пароль (запрашивается интерактивно), администратор же пароль пользователя вводить не должен.

При помощи команды `list-user-tokens` можно посмотреть информацию об активных токенах пользователя. {{product-name}} не сохраняет токены пользователей, и команда `list-user-tokens` возвращает не токены, а их sha256-хеши. Например:

```bash
$ yt list-user-tokens oleg
Current password for oleg: <interactive typing>
["87a5d9406ccf6a42cca510d86e43b20e2943aa7ade7e9129f4f4f947e1b02574"]

$ echo -n 'ytct-2c59-56daecdff8dd45d2561a8679acf5' | sha256sum
87a5d9406ccf6a42cca510d86e43b20e2943aa7ade7e9129f4f4f947e1b02574  -
```

Команда `revoke-token` позволяет отозвать токен пользователя. Для отзыва токена можно указать его sha256-хеш при помощи флага `--token-sha256`, или, если флаг не указан, значение токена будет запрошено интерактивно. Пароль всегда запрашивается интерактивно. Использование `--token-sha256` позволяет использовать результат команда `list-user-tokens` для отзыва конкретных токенов.

```bash
$ yt revoke-token oleg --token-sha256 87a5d9406ccf6a42cca510d86e43b20e2943aa7ade7e9129f4f4f947e1b02574
Current password for oleg: <interactive typing>
$ yt revoke-token oleg
Current password for oleg: <interactive typing>
Token to revoke: <interactive typing>
$ yt list-user-tokens oleg
Current password for oleg: <interactive typing>
[]
```
-->
