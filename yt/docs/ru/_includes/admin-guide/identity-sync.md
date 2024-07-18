# Синхронизация пользователей и групп с внешней системой

Иногда возникает необходимость поддерживать синхронизацию списка пользователей и групп с внешней системой. Одним из возможных решений этой задачи может быть использование приложения [ytsaurus-identity-sync](https://github.com/tractoai/ytsaurus-identity-sync). Приложение периодически запрашивает пользователей, группы и участия в группах из внешнего источника и обновляет их в {{product-name}}.

{% note warning %}

Приложение `ytsaurus-identity-sync` позволяет выполнять синхронизацию только в одну сторону — из внешнего источника в систему {{product-name}}. Обратная синхронизация — из {{product-name}} во внешнюю систему — не поддерживается.

{% endnote %}

Приложение поддерживает импорт пользователей и групп из двух источников:
 - Microsoft Entra (ранее известная как Azure Active Directory) с использованием [Microsoft Graph REST API](https://learn.microsoft.com/en-us/graph/azuread-users-concept-overview);
 - [Lightweight Directory Access Protocol (LDAP)](https://en.wikipedia.org/wiki/Lightweight_Directory_Access_Protocol).

Детали об установке и конфигурации приложения можно найти в [README приложения](https://github.com/tractoai/ytsaurus-identity-sync?tab=readme-ov-file#installing).

## См. также

- [Управление пользователями и группами](../../admin-guide/cluster-operations.md#upravlenie-polzovatelyami,-gruppami-i-pravami-dostupa) — в статье написано про ручное администрирование пользователей и групп в {{product-name}}.
