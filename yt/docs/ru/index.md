---
title: documentation
links:
  - title: documentation
    href: documentation/
---

# {{product-name}}

<style scoped>
.grid-container {
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(300px, 1fr));
  column-gap: 50px;
  row-gap: 20px;
}
.grid-item {
  display: flex;
  flex-direction: column;
}
.last {
  grid-column: -2;
}
h2 {
  padding-top: 32px !important;
  margin-top: 0 !important;
}
h3 {
  padding-top: 8px !important;
  margin-top: 0 !important;
}
</style>

**{{product-name}}** — платформа распределенного хранения и обработки больших объемов данных с поддержкой [MapReduce](http://ru.wikipedia.org/wiki/MapReduce), распределенной файловой системой и NoSQL key-value базой данных.

<div class="grid-container">
    <div class="grid-item">
        <h3><a lang="ru" href="overview/about">Обзор</a></h3>
        <p>Общее описание системы: назначение {{product-name}} и основные возможности платформы.</p>
    </div>
    <div class="grid-item">
        <h3><a lang="ru" href="user-guide/storage/cypress">Хранение информации</a></h3>
        <p>Хранение данных в {{product-name}}: дерево метаинформации Кипарис, основные объекты системы, ACL, статические таблицы, транзакции, форматы хранения.</p>
    </div>
    <div class="grid-item">
        <h3><a lang="ru" href="overview/try-yt">Как попробовать</a></h3>
        <p>Пошаговая инструкция по тому, как быстро развернуть свою копию {{product-name}}.</p>
    </div>
    <div class="grid-item">
        <h3><a lang="ru" href="user-guide/dynamic-tables/overview">Динамические таблицы</a></h3>
        <p>NoSQL key-value база данных: транзакции, язык запросов, реплицированные динамические таблицы.</p>
    </div>
    <div class="grid-item">
        <h3><a lang="ru" href="api/commands">API и справочник</a></h3>
        <p>Команды и их параметры, описание SDK и примеры кода для взаимодействия с платформой.</p>
    </div>
    <div class="grid-item">
        <h3><a lang="ru" href="user-guide/data-processing/scheduler/scheduler-and-pools">Обработка данных</a></h3>
        <p>Обработка данных при помощи {{product-name}}: планировщик, парадигма MapReduce, поддерживаемые операции.</p>
        <ul>
            <li><b><a lang="ru" href="yql/index">YQL</a></b> — декларативный SQL-подобный язык запросов.</li>
            <li><b><a lang="ru" href="user-guide/data-processing/chyt/about-chyt">CHYT</a></b> — кластер ClickHouse внутри {{product-name}}.</li>
            <li><b><a lang="ru" href="user-guide/data-processing/spyt/overview">SPYT</a></b> — кластер Apache Spark внутри {{product-name}}.</li>
        </ul>
    </div>
</div>

## Полезные ссылки { #links }

{% if audience == "internal" %}

{% include [Полезные ссылки](_includes/links-int.md) %}

{% else %}
* [GitHub](https://github.com/ytsaurus/ytsaurus)
* [Сайт {{product-name}}](https://ytsaurus.tech/ru)
* [Telegram](https://t.me/ytsaurus_ru)
* [Stack Overflow](https://stackoverflow.com/tags/ytsaurus)
* [Рассылка для вопросов](mailto:community_ru@ytsaurus.tech)
{% endif %}
