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
h2 {
  padding-top: 32px !important;
  margin-top: 0 !important;
}
h3 {
  padding-top: 8px !important;
  margin-top: 0 !important;
}
</style>

**{{product-name}}** is a distributed storage and processing platform for large amounts of data. It includes [MapReduce](http://en.wikipedia.org/wiki/MapReduce) computation model, a distributed file system and a NoSQL key-value storage.

<div class="grid-container">
    <div class="grid-item">
        <h3><a lang="en" href="overview/about">Overview</a></h3>
        <p>System overview: {{product-name}} purpose and key features of the platform.</p>
    </div>
    <div class="grid-item">
        <h3><a lang="en" href="user-guide/storage/cypress">Data storage</a></h3>
        <p>Storing data in {{product-name}}: Cypress metadata storage, key system entities, static tables, transactions, data storage formats.</p>
    </div>
    <div class="grid-item">
        <h3><a lang="en" href="overview/try-yt">How to try?</a></h3>
        <p>Step-by-step tutorial on how to quickly deploy a {{product-name}} instance.</p>
    </div>
    <div class="grid-item">
        <h3><a lang="en" href="user-guide/dynamic-tables/overview">Dynamic tables</a></h3>
        <p>NoSQL key-value database: transactions, query language, replicated dynamic tables.</p>
    </div>
    <div class="grid-item">
        <h3><a lang="en" href="api/commands">API and reference</a></h3>
        <p>Commands and their parameters, SDK description, and sample code for platform interaction.</p>
    </div>
    <div class="grid-item">
        <h3><a lang="en" href="user-guide/data-processing/scheduler/scheduler-and-pools">Data processing</a></h3>
        <p>Processing data with {{product-name}}: scheduler, MapReduce paradigm, operations supported.</p>
    </div>
    <!-- <div class="grid-item">
        <h3><a href="admin-guide/set-req">Administration Guide</a></h3>
        <p>Configuration, installation process and instructions for basic administrative tasks.</p>
    </div> -->
        <div class="grid-item">
        <ul>
            <li><b><a lang="en" href="yql/index">YQL</a></b>: A declarative SQL-like query language.</li>
            <li><b><a lang="en" href="user-guide/data-processing/chyt/about-chyt">CHYT</a></b>: A ClickHouse cluster running in {{product-name}}.</li>
            <li><b><a lang="en" href="user-guide/data-processing/spyt/overview">SPYT</a></b>: An Apache Spark cluster running in {{product-name}}.</li>
        </ul>
    </div>


</div>

## Useful links { #links }

* [GitHub](https://github.com/ytsaurus/ytsaurus)
* [{{product-name}} site](https://ytsaurus.tech)
* [Telegram](https://t.me/ytsaurus)
* [Stack Overflow](https://stackoverflow.com/tags/ytsaurus)
* [Email for questions](mailto:community@ytsaurus.tech)

