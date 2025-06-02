/* syntax version 1 */
use plato;
process Input using ClickHouse::run(TableRows(), "select (key,subkey) as x, value || '_foo' as y from Input");

