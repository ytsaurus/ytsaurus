use plato;
pragma UseBlocks;
select ClickHouse::plus(key,1u), ClickHouse::plus(2u,subkey) from Input;