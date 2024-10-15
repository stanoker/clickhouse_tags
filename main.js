import { createClient as CreateClickHouseClient } from '@clickhouse/client';

const clickhouse_client = CreateClickHouseClient({
  url: process.env.CLICKHOUSE_HOST ?? 'http://localhost:8123',
  username: process.env.CLICKHOUSE_USER ?? 'default',
  password: process.env.CLICKHOUSE_PASSWORD ?? '',
});


clickhouse_client.command({
  query: `
    CREATE TABLE IF NOT EXISTS tags 
    (
      record_id   UUID,
      tag         LowCardinality(String),
      sign        Int8
    )
    ENGINE = CollapsingMergeTree(sign)
    ORDER BY (tag, record_id)
  `,
}).then((result)=>{
  console.log("Table tags created", result.summary)
}).catch(err=>{
  console.log(err);
});


clickhouse_client.command({
  query: `
    CREATE TABLE IF NOT EXISTS tags_array
    (
      record_id  UUID,
      tags       Array(LowCardinality(String))
    )
    ENGINE = ReplacingMergeTree
    ORDER BY record_id
  `,
}).then((result)=>{
  console.log("Table tags_array created", result.summary)
}).catch(err=>{
  console.log(err);
});


clickhouse_client.command({
  query: `
    CREATE MATERIALIZED VIEW IF NOT EXISTS tags_array_mv TO tags_array AS 
    SELECT 
      record_id,
      if(sum(sign)>0, groupUniqArray(tag), []) AS tags
    FROM tags
    GROUP BY record_id
  `,
}).then((result)=>{
  console.log("MATERIALIZED VIEW tags_array_mv created IN", result.summary.elapsed_ns,'ns')
}).catch(err=>{
  console.log(err);
});


function AddTag(record_id, tag)
{
  return clickhouse_client.insert({
    table: 'tags',
    values: [{ 
      record_id: record_id,
      tag: tag,
      sign: 1 
    },],
    format: 'JSONEachRow',
  }).then(result=>{
    console.log(result)
  });
}


function DelTag(record_id, tag)
{
  return clickhouse_client.insert({
    table: 'tags',
    values: [
      { record_id: record_id, tag: tag, sign: -1},
    ],
    format: 'JSONEachRow',
  }).then(result=>{
    console.log(result)
  });
}