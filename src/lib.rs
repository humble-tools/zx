mod expr;
mod proc;
mod test;

use expr::*;
use test::tests;

use anyhow::{anyhow, Result};
use chrono::prelude::*;
use chrono::Duration;
use itertools::Itertools;
use maplit::{convert_args, hashmap};
use redis_module::{Context, RedisValue};
use serde::Serialize;
use serde::Serializer;
use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
use std::convert::TryFrom;
use std::hash::{Hash, Hasher};
use std::iter::FromIterator;

#[macro_use]
extern crate redis_module;

use maplit::hashset;
use redis_module::{RedisError, RedisResult};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

pub type ID = isize;
pub type Value = String;
pub type Column = String;
pub type Dataset = String;
pub type Number = f64;

#[derive(Debug, Serialize, PartialEq, PartialOrd)]
#[serde(untagged)]
pub enum AggregateResult {
    Number(Number),
    Value(Value),
    List(Vec<Number>),
}

impl From<f64> for AggregateResult {
    fn from(f: f64) -> Self {
        AggregateResult::Number(f)
    }
}

#[derive(Debug)]
pub struct Query {
    pub granularity: Duration,
    pub fields: QueryFields,
}

pub const NIL: &str = "__nil";
pub const TS: &str = "__ts";
pub const SAMPLE_RATE: &str = "__sample_rate";

// basically, (1 group_by cartesian product, all_conditions, 1 select) is a query to run
#[derive(Debug)]
pub struct QueryFields {
    pub t_start: DateTime<Utc>,
    pub t_end: DateTime<Utc>,
    pub selects: Vec<Select>,       // each select gives a separated result
    pub conditions: Vec<Condition>, // AND between conditions
    pub group_by: Vec<Column>,      // cartesian product between multiple group by
    pub order_by: Vec<OrderBy>,
}

pub type SparseData = HashMap<Column, Value>;
pub type SparseNumberData = HashMap<Column, Number>;

#[derive(Debug, Serialize, PartialEq, PartialOrd)]
pub struct AggregatedTimeseries {
    data: Vec<AggregateResult>,
    window_starts: Vec<isize>,
    window_ends: Vec<isize>,
}

#[derive(Eq, PartialEq)]
pub struct Segment {
    t_start: DateTime<Utc>,
    t_end: DateTime<Utc>,
    col: Column,
}

impl Hash for Segment {
    fn hash<H>(&self, state: &mut H)
    where
        H: Hasher,
    {
        self.t_start.hash(state);
        self.t_end.hash(state);
        self.col.hash(state);
    }
}

#[derive(Eq, PartialEq, Debug, Clone)]
pub struct GroupKey(SparseData);

impl Serialize for GroupKey {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(serde_json::to_string(&self.0).unwrap().as_str())
    }
}

impl Hash for GroupKey {
    fn hash<H>(&self, state: &mut H)
    where
        H: Hasher,
    {
        self.0.keys().len().hash(state);
        for k in self.0.keys().sorted() {
            k.hash(state);
            ";;;".hash(state);
            self.0[k].hash(state);
            "\n".hash(state);
        }
    }
}

#[derive(Debug)]
pub struct Select {
    pub agg: Aggregate,
    pub col: Column,
}

#[derive(Debug)]
pub enum Aggregate {
    Sum,
    Max,
    Min,
    Count,
    CountDistinct,
    Mean,
    P50,
    Raw,
    Heatmap,
}

#[derive(Debug, Clone)]
pub struct Condition {
    pub cmp: Cmp,
    pub col: Column,
    pub val: Value,
}

impl Condition {
    pub fn new(cmp: Cmp, col: &str, val: &str) -> Condition {
        Condition {
            cmp,
            col: col.to_string(),
            val: val.to_string(),
        }
    }
}

#[derive(Debug, Copy, Clone)]
pub enum Cmp {
    Eq,
    NE,
    GT,
    LT,
    GTE, // greater than or equal to
    LTE, // less than or equal to
    EXISTS,
    NOTEXISTS,
    STARTSWITH,
    NOTSTARTSWITH,
    CONTAINS,
    NOTCONTAINS,
}

#[derive(Debug)]
pub struct OrderBy {
    pub ord: Order,
    pub col: Column,
}

#[derive(Debug)]
pub enum Order {
    ASC,
    DESC,
}

// for each select -> get all id in time range -> filter all by conditions -> get selected and group_by columns -> group by "group_by" cartesian product -> aggregate selected column for each window -> return
pub fn run_unsorted(
    ctx: &Context,
    q: Query,
) -> Result<HashMap<GroupKey, HashMap<Column, AggregatedTimeseries>>> {
    let ids = get_ids_in_range(ctx, q.fields.t_start, q.fields.t_end)?;

    let mut filtered_ids = ids;
    for cond in &q.fields.conditions {
        filtered_ids = filter(ctx, filtered_ids, cond)?;
    }

    let mut required_columns: Vec<Column> = vec![];
    for select in &q.fields.selects {
        required_columns.push(select.col.clone());
    }
    for g in &q.fields.group_by {
        required_columns.push(g.clone());
    }
    required_columns.push(SAMPLE_RATE.to_string());

    let data = get_columns_in_ids(ctx, filtered_ids, &required_columns)?;

    let grouped = group(data, &q.fields.group_by);

    let aggregated = aggregate(&grouped, &q.fields.selects, &q.granularity)?;

    Ok(aggregated)
}

// run_sorted does not support aggregation, instead it directly sort the grouped result
// The return value is a list of sorted groups
pub fn run_sorted(ctx: &Context, q: Query) -> Result<Vec<(GroupKey, SparseNumberData)>> {
    let ids = get_ids_in_range(ctx, q.fields.t_start, q.fields.t_end)?;

    let mut filtered_ids = ids;
    for cond in &q.fields.conditions {
        filtered_ids = filter(ctx, filtered_ids, cond)?;
    }

    let mut required_columns: Vec<Column> = vec![];
    for select in &q.fields.selects {
        required_columns.push(select.col.clone());
    }
    for g in &q.fields.group_by {
        required_columns.push(g.clone());
    }
    required_columns.push(SAMPLE_RATE.to_string());

    let data = get_columns_in_ids(ctx, filtered_ids, &required_columns)?;

    let grouped = group(data, &q.fields.group_by);

    let sorted = sort_group(&grouped, &q.fields.selects, &q.fields.order_by)?;

    Ok(sorted)
}

pub fn flushall(ctx: &Context) -> Result<()> {
    ctx.call("FLUSHALL", &vec![])
        .map_err(|err| return anyhow!(err))?;
    Ok(())
}

pub fn init(ctx: &Context, id_offset: ID) -> Result<()> {
    ctx.call("SET", &vec!["LAST-ID", "0"])
        .map_err(|err| return anyhow!(err))?;
    ctx.call("SET", &vec!["ID-OFFSET", &id_offset.to_string()])
        .map_err(|err| return anyhow!(err))?;

    Ok(())
}

pub fn insert(ctx: &Context, ts: DateTime<Utc>, data: SparseData) -> Result<()> {
    let ret = ctx
        .call("INCR", &vec!["LAST-ID"])
        .map_err(|err| return anyhow!(err))?;
    if let RedisValue::Integer(id) = ret {
        // insert timestamp
        ctx.call(
            "ZADD",
            &vec![
                "TS",
                ts.timestamp().to_string().as_str(),
                id.to_string().as_str(),
            ],
        )
        .map_err(|err| return anyhow!(err))?;

        // insert key value pairs
        for (k, v) in data.iter() {
            // open column's redis server
            let mut reg = proc::REGISTRY.lock().unwrap();
            let socket_path = reg.start_col_server(k).unwrap();

            let path = format!("redis+unix://localhost{}", socket_path.to_string_lossy());
            let c = redis::Client::open(path)?;
            let mut conn = c.get_connection()?;
            // id is also inserted into the sorted set because it's a set and doesn't allow
            // duplicated values
            redis::cmd("ZADD")
                .arg(format!("COL-{}", k))
                .arg(id.to_string().as_str())
                .arg(format!("{};;;{}", id, v).as_str())
                .query(&mut conn)?;
            redis::cmd("SAVE") // force fsync, this is slow, use batch redis import instead
                .query(&mut conn)?;
        }
    } else {
        panic!("failed to convert id")
    }

    Ok(())
}

pub fn get_ids_in_range(
    ctx: &Context,
    t_start: DateTime<Utc>,
    t_end: DateTime<Utc>,
) -> Result<HashSet<ID>> {
    let start = isize::try_from(t_start.timestamp()).ok().unwrap();
    let end = isize::try_from(t_end.timestamp()).ok().unwrap();

    let ids = ctx
        .call(
            "zrangebyscore",
            &vec!["TS", start.to_string().as_str(), end.to_string().as_str()],
        )
        .map_err(|err| return anyhow!(err))?;

    let mut set: HashSet<ID> = hashset! {};

    if let RedisValue::Array(ss) = ids {
        for s in ss {
            if let RedisValue::SimpleString(x) = s {
                set.insert(x.parse::<isize>().unwrap());
            }
        }
    }

    Ok(set)
}

pub fn get_column_values(
    ctx: &Context,
    ids: &HashSet<ID>,
    col: &Column,
) -> Result<Vec<(String, ID)>> {
    let max_range = max_range(&ids);

    if col != TS && col != SAMPLE_RATE {
        // open column's redis server
        let mut reg = proc::REGISTRY.lock().unwrap();
        let socket_path = reg.start_col_server(col).unwrap();

        let path = format!("redis+unix://localhost{}", socket_path.to_string_lossy());
        let c = redis::Client::open(path)?;
        let mut conn = c.get_connection()?;
        let ret: Vec<String> = redis::cmd("zrangebyscore")
            .arg(format!("COL-{}", col))
            .arg(max_range.start().to_string().as_str())
            .arg(max_range.end().to_string().as_str())
            .arg("WITHSCORES")
            .query(&mut conn)?;

        dbg!(col);
        dbg!(ret.len());
        let mut result: Vec<(String, ID)> = vec![];
        let mut last_val: String = "".to_string();
        for (i, val) in ret.iter().enumerate() {
            if i % 2 == 1 {
                result.push((last_val.clone(), val.parse::<ID>().unwrap()));
            } else {
                last_val = val.clone();
            }
        }

        return Ok(result);
    }
    let ret = ctx
        .call(
            "ZRANGEBYSCORE",
            &vec![
                format!("COL-{}", col).as_str(),
                max_range.start().to_string().as_str(),
                max_range.end().to_string().as_str(),
                "WITHSCORES",
            ],
        )
        .map_err(|err| return anyhow!(err))?;

    let mut result: Vec<(String, ID)> = vec![];

    if let RedisValue::Array(list) = ret {
        let iter = list
            .iter()
            .map(|x| {
                if let RedisValue::SimpleString(s) = x {
                    return Some(s);
                } else {
                    return None;
                }
            })
            .enumerate();

        let mut last_val: String = "".to_string();
        for (i, val) in iter {
            if i % 2 == 1 {
                result.push((last_val.clone(), val.unwrap().parse::<ID>().unwrap()));
            } else {
                last_val = val.unwrap().clone();
            }
        }
    }

    dbg!(col);
    dbg!(result.len());

    Ok(result)
}

pub fn filter(ctx: &Context, ids: HashSet<ID>, cond: &Condition) -> Result<HashSet<ID>> {
    let column_values = get_column_values(ctx, &ids, &cond.col)?;
    let mut ret = HashSet::new();
    if let Cmp::NOTEXISTS = cond.cmp {
        let mut exist_ids = HashSet::new();

        for (_, id) in column_values.iter() {
            exist_ids.insert(*id);
        }

        return Ok(HashSet::from_iter(ids.difference(&exist_ids).map(|x| *x)));
    }

    for (x, id) in column_values.iter() {
        let id = *id;
        let x = x.split(";;;").collect::<Vec<&str>>()[1];
        if !ids.contains(&id) {
            continue;
        }

        match cond.cmp {
            Cmp::Eq => {
                if *x == cond.val {
                    ret.insert(id);
                }
            }
            Cmp::NE => {
                if *x != cond.val {
                    ret.insert(id);
                }
            }
            Cmp::GT => {
                let num = x.parse::<ID>().unwrap();
                let cond_num = cond.val.parse::<ID>().unwrap();
                if num > cond_num {
                    ret.insert(id);
                }
            }
            Cmp::LT => {
                let num = x.parse::<ID>().unwrap();
                let cond_num = cond.val.parse::<ID>().unwrap();
                if num < cond_num {
                    ret.insert(id);
                }
            }
            Cmp::GTE => {
                let num = x.parse::<ID>().unwrap();
                let cond_num = cond.val.parse::<ID>().unwrap();
                if num >= cond_num {
                    ret.insert(id);
                }
            }
            Cmp::LTE => {
                let num = x.parse::<ID>().unwrap();
                let cond_num = cond.val.parse::<ID>().unwrap();
                if num <= cond_num {
                    ret.insert(id);
                }
            }
            Cmp::EXISTS => {
                ret.insert(id);
            }
            Cmp::NOTEXISTS => {
                unimplemented!();
            }
            Cmp::STARTSWITH => {
                if x.starts_with(&cond.val) {
                    ret.insert(id);
                }
            }
            Cmp::NOTSTARTSWITH => {
                if !x.starts_with(&cond.val) {
                    ret.insert(id);
                }
            }
            Cmp::CONTAINS => {
                if x.contains(&cond.val) {
                    ret.insert(id);
                }
            }
            Cmp::NOTCONTAINS => {
                if !x.contains(&cond.val) {
                    ret.insert(id);
                }
            }
        }
    }

    Ok(ret)
}

pub fn get_columns_in_ids(
    ctx: &Context,
    ids: HashSet<ID>,
    cols: &Vec<Column>,
) -> Result<HashMap<ID, SparseData>> {
    let mut ret = HashMap::new();
    for col in cols {
        let column_values = get_column_values(ctx, &ids, col)?;
        for (x, id) in column_values.iter() {
            let val =
                std::str::from_utf8(x.as_bytes().split_at(id.to_string().chars().count() + 3).1)?;
            if !ids.contains(id) {
                continue;
            }

            ret.entry(*id)
                .or_insert(HashMap::new())
                .insert(col.clone(), val.to_string().clone());
            ret.entry(*id)
                .or_insert(HashMap::new())
                .insert("id".to_string(), id.to_string().clone());
        }
    }
    // get timestamp
    for (id, data) in ret.iter_mut() {
        // let ts: String = conn.zscore("TS", *id)?;
        if let RedisValue::SimpleString(ts) = ctx
            .call("ZSCORE", &vec!["TS", id.to_string().as_str()])
            .map_err(|err| return anyhow!(err))?
        {
            data.insert(TS.into(), ts.clone());
        }
    }
    Ok(ret)
}

pub fn group(
    data: HashMap<ID, SparseData>,
    group_bys: &Vec<Column>,
) -> HashMap<GroupKey, Vec<SparseData>> {
    let mut ret = HashMap::new();

    for id in data.keys().sorted() {
        let mut gk = GroupKey(HashMap::new());
        for k in group_bys {
            let v = match data[id].get(k) {
                Some(x) => x,
                None => NIL,
            };
            gk.0.insert(k.clone(), v.to_string());
        }
        ret.entry(gk).or_insert(vec![]).push(data[id].clone());
    }
    ret
}

fn aggregate_window(window: &[SparseData], select: &Select) -> Result<AggregateResult> {
    match select.agg {
        Aggregate::Sum => {
            let mut sum = 0.0;
            for row in window {
                let sample_rate = row
                    .get(SAMPLE_RATE)
                    .unwrap_or(&"1".to_string())
                    .parse::<f64>()?;

                sum += sample_rate
                    * (row
                        .get(&select.col)
                        .unwrap_or(&"0".to_string())
                        .parse::<f64>()?);
            }

            Ok(AggregateResult::Number(sum))
        }
        Aggregate::Max => {
            let mut max = f64::MIN;
            for row in window {
                match row.get(&select.col) {
                    Some(val) => {
                        let v = val.parse::<f64>()?;
                        if v > max {
                            max = v
                        }
                    }
                    None => {}
                }
            }
            Ok(AggregateResult::Number(max))
        }
        Aggregate::Min => {
            let mut min = f64::MAX;
            for row in window {
                match row.get(&select.col) {
                    Some(val) => {
                        let v = val.parse::<f64>()?;
                        if v < min {
                            min = v;
                        }
                    }
                    None => {}
                }
            }

            Ok(AggregateResult::Number(min))
        }
        Aggregate::Count => {
            let mut count = 0.0;
            for row in window {
                match row.get(&select.col) {
                    Some(_) => {
                        let sample_rate = row
                            .get(SAMPLE_RATE)
                            .unwrap_or(&"1".to_string())
                            .parse::<f64>()?;
                        count += sample_rate;
                    }
                    None => {}
                }
            }
            Ok(AggregateResult::Number(count))
        }
        Aggregate::CountDistinct => {
            let mut counter = HashSet::new();
            let mut include_nil = 0.0;
            for row in window {
                match row.get(&select.col) {
                    Some(val) => {
                        counter.insert(val);
                    }
                    None => include_nil = 1.0,
                }
            }
            Ok(AggregateResult::Number(counter.len() as f64 + include_nil))
        }
        Aggregate::Mean => {
            let mut avg = 0.0;
            let mut t = 1.0;
            for row in window {
                match row.get(&select.col) {
                    Some(val) => {
                        let sample_rate = row
                            .get(SAMPLE_RATE)
                            .unwrap_or(&"1".to_string())
                            .parse::<isize>()?;
                        let v = val.parse::<f64>()?;
                        for _ in 0..sample_rate {
                            avg += (v - avg) / t;
                            t += 1.0;
                        }
                    }
                    None => {}
                }
            }
            Ok(AggregateResult::Number(avg))
        }
        // TODO: implement these
        Aggregate::P50 => unimplemented!(),
        Aggregate::Raw => unimplemented!(),
        Aggregate::Heatmap => unimplemented!(),
    }
}

pub fn aggregate(
    grouped_data: &HashMap<GroupKey, Vec<SparseData>>,
    selects: &Vec<Select>,
    window_size: &Duration,
) -> Result<HashMap<GroupKey, HashMap<Column, AggregatedTimeseries>>> {
    let mut ret = HashMap::new();

    let window_size_seconds = isize::try_from(window_size.num_seconds()).ok().unwrap();

    for select in selects {
        let agg_key = aggregate_key(select);

        for (gk, group) in grouped_data {
            let mut window_start_idx = 0;
            let mut window_start_timestamp = -1;
            let mut window_end_idx = 0;
            let mut window_end_timestamp = -1;

            let mut ats = AggregatedTimeseries {
                data: vec![],
                window_starts: vec![],
                window_ends: vec![],
            };

            for (i, data) in group.iter().enumerate() {
                let ts = data.get(TS).unwrap().parse::<isize>()?;
                if window_start_timestamp == -1 {
                    window_start_timestamp = ts;
                    window_start_idx = i;
                }

                if (ts - window_start_timestamp) <= window_size_seconds {
                    window_end_idx = i + 1;
                    window_end_timestamp = ts;
                    continue;
                }

                let window = &group[window_start_idx..window_end_idx];

                // collect window aggregation result
                let v = aggregate_window(&window, select)?;
                ats.data.push(v);
                ats.window_starts.push(window_start_timestamp);
                ats.window_ends.push(window_end_timestamp);

                // bump to next window
                window_start_idx = i + 1;
                window_start_timestamp = ts;
                window_end_idx = window_start_idx;
            }

            // leftovers
            if window_start_idx < window_end_idx {
                let window = &group[window_start_idx..window_end_idx];

                // collect window aggregation result
                let v = aggregate_window(&window, select)?;
                ats.data.push(v);
                ats.window_starts.push(window_start_timestamp);
                ats.window_ends.push(window_end_timestamp);
            }
            ret.entry(gk.clone())
                .or_insert(HashMap::new())
                .insert(agg_key.clone(), ats);
        }
    }
    Ok(ret)
}

// aggregate grouped data into an aggregation value, then sort
pub fn sort_group(
    grouped_data: &HashMap<GroupKey, Vec<SparseData>>,
    selects: &Vec<Select>,
    order_bys: &Vec<OrderBy>,
) -> Result<Vec<(GroupKey, SparseNumberData)>> {
    let mut flattened_groups: Vec<(GroupKey, SparseNumberData)> = vec![];
    for select in selects {
        let agg_key = aggregate_key(select);
        for (gk, group) in grouped_data {
            let v = aggregate_window(&group, select)?;
            // only sort number result, ignore everything else
            if let AggregateResult::Number(num) = v {
                flattened_groups.push((gk.clone(), hashmap! {agg_key.clone() => num}));
            }
        }
    }

    Ok(flattened_groups
        .into_iter()
        .sorted_by(|a, b| {
            let mut ord_ret: Option<Ordering> = None;
            for order in order_bys {
                let a_col = a.1.get(&order.col).unwrap();
                let b_col = b.1.get(&order.col).unwrap();
                if ord_ret.is_none() {
                    match order.ord {
                        Order::ASC => {
                            ord_ret = a_col.partial_cmp(b_col);
                        }
                        Order::DESC => {
                            ord_ret = b_col.partial_cmp(a_col);
                        }
                    }
                } else {
                    match order.ord {
                        Order::ASC => {
                            ord_ret =
                                Some(ord_ret.unwrap().then(a_col.partial_cmp(b_col).unwrap()));
                        }
                        Order::DESC => {
                            ord_ret =
                                Some(ord_ret.unwrap().then(b_col.partial_cmp(a_col).unwrap()));
                        }
                    }
                }
            }
            ord_ret.unwrap()
        })
        .collect())
}

fn max_range(ids: &HashSet<ID>) -> std::ops::RangeInclusive<ID> {
    let min = ids
        .iter()
        .min_by(|a, b| a.partial_cmp(b).expect("Found a NaN"))
        .cloned()
        .expect("There is no minimum");
    let max = ids
        .iter()
        .max_by(|a, b| a.partial_cmp(b).expect("Found a NaN"))
        .cloned()
        .expect("There is no maximum");

    std::ops::RangeInclusive::new(min, max)
}

fn aggregate_key(select: &Select) -> String {
    match select.agg {
        Aggregate::Sum => format!("$$sum({})", select.col),
        Aggregate::Max => format!("$$max({})", select.col),
        Aggregate::Min => format!("$$min({})", select.col),
        Aggregate::Count => format!("$$count({})", select.col),
        Aggregate::CountDistinct => format!("$$count_distinct({})", select.col),
        Aggregate::Mean => format!("$$mean({})", select.col),
        Aggregate::P50 => format!("$$p50({})", select.col),
        Aggregate::Raw => format!("$$raw({})", select.col),
        Aggregate::Heatmap => format!("$$heatmap({})", select.col),
    }
}

fn hello_sql(ctx: &Context, args: Vec<String>) -> RedisResult {
    let args = args
        .into_iter()
        .skip(1)
        .map(|x| x.to_lowercase())
        .collect::<Vec<String>>();
    if args.len() < 2 || args.first().unwrap() != "select" {
        return Err(redis_module::RedisError::Str("failed to parse query"));
    }

    match execute(ctx, args) {
        Ok(json) => Ok(json.into()),
        Err(e) => Err(redis_module::RedisError::String(e.to_string())),
    }
}

fn execute(ctx: &Context, args: Vec<String>) -> anyhow::Result<String> {
    let sql = args.join(" ");
    let dialect = GenericDialect {};

    let ast = Parser::parse_sql(&dialect, &sql)?;
    let stmt = ast.get(0).ok_or(anyhow!("unable to get ast item 0"))?;
    let q = expr_to_query(&stmt)?;
    dbg!(&q);

    let ret = run_unsorted(&ctx, q)?;
    let json = serde_json::to_string(&ret)?;

    Ok(json.into())
}

fn i(ctx: &Context, _args: Vec<String>) -> RedisResult {
    insert(
        ctx,
        Utc::now(),
        convert_args!(hashmap!(
            "yy" => "z"
        )),
    )
    .unwrap();
    Ok("ok".into())
}

fn t(ctx: &Context, _args: Vec<String>) -> RedisResult {
    tests::group_key_equality();
    tests::filter_test(ctx).unwrap();
    tests::group_test(ctx).unwrap();
    tests::aggregate_sum(ctx).unwrap();
    tests::aggregate_max(ctx).unwrap();
    tests::aggregate_min(ctx).unwrap();
    tests::aggregate_count(ctx).unwrap();
    tests::aggregate_count_distinct(ctx).unwrap();
    tests::aggregate_mean(ctx).unwrap();
    tests::sort_group_test(ctx).unwrap();
    Ok("ok".into())
}

//////////////////////////////////////////////////////

redis_module! {
    name: "zx",
    version: 1,
    data_types: [],
    commands: [
        ["zx.sql", hello_sql, "", 0, 0, 0],
        ["zx.t", t, "", 0, 0, 0],
        ["zx.i", i, "", 0, 0, 0],
    ],
}
