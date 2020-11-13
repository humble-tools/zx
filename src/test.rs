pub mod tests {
    use crate::*;
    use maplit::{convert_args, hashmap, hashset};
    use std::collections::HashMap;

    pub fn group_key_equality() {
        let mut m1 = HashMap::new();
        m1.insert("foo".to_string(), "1".to_string());

        let mut m2 = HashMap::new();
        m2.insert("foo".to_string(), "1".to_string());
        assert_eq!(m1, m2);

        let gk1 = GroupKey(m1);
        let gk2 = GroupKey(m2);
        assert_eq!(gk1, gk2);

        let mut m = HashMap::new();
        m.insert(gk1, "gk1");
        assert_eq!(*m.get(&gk2).unwrap(), "gk1");
    }

    pub fn filter_test(ctx: &Context) -> Result<(), anyhow::Error> {
        let ids = get_ids_in_range(ctx, Utc.timestamp(0, 0), Utc::now())?;
        assert_eq!(ids, hashset! {1,2,3,4,5,6,7,8,9,10});
        let ret = filter(ctx, ids.clone(), &Condition::new(Cmp::Eq, "foo", "200"))?;
        assert_eq!(ret, hashset! {2});

        let ret = filter(ctx, ids.clone(), &Condition::new(Cmp::NE, "foo", "200"))?;
        assert_eq!(ret, hashset! {1,3,4,5,6,7,8,9,10});

        let ret = filter(ctx, ids.clone(), &Condition::new(Cmp::GT, "foo", "200"))?;
        assert_eq!(ret, hashset! {3,4,5,6,7,8,9,10});

        let ret = filter(ctx, ids.clone(), &Condition::new(Cmp::LT, "foo", "200"))?;
        assert_eq!(ret, hashset! {1});

        let ret = filter(ctx, ids.clone(), &Condition::new(Cmp::LTE, "foo", "200"))?;
        assert_eq!(ret, hashset! {1, 2});

        let ret = filter(ctx, ids.clone(), &Condition::new(Cmp::EXISTS, "y", ""))?;
        assert_eq!(ret, hashset! {1, 4, 10});

        let ret = filter(ctx, ids.clone(), &Condition::new(Cmp::NOTEXISTS, "y", ""))?;
        assert_eq!(ret, hashset! {2, 3, 5, 6, 7, 8, 9});

        let ret = filter(ctx, ids.clone(), &Condition::new(Cmp::GTE, "foo", "100"))?;
        assert_eq!(ret, hashset! {1,2,3,4,5,6,7,8,9,10});

        let ret = get_columns_in_ids(ctx, ret, &vec!["x".to_string()])?;
        assert_eq!(
            ret[&2],
            convert_args!(hashmap!( "id" => "2", "x" => "1", TS => "1" ))
        );

        let ret = filter(
            ctx,
            ids.clone(),
            &Condition::new(Cmp::CONTAINS, "tag", "t1"),
        )?;
        assert_eq!(ret, hashset! {1,2});

        let ret = filter(
            ctx,
            ids.clone(),
            &Condition::new(Cmp::NOTCONTAINS, "tag", "t1"),
        )?;
        assert_eq!(ret, hashset! {4,6,3});

        let ret = filter(
            ctx,
            ids.clone(),
            &Condition::new(Cmp::STARTSWITH, "tag", "t2"),
        )?;
        assert_eq!(ret, hashset! {4});

        let ret = filter(
            ctx,
            ids.clone(),
            &Condition::new(Cmp::NOTSTARTSWITH, "tag", "t2"),
        )?;
        assert_eq!(ret, hashset! {3,1,2,6});
        Ok(())
    }

    pub fn group_test(ctx: &Context) -> Result<(), anyhow::Error> {
        let ids = get_ids_in_range(ctx, Utc.timestamp(0, 0), Utc::now())?;
        assert_eq!(ids, hashset! {1, 2, 3, 4, 5, 6, 7, 8, 9, 10});

        let selected = get_columns_in_ids(ctx, ids, &vec!["foo".to_string(), "y".to_string()])?;
        let grouped = group(selected, &vec!["y".to_string()]);
        assert_eq!(grouped.len(), 3);
        let group_y9 = grouped
            .get(&GroupKey(hashmap! {"y".to_string() => "9".to_string()}))
            .unwrap()
            .clone();
        assert_eq!(
            group_y9,
            vec![
                convert_args!(hashmap!("id" => "4", "y" => "9", TS => "3", "foo" => "400")),
                convert_args!(hashmap!("id" => "10", "y" => "9", "foo" => "1000", TS => "9")),
            ]
        );
        assert_eq!(
            grouped
                .get(&GroupKey(hashmap! {"y".to_string() => "1".to_string()}))
                .unwrap(),
            &vec![convert_args!(
                hashmap!("id" => "1", "foo" => "100", TS => "0", "y" => "1")
            )]
        );

        Ok(())
    }

    pub fn aggregate_sum(ctx: &Context) -> Result<(), anyhow::Error> {
        let ids = get_ids_in_range(ctx, Utc.timestamp(0, 0), Utc::now())?;
        assert_eq!(ids, hashset! {1, 2, 3, 4, 5, 6, 7, 8, 9, 10});

        let selected = get_columns_in_ids(ctx, ids, &vec!["foo".to_string(), "y".to_string()])?;
        let grouped = group(selected, &vec!["y".to_string()]);

        let sum = aggregate(
            &grouped,
            &vec![Select {
                agg: Aggregate::Sum,
                col: "foo".into(),
            }],
            &Duration::seconds(15),
        )?;
        assert_eq!(
            sum.get(&GroupKey(convert_args!(hashmap!("y" => "1"))))
                .unwrap()
                .get("$$sum(foo)")
                .unwrap(),
            &AggregatedTimeseries {
                window_starts: vec![0],
                window_ends: vec![0],
                data: vec![AggregateResult::from(100.0)]
            },
        );
        assert_eq!(
            sum.get(&GroupKey(convert_args!(hashmap!("y" => "9"))))
                .unwrap()
                .get("$$sum(foo)")
                .unwrap(),
            &AggregatedTimeseries {
                window_starts: vec![3],
                window_ends: vec![9],
                data: vec![AggregateResult::from(1400.0)]
            }
        );
        assert_eq!(
            sum.get(&GroupKey(convert_args!(hashmap!("y" => NIL))))
                .unwrap()
                .get("$$sum(foo)")
                .unwrap(),
            &AggregatedTimeseries {
                window_starts: vec![1],
                window_ends: vec![8],
                data: vec![AggregateResult::from(4000.0)]
            }
        );
        let sum2 = aggregate(
            &grouped,
            &vec![Select {
                agg: Aggregate::Sum,
                col: "y".into(),
            }],
            &Duration::seconds(15),
        )
        .unwrap();
        assert_eq!(
            sum2.get(&GroupKey(convert_args!(hashmap!("y" => "9"))))
                .unwrap()
                .get("$$sum(y)")
                .unwrap(),
            &AggregatedTimeseries {
                window_starts: vec![3],
                window_ends: vec![9],
                data: vec![AggregateResult::from(18.0)]
            }
        );

        Ok(())
    }

    pub fn aggregate_max(ctx: &Context) -> Result<(), anyhow::Error> {
        let ids = get_ids_in_range(ctx, Utc.timestamp(0, 0), Utc::now())?;
        assert_eq!(ids, hashset! {1, 2, 3, 4, 5, 6, 7, 8, 9, 10});

        let selected = get_columns_in_ids(ctx, ids, &vec!["foo".to_string(), "y".to_string()])?;
        let grouped = group(selected, &vec!["y".to_string()]);

        let max = aggregate(
            &grouped,
            &vec![Select {
                agg: Aggregate::Max,
                col: "foo".into(),
            }],
            &Duration::seconds(15),
        )?;

        assert_eq!(
            max.get(&GroupKey(convert_args!(hashmap!("y" => "1"))))
                .unwrap()
                .get("$$max(foo)")
                .unwrap(),
            &AggregatedTimeseries {
                window_starts: vec![0],
                window_ends: vec![0],
                data: vec![AggregateResult::from(100.0)]
            }
        );
        assert_eq!(
            max.get(&GroupKey(convert_args!(hashmap!("y" => "9"))))
                .unwrap()
                .get("$$max(foo)")
                .unwrap(),
            &AggregatedTimeseries {
                window_starts: vec![3],
                window_ends: vec![9],
                data: vec![AggregateResult::from(1000.0)]
            }
        );
        assert_eq!(
            max.get(&GroupKey(convert_args!(hashmap!("y" => NIL))))
                .unwrap()
                .get("$$max(foo)")
                .unwrap(),
            &AggregatedTimeseries {
                window_starts: vec![1],
                window_ends: vec![8],
                data: vec![AggregateResult::from(900.0)]
            }
        );

        Ok(())
    }

    pub fn aggregate_min(ctx: &Context) -> Result<(), anyhow::Error> {
        let ids = get_ids_in_range(ctx, Utc.timestamp(0, 0), Utc::now())?;
        assert_eq!(ids, hashset! {1, 2, 3, 4, 5, 6, 7, 8, 9, 10});

        let selected = get_columns_in_ids(ctx, ids, &vec!["foo".to_string(), "y".to_string()])?;
        let grouped = group(selected, &vec!["y".to_string()]);

        let min = aggregate(
            &grouped,
            &vec![Select {
                agg: Aggregate::Min,
                col: "foo".into(),
            }],
            &Duration::seconds(15),
        )?;

        assert_eq!(
            min.get(&GroupKey(convert_args!(hashmap!("y" => "1"))))
                .unwrap()
                .get("$$min(foo)")
                .unwrap(),
            &AggregatedTimeseries {
                window_starts: vec![0],
                window_ends: vec![0],
                data: vec![AggregateResult::from(100.0)]
            }
        );
        assert_eq!(
            min.get(&GroupKey(convert_args!(hashmap!("y" => "9"))))
                .unwrap()
                .get("$$min(foo)")
                .unwrap(),
            &AggregatedTimeseries {
                window_starts: vec![3],
                window_ends: vec![9],
                data: vec![AggregateResult::from(400.0)]
            }
        );
        assert_eq!(
            min.get(&GroupKey(convert_args!(hashmap!("y" => NIL))))
                .unwrap()
                .get("$$min(foo)")
                .unwrap(),
            &AggregatedTimeseries {
                window_starts: vec![1],
                window_ends: vec![8],
                data: vec![AggregateResult::from(200.0)]
            }
        );

        Ok(())
    }

    pub fn aggregate_count(ctx: &Context) -> Result<(), anyhow::Error> {
        let ids = get_ids_in_range(ctx, Utc.timestamp(0, 0), Utc::now())?;
        assert_eq!(ids, hashset! {1, 2, 3, 4, 5, 6, 7, 8, 9, 10});

        let selected = get_columns_in_ids(ctx, ids, &vec!["foo".to_string(), "y".to_string()])?;
        let grouped = group(selected, &vec!["y".to_string()]);

        let count = aggregate(
            &grouped,
            &vec![Select {
                agg: Aggregate::Count,
                col: "foo".into(),
            }],
            &Duration::seconds(15),
        )?;

        assert_eq!(
            count
                .get(&GroupKey(convert_args!(hashmap!("y" => "1"))))
                .unwrap()
                .get("$$count(foo)")
                .unwrap(),
            &AggregatedTimeseries {
                window_starts: vec![0],
                window_ends: vec![0],
                data: vec![AggregateResult::from(1.0)]
            }
        );
        assert_eq!(
            count
                .get(&GroupKey(convert_args!(hashmap!("y" => "9"))))
                .unwrap()
                .get("$$count(foo)")
                .unwrap(),
            &AggregatedTimeseries {
                window_starts: vec![3],
                window_ends: vec![9],
                data: vec![AggregateResult::from(2.0)]
            }
        );
        assert_eq!(
            count
                .get(&GroupKey(convert_args!(hashmap!("y" => NIL))))
                .unwrap()
                .get("$$count(foo)")
                .unwrap(),
            &AggregatedTimeseries {
                window_starts: vec![1],
                window_ends: vec![8],
                data: vec![AggregateResult::from(7.0)]
            }
        );

        Ok(())
    }

    pub fn aggregate_count_distinct(ctx: &Context) -> Result<(), anyhow::Error> {
        let ids = get_ids_in_range(ctx, Utc.timestamp(0, 0), Utc::now())?;
        assert_eq!(ids, hashset! {1, 2, 3, 4, 5, 6, 7, 8, 9, 10});

        let selected = get_columns_in_ids(ctx, ids, &vec!["x".to_string(), "y".to_string()])?;
        let grouped = group(selected, &vec!["y".to_string()]);

        let count = aggregate(
            &grouped,
            &vec![Select {
                agg: Aggregate::CountDistinct,
                col: "y".into(),
            }],
            &Duration::seconds(15),
        )?;

        assert_eq!(
            count
                .get(&GroupKey(convert_args!(hashmap!("y" => "1"))))
                .unwrap()
                .get("$$count_distinct(y)")
                .unwrap(),
            &AggregatedTimeseries {
                window_starts: vec![0],
                window_ends: vec![0],
                data: vec![AggregateResult::from(1.0)]
            }
        );
        assert_eq!(
            count
                .get(&GroupKey(convert_args!(hashmap!("y" => "9"))))
                .unwrap()
                .get("$$count_distinct(y)")
                .unwrap(),
            &AggregatedTimeseries {
                window_starts: vec![3],
                window_ends: vec![9],
                data: vec![AggregateResult::from(1.0)]
            }
        );
        assert_eq!(
            count
                .get(&GroupKey(convert_args!(hashmap!("y" => NIL))))
                .unwrap()
                .get("$$count_distinct(y)")
                .unwrap(),
            &AggregatedTimeseries {
                window_starts: vec![1],
                window_ends: vec![8],
                data: vec![AggregateResult::from(1.0)]
            }
        );

        Ok(())
    }

    pub fn aggregate_mean(ctx: &Context) -> Result<(), anyhow::Error> {
        let ids = get_ids_in_range(ctx, Utc.timestamp(0, 0), Utc::now())?;
        assert_eq!(ids, hashset! {1, 2, 3, 4, 5, 6, 7, 8, 9, 10});

        let selected = get_columns_in_ids(ctx, ids, &vec!["foo".to_string(), "y".to_string()])?;
        let grouped = group(selected, &vec!["y".to_string()]);

        let mean = aggregate(
            &grouped,
            &vec![Select {
                agg: Aggregate::Mean,
                col: "foo".into(),
            }],
            &Duration::seconds(15),
        )?;

        assert_eq!(
            mean.get(&GroupKey(convert_args!(hashmap!("y" => "1"))))
                .unwrap()
                .get("$$mean(foo)")
                .unwrap(),
            &AggregatedTimeseries {
                window_starts: vec![0],
                window_ends: vec![0],
                data: vec![AggregateResult::from(100.0)]
            }
        );
        assert_eq!(
            mean.get(&GroupKey(convert_args!(hashmap!("y" => "9"))))
                .unwrap()
                .get("$$mean(foo)")
                .unwrap(),
            &AggregatedTimeseries {
                window_starts: vec![3],
                window_ends: vec![9],
                data: vec![AggregateResult::from(700.0)]
            }
        );
        assert_eq!(
            mean.get(&GroupKey(convert_args!(hashmap!("y" => NIL))))
                .unwrap()
                .get("$$mean(foo)")
                .unwrap(),
            &AggregatedTimeseries {
                window_starts: vec![1],
                window_ends: vec![8],
                data: vec![AggregateResult::from(571.4285714285714)]
            }
        );

        Ok(())
    }

    pub fn sort_group_test(ctx: &Context) -> Result<(), anyhow::Error> {
        let ids = get_ids_in_range(ctx, Utc.timestamp(0, 0), Utc::now())?;
        assert_eq!(ids, hashset! {1, 2, 3, 4, 5, 6, 7, 8, 9, 10});

        let selected = get_columns_in_ids(ctx, ids, &vec!["foo".to_string(), "y".to_string()])?;
        let grouped = group(selected, &vec!["y".to_string()]);
        assert_eq!(grouped.len(), 3);

        let sorted_asc = sort_group(
            &grouped,
            &vec![Select {
                agg: Aggregate::Mean,
                col: "foo".into(),
            }],
            &vec![OrderBy {
                ord: Order::ASC,
                col: "$$mean(foo)".into(),
            }],
        )
        .unwrap();
        assert_eq!(
            sorted_asc,
            vec![
                (
                    GroupKey(convert_args!(hashmap!("y" => "1"))),
                    convert_args!(hashmap!("$$mean(foo)" => 100 as Number)),
                ),
                (
                    GroupKey(convert_args!(hashmap!("y" => "__nil"))),
                    convert_args!(hashmap!("$$mean(foo)" => 571.4285714285714 as Number)),
                ),
                (
                    GroupKey(convert_args!(hashmap!("y" => "9"))),
                    convert_args!(hashmap!("$$mean(foo)" => 700 as Number)),
                )
            ]
        );
        let sorted_desc = sort_group(
            &grouped,
            &vec![Select {
                agg: Aggregate::Mean,
                col: "foo".into(),
            }],
            &vec![OrderBy {
                ord: Order::DESC,
                col: "$$mean(foo)".into(),
            }],
        )
        .unwrap();
        assert_eq!(
            sorted_desc,
            vec![
                (
                    GroupKey(convert_args!(hashmap!("y" => "9"))),
                    convert_args!(hashmap!("$$mean(foo)" => 700 as Number)),
                ),
                (
                    GroupKey(convert_args!(hashmap!("y" => "__nil"))),
                    convert_args!(hashmap!("$$mean(foo)" => 571.4285714285714 as Number)),
                ),
                (
                    GroupKey(convert_args!(hashmap!("y" => "1"))),
                    convert_args!(hashmap!("$$mean(foo)" => 100 as Number)),
                ),
            ]
        );
        Ok(())
    }
}
