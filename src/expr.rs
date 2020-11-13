use crate::{Aggregate, Cmp, Column, Condition, Order, OrderBy, Query, QueryFields, Select};
use anyhow::Result;
use chrono::prelude::*;
use chrono::Duration;
use sqlparser::ast;
use sqlparser::ast::Statement;

fn expr_to_conditions(expr: &ast::Expr) -> Vec<Condition> {
    let mut col: Option<String> = None;
    let mut val: Option<String> = None;
    if let ast::Expr::BinaryOp { left, op, right } = expr {
        if let ast::Expr::Identifier(lhs) = *left.clone() {
            col = Some(lhs.value);
            if let ast::Expr::Identifier(rhs) = *right.clone() {
                val = Some(rhs.value);
            } else if let ast::Expr::Value(ast::Value::Number(rhs)) = *right.clone() {
                val = Some(rhs)
            }
        }
        match op {
            ast::BinaryOperator::Gt => {
                let cond = Condition {
                    col: col.unwrap(),
                    val: val.unwrap(),
                    cmp: Cmp::GT,
                };

                return vec![cond];
            }
            ast::BinaryOperator::Lt => {
                let cond = Condition {
                    col: col.unwrap(),
                    val: val.unwrap(),
                    cmp: Cmp::LT,
                };

                return vec![cond];
            }
            ast::BinaryOperator::GtEq => {
                let cond = Condition {
                    col: col.unwrap(),
                    val: val.unwrap(),
                    cmp: Cmp::GTE,
                };

                return vec![cond];
            }
            ast::BinaryOperator::LtEq => {
                let cond = Condition {
                    col: col.unwrap(),
                    val: val.unwrap(),
                    cmp: Cmp::LTE,
                };

                return vec![cond];
            }
            ast::BinaryOperator::Eq => {
                let cond = Condition {
                    col: col.unwrap(),
                    val: val.unwrap(),
                    cmp: Cmp::Eq,
                };

                return vec![cond];
            }
            ast::BinaryOperator::NotEq => {
                let cond = Condition {
                    col: col.unwrap(),
                    val: val.unwrap(),
                    cmp: Cmp::Eq,
                };

                return vec![cond];
            }
            ast::BinaryOperator::And => {
                let mut v: Vec<Condition> = vec![];
                v.append(&mut expr_to_conditions(&left));
                v.append(&mut expr_to_conditions(&right));
                return v;
            }
            ast::BinaryOperator::Like => {
                let cond = Condition {
                    col: col.unwrap(),
                    val: val.unwrap(),
                    cmp: Cmp::CONTAINS,
                };

                return vec![cond];
            }
            ast::BinaryOperator::NotLike => {
                let cond = Condition {
                    col: col.unwrap(),
                    val: val.unwrap(),
                    cmp: Cmp::NOTCONTAINS,
                };

                return vec![cond];
            }
            _ => unimplemented!(),
        }
    }

    vec![]
}

pub fn expr_to_query(stmt: &Statement) -> Result<Query> {
    let mut t_start = Utc.timestamp(0, 0);
    let mut t_end = Utc::now();
    let mut selects: Vec<Select> = vec![];
    let mut conditions: Vec<Condition> = vec![];
    let mut group_by: Vec<Column> = vec![];
    let mut order_by: Vec<OrderBy> = vec![];

    if let ast::Statement::Query(query) = stmt {
        if let ast::SetExpr::Select(select) = &query.body {
            // parse selects
            for proj in &select.projection {
                if let ast::SelectItem::UnnamedExpr(expr) = proj {
                    match expr {
                        ast::Expr::Identifier(ident) => {
                            let s = Select {
                                agg: Aggregate::Raw,
                                col: ident.value.clone(),
                            };
                            selects.push(s);
                        }
                        ast::Expr::Function(fx) => {
                            let fx_name = fx.name.0.get(0).unwrap();
                            let agg = match fx_name.value.as_str() {
                                "count" => Aggregate::Count,
                                "sum" => Aggregate::Sum,
                                "max" => Aggregate::Max,
                                "min" => Aggregate::Min,
                                "count_distinct" => Aggregate::CountDistinct,
                                "mean" => Aggregate::Mean,
                                "p50" => Aggregate::P50,
                                "heatmap" => Aggregate::Heatmap,
                                _ => unimplemented!(),
                            };
                            match fx.args.get(0).unwrap() {
                                ast::Expr::Identifier(fx_col) => {
                                    let s = Select {
                                        agg,
                                        col: fx_col.value.clone(),
                                    };
                                    selects.push(s);
                                }
                                ast::Expr::CompoundIdentifier(idents) => {
                                    let s = Select {
                                        agg,
                                        col: idents
                                            .iter()
                                            .map(|i| i.value.clone())
                                            .collect::<Vec<String>>()
                                            .join("."),
                                    };
                                    selects.push(s);
                                }
                                _ => unimplemented!(),
                            }
                        }
                        _ => unimplemented!(),
                    }
                }
            }

            if let Some(selection) = &select.selection {
                let conds = expr_to_conditions(selection);

                // extract tstart & tend
                conditions = conds
                    .iter()
                    .filter_map(|cond| {
                        if cond.col == "$T_START" {
                            t_start = Utc.timestamp(cond.val.parse::<i64>().unwrap(), 0);
                            return None;
                        }
                        if cond.col == "$T_END" {
                            t_end = Utc.timestamp(cond.val.parse::<i64>().unwrap(), 0);
                            return None;
                        }

                        return Some(cond.clone());
                    })
                    .collect();
            }

            // parse group by
            for expr in &select.group_by {
                if let ast::Expr::Identifier(ident) = expr {
                    group_by.push(ident.value.clone());
                }
            }
        }

        // parse order by
        for ord_by_expr in &query.order_by {
            println!("{:?}", ord_by_expr);
            let mut ord = OrderBy {
                ord: Order::ASC,
                col: "".to_string(),
            };
            if let ast::Expr::Identifier(ident) = &ord_by_expr.expr {
                ord.col = ident.value.clone();
            }
            if let Some(x) = ord_by_expr.asc {
                if !x {
                    ord.ord = Order::DESC;
                }
            }

            order_by.push(ord)
        }
    }

    let qf = QueryFields {
        selects,
        conditions,
        group_by,
        order_by,
        t_start,
        t_end,
    };
    let q = Query {
        granularity: Duration::minutes(5),
        fields: qf,
    };

    Ok(q)
}
