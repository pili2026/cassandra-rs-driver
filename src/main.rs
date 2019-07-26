#![allow(unused)]
extern crate actix;
extern crate actix_web;

#[macro_use]
extern crate serde_derive;
extern crate serde;
extern crate serde_json;

#[macro_use(stmt)]
extern crate cassandra_cpp;
extern crate futures;

#[macro_use]
extern crate lazy_static;

#[macro_use]
extern crate log;
extern crate fern;

extern crate eui48;
extern crate config;

mod function_util;
mod schema_util;
mod status_util;
mod cql_command;
use crate::cql_command::*;
use crate::schema_util::*;
use crate::function_util::*;
use crate::status_util::*;

use cassandra_cpp::{Cluster, Session, ValueType, Statement, Value, Consistency::QUORUM, CassResult, Row};
use cassandra_cpp::RetryPolicy;
use futures::{Future, Stream};
use std::{fmt, env};
use chrono::prelude::*;
use math::round;
use time::Duration;
use serde::*;
use serde::{Deserialize, Serialize};
use serde_json::{Value as _Value, Map, Number, to_value, json};
use actix_web::{error, http, middleware::*, App, Error, HttpMessage, HttpRequest, HttpResponse, Responder, Result as _Result, HttpServer, web, Route, guard};
use actix_web::web::{Path, Json, method};
use actix_web::http::{ContentEncoding, Method};
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};
use std::collections::HashMap;
use core::fmt::Debug;
use serde_json::value::Value::Array;
use uuid::Uuid;
use eui48::{MacAddress, Eui48};
use config::{Config, File};

const PAGE_SIZE: i32 = 100;

lazy_static! {
    static ref SESSION: Session = {

        let args: Vec<_> = env::args().collect();
        let mut settings = config::Config::default();
        settings.merge(File::with_name(&args[1])).unwrap();
        let ip_addr = settings.get::<String>("tsc.ip").unwrap();
        let port_num = settings.get::<u16>("tsc.port").unwrap();
        let thread_num = settings.get::<u32>("thread.num").unwrap();

        Cluster::default()
            .set_contact_points(&ip_addr)
                .expect("Failed to set_contact_points")
            .set_port(port_num)
                .expect("Failed to set_port")
            .set_num_threads_io(thread_num)
                .expect("Failed to set_num_threads_io")
            .set_retry_policy(RetryPolicy::default_new())
            .connect()
                .expect("Failed to connect to the cluster")
    };
}

fn time_series_api(session: &Session, dev: &str, epoch: &str, feature_name: &str, post_filter: Vec<String>) -> Status {

    // If status_bool is true, status_result is null
    let (status_result, status_bool) = time_status(epoch);
    let mut time_series_data = Vec::new();

    if status_bool == false {
        return status_result
    } else if Uuid::parse_str(dev).is_ok() == false {
        return Status {
            msg: Msg::UuidFormedBadly,
            result: Msg::Failed,
            data: _Value::Array(Vec::new()),
        }
    }

    let feature = feature_replace(feature_name);

    //Change the feature name obtained by url to the table name of cassandra
    let table_name = feature_to_table(&feature);
    let table_bool = table_schema(session, &table_name);

    if table_bool == false {
        return Status {
            msg: Msg::UnknownFeature,
            result: Msg::Failed,
            data: _Value::Array(Vec::new()),
        }
    }

    let (start, end, status) = split_once(epoch);
    let start_timestamp = start.parse::<i64>().unwrap();
    let end_timestamp = end.parse::<i64>().unwrap();
    let (date, start_time, end_time) = get_time_slices( start_timestamp, end_timestamp);

    for (day, (start_epoch, end_epoch)) in date.iter().zip(start_time.iter().zip(end_time)) {
        //Get method
        if post_filter.is_empty() {
            let cql_cmd = cql_syntax(&table_name, dev, day, start_epoch, &end_epoch);
            match _time_series_process(session, cql_cmd, &table_name, post_filter.clone()) {
                Ok(cass_data) => {
                    time_series_data.extend(cass_data)
                },
                Err(CassandraError::UnknownFeature) => {
                    error!("unknown feature => {:?}", feature_name);
                    return Status {
                        msg: Msg::UnknownFeature,
                        result: Msg::Failed,
                        data: _Value::Array(Vec::new())
                    }
                },
            }

        }
        //POST method
        else {
            let filter_str = post_filter.join(",");
            let (field, field_bool) = _field_inspection(session, filter_str, table_name.clone());

            if field_bool == false {
                return Status {
                    msg: Msg::UnknownFilter,
                    result: Msg::Failed,
                    data: _Value::Array(Vec::new())
                }
            }

            match table_name.as_str() {
                "t_log_mgnt_date"|
                "t_event_log"|
                "t_zyxelnebulastatisticseventlog" => {
                    let cql_cmd = cql_syntax(&table_name, dev, day, start_epoch, &end_epoch);
                    match _time_series_process(session, cql_cmd, &table_name, post_filter.clone()) {
                        Ok(cass_data) => {
                            time_series_data.extend(cass_data)
                        },
                        Err(CassandraError::UnknownFeature) => {
                            error!("unknown feature => {:?}", feature_name);
                            return Status {
                                msg: Msg::UnknownFeature,
                                result: Msg::Failed,
                                data: _Value::Array(Vec::new())
                            }
                        },
                    }

                }
                _ => {
                    let cql_cmd = post_cql_syntax(&table_name, dev, day, start_epoch, &end_epoch, &field);
                    match _time_series_process(session, cql_cmd, &table_name, post_filter.clone()) {
                        Ok(cass_data) => {
                            time_series_data.extend(cass_data)
                        },
                        Err(CassandraError::UnknownFeature) => {
                            error!("unknown feature => {:?}", feature_name);
                            return Status {
                                msg: Msg::UnknownFeature,
                                result: Msg::Failed,
                                data: _Value::Array(Vec::new())
                            }
                        },
                    }
                }
            }
        }
    }

    let data_return = match time_series_data.len() {

        0 => {
            let data_to_json = to_value(time_series_data).expect("data type was vec");
            return Status {
                msg: Msg::Ok,
                result: Msg::Ok,
                data: data_to_json,
            };
        }
        _ => {
            let data_to_json = to_value(time_series_data).expect("data type was vec");
            return Status {
                msg: Msg::Ok,
                result: Msg::Ok,
                data: data_to_json,
            };
        }
    };

    fn _time_series_process(session: &Session, cql_cmd: String, table_name: &str, post_filter: Vec<String>) -> _Result<Vec<HashMap<String, _Value>>, CassandraError,> {
        let mut has_more_pages = true;

        session.execute(&stmt!("USE nebula_device_data")).wait().unwrap();
        let mut statement = stmt!(cql_cmd.as_str());
        statement.set_consistency(QUORUM).unwrap();
        statement.set_paging_size(PAGE_SIZE).unwrap();

        match table_name {
            "t_log_mgnt_date" => {
                let log_value = _log_handle_func(session, "log_entry", statement);
                return Ok(log_value)
            }
            "t_event_log" => {
                let log_value = _log_handle_func(session, "list", statement);
                return Ok(log_value)
            }
            "t_zyxelnebulastatisticseventlog" => {
                let mut rule_out_counter = 0;
                let mut _percent = 0.0;

                let log_value = _log_handle_func(session, "zyxelNebulaStatisticsEventLogEntry", statement);

                for nebula_event_log in log_value.iter() {
                    if nebula_event_log["zynebulastatisticseventlogdescription"] == "Cloud: DNS Requery for d.nebula.zyxel.com" {
                        rule_out_counter += 1
                    }
                }

                let _len = log_value.len() as i32;
                if _len > 0 {
                    let percent = (100 * (rule_out_counter / _len));
                    _percent = round::ceil(percent as f64, 2);
                }
                warn!("number of ruled out log: {}, total logs: {} , rule out percent: {}%", rule_out_counter, _len, _percent);

                return Ok(log_value)
            }
            _ => {

                if post_filter.is_empty() {
                    statement.set_paging_size(PAGE_SIZE).unwrap();
                    let mut has_more_pages = true;
                    let mut value_list = Vec::new();
                    let mut column = column_schema(session, "*", &table_name);

                    while has_more_pages {

                        let result = session.execute(&statement).wait().unwrap();

                        for row in result.iter() {
                            let mut map = HashMap::new();

                            for column in column.iter() {

                                let mut json_value = _Value::Null;
                                let column_name = column.to_string();
                                let column_value = row.get_column_by_name(column.to_string()).unwrap();
                                if column_value.is_collection() {
                                    if column_value.is_null() {
                                        json_value = parse_value_func(column_value);
                                    } else {
                                        let items_iterator = column_value.get_set().unwrap();
                                        let mut list_udt = Vec::new();

                                        for items in items_iterator {
                                            match items.get_type() {
                                                ValueType::UDT => {
                                                    list_udt.push(query_collection_func(items));
                                                    json_value = json!(list_udt);
                                                }
                                                _ => {
                                                    match items.is_null() {
                                                        false => warn!("non udt, value is => {:?}", parse_value_func(items)),
                                                        true => error!("non udt type, value is null")
                                                    }
                                                }
                                            }
                                        }
                                    }
                                    //Not collections type
                                } else {
                                    json_value = parse_value_func(column_value);
                                }
                                map.insert(column_name,json_value);
                            }
                            value_list.push(map);
                        }
                        has_more_pages = result.has_more_pages();
                        if has_more_pages {
                            statement.set_paging_state(result).unwrap();
                        }
                    }
                    return Ok(value_list)

                } else {
                    statement.set_paging_size(PAGE_SIZE).unwrap();
                    let mut has_more_pages = true;
                    let mut value_list = Vec::new();
                    let mut column = column_schema(session, "*", &table_name);

                    while has_more_pages {

                        let result = session.execute(&statement).wait().unwrap();

                        for row in result.iter() {
                            let mut map = HashMap::new();

                            for filter_field in post_filter.iter() {

                                let mut json_value = _Value::Null;
                                let filter_column = filter_field.to_string();
                                let column_value = row.get_column_by_name(filter_field.to_string()).unwrap();
                                if column_value.is_collection() {
                                    if column_value.is_null() {
                                        json_value = parse_value_func(column_value);
                                    } else {
                                        let items_iterator = column_value.get_set().unwrap();
                                        let mut list_udt = Vec::new();

                                        for items in items_iterator {
                                            match items.get_type() {
                                                ValueType::UDT => {
                                                    list_udt.push(query_collection_func(items));
                                                    json_value = json!(list_udt);
                                                }
                                                _ => {
                                                    match items.is_null() {
                                                        false => warn!("non udt, value is => {:?}", parse_value_func(items)),
                                                        true => error!("non udt type, value is null")
                                                    }
                                                }
                                            }
                                        }
                                    }
                                    //Not collections type
                                } else {
                                    json_value = parse_value_func(column_value);
                                }
                                map.insert(filter_column,json_value);
                            }
                            value_list.push(map);
                        }
                        has_more_pages = result.has_more_pages();
                        if has_more_pages {
                            statement.set_paging_state(result).unwrap();
                        }
                    }
                    return Ok(value_list)
                }
            }
        }
    }

    fn _field_inspection(session: &Session, filter_str: String, table_name: String) -> (String, bool) {
        let split = filter_str.split(",");
        let vec = split.collect::<Vec<&str>>();
        let mut field_bool = false;

        let mut field_bool = column_schema_inspection(session, vec, &table_name);
        (filter_str, field_bool)
    }
}

fn time_series_web(url: Path<Url>) -> HttpResponse {
    let data =  time_series_api(&*SESSION, &url.deviceid, &url.epoch, &url.feature, vec![]);
    let mut response_builder = match data.msg {
        | Msg::Ok => HttpResponse::Ok(),
        | _ => HttpResponse::BadRequest(),
    };
    response_builder
        .content_type("text/html")
        .encoding(ContentEncoding::Gzip)
        .json(data)

}

fn main() {

    let args: Vec<_> = env::args().collect();

    if args.len() == 1 {
        error!("Please input config path!!");
    }
    let mut settings = config::Config::default();
    settings.merge(File::with_name(&args[1])).unwrap();
    let web_worker = settings.get::<usize>("web.worker").unwrap();
    let web_port = settings.get::<String>("web.port").unwrap();
    let _ = setup_logger(&args[1]);
    
    HttpServer::new(|| {
        App::new()
            .wrap(Compress::new(ContentEncoding::Gzip))
            .wrap(Logger::default())
            .service(web::scope("/data/1")
                .wrap(NormalizePath)
                .route("/{deviceid}/{epoch}/{feature}", web::post().to(log_post_web))
                .route("/{deviceid}/{epoch}/{feature}", web::get().to(time_series_web))
            .route("/health", web::to(|| HttpResponse::Ok().body("ok")))
    })
        .workers(web_worker)
        .bind("0.0.0.0:".to_owned() + &web_port)
        .unwrap()
        .run();

}
