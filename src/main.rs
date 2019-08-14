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
extern crate multizip;

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
use multizip::{zip3, zip2};

const PAGE_SIZE: i32 = 5000;

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

    for (day, start_epoch, end_epoch) in zip3(date, start_time, end_time) {
        //Get method
        if post_filter.is_empty() {
            let cql_cmd = cql_syntax(&table_name, dev, &day, &start_epoch, &end_epoch);
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
                    let cql_cmd = cql_syntax(&table_name, dev, &day, &start_epoch, &end_epoch);
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
                    let cql_cmd = post_cql_syntax(&table_name, dev, &day, &start_epoch, &end_epoch, &field);
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

    fn _time_series_process(session: &Session, cql_cmd: String, table_name: &str, post_filter: Vec<String>) -> _Result<Vec<Map<String, _Value>>, CassandraError,> {
        let mut has_more_pages = true;

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
                    let mut has_more_pages = true;
                    let mut value_list = Vec::new();
                    let mut column = column_schema(session, "*", &table_name);

                    while has_more_pages {
                        let result = session.execute(&statement).wait().unwrap();

                        for row in result.iter() {
                            let mut map = Map::new();

                            for column in column.iter() {

                                let mut json_value = _Value::Null;
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
                                                    let udt_value = query_collection_func(items);
                                                    let udt_json = serde_json::to_value(udt_value).unwrap();
                                                    list_udt.push(udt_json);
                                                    json_value = _Value::Array(list_udt.clone());

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
                                } else {
                                    //Not collections type
                                    json_value = parse_value_func(column_value);
                                }
                                map.insert(column.to_string(),json_value);
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
                            let mut map = Map::new();

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
                                                    let udt_value = query_collection_func(items);
                                                    let udt_json = serde_json::to_value(udt_value).unwrap();
                                                    list_udt.push(udt_json);
                                                    json_value = _Value::Array(list_udt.clone());
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

    let data_to_json = to_value(time_series_data).expect("data type was vec");
    return Status {
        msg: Msg::Ok,
        result: Msg::Ok,
        data: data_to_json,
    };
}

fn _log_handle_func(session: &Session, log_field: &str, mut statement: Statement) -> Vec<Map<String, _Value>> {
    let mut has_more_pages = true;
    let mut value_list = Vec::new();
    while has_more_pages {
        let result = session.execute(&statement).wait().unwrap();
        for row in result.iter() {
            let column_value = row.get_column_by_name(log_field).unwrap();

            if column_value.is_null() {
                let null_map = Map::new();
                value_list.push(null_map);
            } else {
                //The type of log_entry is set to list[udt]
                let items_iterator = column_value.get_set().unwrap();

                for items in items_iterator {
                    let mut udt_value = query_collection_func(items);
                    let value = row.get_column_by_name("deviceid").unwrap();
                    let device_value = parse_value_func(value);
                    udt_value.insert("deviceid".to_string(), device_value);
                    value_list.push(udt_value);
                }
            }
        }
        has_more_pages = result.has_more_pages();
        if has_more_pages {
            statement.set_paging_state(result).unwrap();
        }
    }
    value_list
}

fn filter_time_series_api(session: &Session, dev: &str, epoch: &str, feature_name: &str , filter: i32, post_filter: Vec<String>) -> Status {

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

    for (start_epoch, end_epoch) in zip2(start_time, end_time) {
        //Get method
        if post_filter.is_empty() {
            let cql_cmd = cql_filter_syntax(&table_name, dev, &start_epoch, &end_epoch);
            match _filter_time_series_process(session, cql_cmd, &table_name, filter) {
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

            let cql_cmd = post_cql_filter_syntax(&table_name, dev,  &start_epoch, &end_epoch, &field);
            match _post_filter_time_series_process(session, cql_cmd, &table_name, filter, post_filter.clone()) {
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

    fn _filter_time_series_process(session: &Session, cql_cmd: String, table_name: &str, filter: i32) -> _Result<Vec<HashMap<String, _Value>>, CassandraError,>{

        let mut value_list = Vec::new();
        let mut has_more_pages = true;

        session.execute(&stmt!("USE nebula_device_data")).wait().unwrap();
        let mut statement = stmt!(cql_cmd.as_str());
        statement.set_consistency(QUORUM).unwrap();
        statement.set_paging_size(PAGE_SIZE).unwrap();

        while has_more_pages {
            let result = session.execute(&statement).wait().unwrap();
            let mut column = column_schema(session, "*", &table_name);

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
                            for items in items_iterator {

                                match items.get_type() {
                                    ValueType::UDT => {
                                        let mut udt_value = query_collection_func(items);
                                        let mut filter_list = Vec::new();

                                        let filter_point = _find_rawdata_by_port_num(table_name);
                                        let filter_str = filter_point.as_str();

                                        let filter_match = match filter_str {
                                            "t_lldpstatistics" => {
                                                if udt_value.contains_key("lldpstatsrxportnum") == true {
                                                    if udt_value["lldpstatsrxportnum"] == json!(filter) {
                                                        filter_list.push(udt_value);
                                                        json_value = json!(filter_list);
                                                    }
                                                } else if udt_value.contains_key("lldpstatstxportnum") == true {
                                                    if udt_value["lldpstatstxportnum"] == json!(filter) {
                                                        filter_list.push(udt_value);
                                                        json_value = json!(filter_list);
                                                    }
                                                }
                                            }
                                            "t_dot1dtp" => {
                                                if udt_value.contains_key("dot1dtpfdbport") == true {
                                                    if udt_value["dot1dtpfdbport"] == json!(filter) {
                                                        filter_list.push(udt_value);
                                                        json_value = json!(filter_list);
                                                    }
                                                } else if udt_value.contains_key("dot1dtpport") == true {
                                                    if udt_value["dot1dtpport"] == json!(filter) {
                                                        filter_list.push(udt_value);
                                                        json_value = json!(filter_list);
                                                    }
                                                }
                                            }
                                            ///filter data content equal no filter
                                            "t_zyxeligmpsnoopingstatus" => {
                                                filter_list.push(udt_value);
                                                json_value = json!(filter_list);
                                            }
                                            _ => {
                                                if udt_value[filter_str] == json!(filter) {
                                                    filter_list.push(udt_value);
                                                    json_value = json!(filter_list);
                                                }
                                            }
                                        };
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

        Ok(value_list)

    }

    fn _post_filter_time_series_process(session: &Session, cql_cmd: String, table_name: &str, filter: i32, post_filter: Vec<String>)
        -> _Result<Vec<HashMap<String, _Value>>, CassandraError,> {

        let mut value_list = Vec::new();
        let mut has_more_pages = true;

        session.execute(&stmt!("USE nebula_device_data")).wait().unwrap();
        let mut statement = stmt!(cql_cmd.as_str());
        statement.set_consistency(QUORUM).unwrap();
        statement.set_paging_size(PAGE_SIZE).unwrap();

        while has_more_pages{
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

                            for items in items_iterator {
                                match items.get_type() {
                                    ValueType::UDT => {
                                        let mut udt_value = query_collection_func(items);
                                        let mut filter_list = Vec::new();

                                        let filter_point = _find_rawdata_by_port_num(table_name);
                                        let filter_str = filter_point.as_str();

                                        let filter_match = match filter_str {
                                            "t_lldpstatistics" => {
                                                if udt_value.contains_key("lldpstatsrxportnum") == true {
                                                    if udt_value["lldpstatsrxportnum"] == json!(filter) {
                                                        filter_list.push(udt_value);
                                                        json_value = json!(filter_list);
                                                    }
                                                } else if udt_value.contains_key("lldpstatstxportnum") == true {
                                                    if udt_value["lldpstatstxportnum"] == json!(filter) {
                                                        filter_list.push(udt_value);
                                                        json_value = json!(filter_list);
                                                    }
                                                }
                                            }
                                            "t_dot1dtp" => {
                                                if udt_value.contains_key("dot1dtpfdbport") == true {
                                                    if udt_value["dot1dtpfdbport"] == json!(filter) {
                                                        filter_list.push(udt_value);
                                                        json_value = json!(filter_list);
                                                    }
                                                } else if udt_value.contains_key("dot1dtpport") == true {
                                                    if udt_value["dot1dtpport"] == json!(filter) {
                                                        filter_list.push(udt_value);
                                                        json_value = json!(filter_list);
                                                    }
                                                }
                                            }
                                            ///filter data content equal no filter
                                            "t_zyxeligmpsnoopingstatus" => {
                                                filter_list.push(udt_value);
                                                json_value = json!(filter_list);
                                            }
                                            _ => {
                                                if udt_value[filter_str] == json!(filter) {
                                                    filter_list.push(udt_value);
                                                    json_value = json!(filter_list);
                                                }
                                            }
                                        };
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
        Ok(value_list)
    }

    fn _field_inspection(session: &Session, filter_str: String, table_name: String) -> (String, bool) {
        let split = filter_str.split(",");
        let vec = split.collect::<Vec<&str>>();
        let mut field_bool = false;

        let mut field_bool = column_schema_inspection(session, vec, &table_name);
        (filter_str, field_bool)
    }

    fn _find_rawdata_by_port_num(table_name: &str) -> String{

        match table_name {
            "t_lldplocalsystemdata" => return "lldpLocPortNum".to_lowercase(),
            "t_zyxelnebulastatisticsportcounter"  => return "zyNebulaStatisticsPortCounterPort".to_lowercase(),
            "t_interfaces" => return "ifIndex".to_lowercase(),
            "t_dot1dbase" => return "dot1dBasePort".to_lowercase(),
            "t_dot1dstp" => return "dot1dStpPort".to_lowercase(),
            "t_zyxelportstatus" => return "dot1dBasePort".to_lowercase(),
            "t_zyxelarpstatus" => return "zyArpPort".to_lowercase(),
            "t_pethobjects" => return "pethPsePortIndex".to_lowercase(),
            "t_zyxelnebulastatisticspoe" => return "zyNebulaStatisticsPoePort".to_lowercase(),
            "t_zyxelpoestatus" => return "dot1dBasePort".to_lowercase(),
            "t_lldpstatistics"|"t_dot1dtp" => return table_name.to_string(),
            _ => return table_name.to_string(),
        }
    }

    let data_to_json = to_value(time_series_data).expect("data type was vec");
    return Status {
        msg: Msg::Ok,
        result: Msg::Ok,
        data: data_to_json,
    };

}

fn helper_read_api(session: &Session, dev: &str, feature_name: &str, value: &str, sub_value: &str) -> Status {

    let mut value_list = Vec::new();
    let unknown_feature = Status {
        msg: Msg::UnknownFeature,
        result: Msg::Failed,
        data: _Value::Array(Vec::new()),
    };

    if sub_value.is_empty() {

        if feature_name == "connectivity" {
            match value {
                "current-public-ip" =>{
                    let data_lists = _get_current_public_ip(session, dev);
                    value_list.push(data_lists);
                }

                "last-connect" => {
                    let cql_cmd = get_last_connect_cql(dev);
                    let mut column = vec!["epoch", "outter_ipv4"];
                    let helper_value = _get_helper_value_func(cql_cmd, column, session);
                    value_list = helper_value;

                }

                "last-disconnect" => {
                    let cql_cmd = get_last_disconnect_cql(dev);
                    session.execute(&stmt!("USE nebula_device_data")).wait().unwrap();
                    let mut statement = stmt!(cql_cmd.as_str());
                    statement.set_consistency(QUORUM);
                    let result = session.execute(&statement).wait().unwrap();
                    for row in result.iter() {
                        let mut map = HashMap::new();
                        let mut json_value = _Value::Null;
                        let column_value = row.get_column_by_name("epoch".to_string()).unwrap();
                        json_value = parse_value_func(column_value);
                        map.insert("epoch", json_value);
                        value_list.push(map);
                    }

                }

                "first-connect" => {
                    let cql_cmd = get_first_connect_cql(dev);
                    let mut column = vec!["epoch", "outter_ipv4"];
                    let helper_value = _get_helper_value_func(cql_cmd, column, session);
                    value_list = helper_value;

                }

                _ => {
                    if value.contains("-") {
                        let cql_cmd = _get_connectivity_period_cql(dev, value);
                        let mut column = vec!["deviceid", "epoch", "entry_type", "outter_ipv4"];
                        let helper_value = _get_helper_value_func(cql_cmd, column, session);
                        value_list = helper_value;

                    } else {
                        return unknown_feature
                    }
                }
            }

        } else if feature_name == "slowconnection" {
            if value.contains("-") {
                let (start, end, status) = split_once(value);
                let cql_cmd = format!("select blobAsBigint(timestampAsBlob(epoch)) as epoch, outter_ipv4 \
                    from t_helper_slowconnection where deviceid='{dev}' \
                    and epoch>={start} and epoch<{end}", dev=dev, start=start, end=end);
                let mut column = vec!["deviceid", "epoch", "entry_type", "outter_ipv4"];
                let helper_value = _get_helper_value_func(cql_cmd, column, session);
                value_list = helper_value;
            } else {
                return unknown_feature
            }
        } else {
            return unknown_feature
        }

    } else {

        if feature_name == "connectivity" {
            let mut column = vec!["epoch", "outter_ipv4"];

            if sub_value.parse::<i64>().is_ok() == false {
                return Status {
                    msg: Msg::EpochLastStatus,
                    result: Msg::Failed,
                    data: _Value::Array(Vec::new()),
                }
            }

            match value {
                "last-connect" => {
                    let cql_cmd = get_connectivity_before(dev, sub_value, "connect");
                    let helper_value = _get_helper_value_func(cql_cmd, column, session);
                    value_list = helper_value;

                }
                "last-disconnect" => {
                    let cql_cmd = get_connectivity_before(dev, sub_value, "disconnect");
                    let helper_value = _get_helper_value_func(cql_cmd, column, session);
                    value_list = helper_value;

                }
                "first-connect" => {
                    let cql_cmd = get_connectivity_before(dev, sub_value, "connect");
                    let helper_value = _get_helper_value_func(cql_cmd, column, session);
                    value_list = helper_value;

                }
                _ =>{
                    return unknown_feature
                }
            }

        } else {
            return unknown_feature
        }
    }

    fn _get_helper_value_func(cql_cmd: String, column: Vec<&'static str>, session: &Session) -> Vec<HashMap<&'static str, _Value>> {
        let mut value_list = Vec::new();
        session.execute(&stmt!("USE nebula_device_data")).wait().unwrap();
        let mut statement = stmt!(cql_cmd.as_str());
        statement.set_consistency(QUORUM);
        let result = session.execute(&statement).wait().unwrap();

        for row in result.iter() {
            let mut map = HashMap::new();

            for column_index in 0..column.len() {
                let mut json_value = _Value::Null;
                let column_value = row.get_column(column_index).unwrap();
                json_value = parse_value_func(column_value);
                map.insert(column[column_index], json_value);
            }
            value_list.push(map);
        }
        value_list
    }

    fn _get_current_public_ip(session: &Session, dev: &str) -> HashMap<&'static str, _Value> {

        let mut cnt_time = _Value::Null;
        let mut discnt_time = _Value::Null;
        let mut outter_ipv4 = _Value::Null;
        let mut map = HashMap::new();

        let cql_connect = get_last_connect_cql(dev);
        session.execute(&stmt!("USE nebula_device_data")).wait().unwrap();
        let mut statement = stmt!(cql_connect.as_str());
        statement.set_consistency(QUORUM);
        let result = session.execute(&statement).wait().unwrap();
        for row in  result.iter(){
            let cnt_time_value = row.get_column_by_name("epoch".to_string()).unwrap();
            cnt_time = parse_value_func(cnt_time_value);
            let outter_ipv4_value = row.get_column_by_name("outter_ipv4".to_string()).unwrap();
            outter_ipv4 = parse_value_func(outter_ipv4_value);
            break
        }

        let cql_disconnect = get_last_disconnect_cql(dev);
        session.execute(&stmt!("USE nebula_device_data")).wait().unwrap();
        let mut statement = stmt!(cql_disconnect.as_str());
        statement.set_consistency(QUORUM);
        let result = session.execute(&statement).wait().unwrap();
        for row in  result.iter(){
            let discnt_time_value = row.get_column_by_name("epoch".to_string()).unwrap();
            discnt_time = parse_value_func(discnt_time_value);
            break
        }

        if _is_connected(cnt_time, discnt_time) {
            map.insert("outter_ipv4", outter_ipv4);
            return map
        } else {
            map.insert("outter_ipv4", _Value::String("0.0.0.0".to_string()));
            return map
        }
    }

    let data_to_json = to_value(value_list).expect("data type was vec");

    Status {
        msg: Msg::Ok,
        result: Msg::Ok,
        data: data_to_json,
    }
}

fn parse_value_func(value: Value) -> _Value {

    if value.is_null() {
        _Value::Null
    } else {
        match value.get_type() {
            ValueType::VARCHAR |
            ValueType::ASCII |
            ValueType::TEXT => {
                _Value::String(value.get_string().unwrap())
            }
            ValueType::VARINT => {
                let mut var = value.get_bytes().unwrap();
                let mut counter: i128 = 0;
                let var_length = var.len();

                for slice_number in 0..var_length {
                    let i = var_length - slice_number- 1;
                    let pow = 256i128.pow(i as u32);
                    let result = var[slice_number] as i128 * pow;
                    counter += result;
                }
                let varint = _Value::from(counter);
                varint

            }
            ValueType::INT => {
                _Value::from(value.get_i32().unwrap())
            }
            ValueType::DOUBLE|
            ValueType::FLOAT => {
                let f = value.get_f64().unwrap();
                let float = _Value::from(f);
                float
            }
            ValueType::TIMESTAMP |
            ValueType::BIGINT => {

                _Value::from(value.get_i64().unwrap())
            }
            ValueType::BOOLEAN => {
                _Value::Bool(value.get_bool().unwrap())

            }
            ValueType::TIMEUUID |
            ValueType::UUID => {
                _Value::String(value.get_uuid().unwrap().to_string())
            }
            ValueType::INET => {
                let i = value.get_inet().unwrap();
                let ip_out = IpAddr::from(&i);
                _Value::String(ip_out.to_string())
            }
            ValueType::LIST|
            ValueType::SET => {
                let mut list = Vec::new();
                let items_field = value.get_set().unwrap();
                for item in items_field {
                    let parse_value = parse_value_func(item);
                    list.push(parse_value);
                }
                _Value::Array(list)
            }
            _ => {
                error!("Non support type => {:?}", value.get_type());
                _Value::Null
            }
        }
    }

}

fn query_collection_func(value: Value) -> Map<String, _Value> {
    let mut map_udt = Map::new();

    let mut udt_iterator = value.get_user_type().unwrap();

    for (udt_column, udt_value) in udt_iterator {

        //The field attribute of "zynebulastatisticseventlogtimestamp" is String, here is converted to epoch type
        if udt_column == "zynebulastatisticseventlogtimestamp".to_string() {
            let time_string = udt_value.get_string().unwrap();
            let time = Utc.datetime_from_str(time_string.as_str(), "%Y-%m-%d %H:%M:%S").unwrap();
            let epoch = time.timestamp() * 1000;
            map_udt.insert(udt_column, _Value::Number(epoch.into()));

        } else {
            let value = parse_value_func(udt_value);
            map_udt.insert(udt_column, value);
        }
    }

    map_udt
}

fn data_service_special_api(session: &Session, dev: &str, epoch: &str, feature_name: &str , mac: Vec<String>) -> Status{

    let feature = feature_replace(feature_name);
    let table_name = feature_to_table(&feature);
    let table_bool = table_schema(session, &table_name);
    let mut value_list = Vec::new();

    if table_bool == false {
        return Status {
            msg: Msg::UnknownFeature,
            result: Msg::Failed,
            data: _Value::Array(Vec::new()),
        };
    } else if Uuid::parse_str(dev).is_ok() == false {
        return Status {
            msg: Msg::UuidFormedBadly,
            result: Msg::Failed,
            data: _Value::Array(Vec::new()),
        };
    } else if epoch != "latest" {
        return Status {
            msg: Msg::NotEnoughValues,
            result: Msg::Failed,
            data: _Value::Array(Vec::new()),
        };
    }

    session.execute(&stmt!("USE nebula_device_data")).wait().unwrap();
    let field = "device_id, blobasbigint(timestampasblob(epoch)) as epoch, mac, detection_method, hostname, os";
    let mut column = column_schema(session,"*", &table_name);
    for mac_addr in mac.iter() {

        if MacAddress::parse_str( mac_addr ).is_ok() == false {
            return Status {
                msg: Msg::MacFormedBadly,
                result: Msg::Failed,
                data: _Value::Array(Vec::new()),
            };
        }

        let cql_cmd = format!("select {select_field} from {table_name} where device_id={dev} and mac='{mac}' order by epoch desc limit 1;",
                              select_field=field, table_name=table_name, dev=dev, mac=mac_addr);

        let mut statement = stmt!(cql_cmd.as_str());
        statement.set_consistency(QUORUM);
        let result = session.execute(&statement).wait().unwrap();

        for row in result.iter() {
            let mut map = HashMap::new();
            let mut json_value = _Value::Null;

            for column_name in column.iter() {
                let column_value = row.get_column_by_name(column_name.to_string()).unwrap();
                json_value = parse_value_func(column_value);
                map.insert(column_name, json_value);
            }
            value_list.push(map);
        }
    }

    if value_list.is_empty() {
        let data_to_json = to_value(value_list).expect("data type was vec");
        return Status {
            msg: Msg::Ok,
            result: Msg::Ok,
            data: data_to_json,
        };
    } else {
        let data_to_json = to_value(value_list).expect("data type was vec");
        return Status {
            msg: Msg::Ok,
            result: Msg::Ok,
            data: data_to_json,
        };
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

fn invalid_body_web(url: Path<Url>) -> HttpResponse {

    let data = Status {
        msg: Msg::InvalidRequest,
        result: Msg::Failed,
        data: _Value::Array(Vec::new()),
    };
    HttpResponse::BadRequest()
        .content_type("text/html")
        .json(data)
}

fn filter_data_web(url: Path<FilterUrl>) -> HttpResponse {

    let data =  filter_time_series_api(&*SESSION, &url.deviceid, &url.epoch, &url.feature, url.filter, vec![]);
    let mut response_builder = match data.msg {
        | Msg::Ok => HttpResponse::Ok(),
        | _ => HttpResponse::BadRequest(),
    };
    response_builder
        .content_type("text/html")
        .encoding(ContentEncoding::Gzip)
        .json(data)
}

fn helper_read_epoch_value_web(url: Path<HelperEpochUrl>) -> HttpResponse {

    let data = helper_read_api(&*SESSION, &url.deviceid, &url.feature, &url.value, &url.sub_value );
    let mut response_builder = match data.msg {
        | Msg::Ok => HttpResponse::Ok(),
        | _ => HttpResponse::BadRequest(),
    };
    response_builder
        .content_type("text/html")
        .encoding(ContentEncoding::Gzip)
        .json(data)

}

fn helper_read_web(url: Path<HelperUrl>) -> HttpResponse {

    let data = helper_read_api(&*SESSION, &url.deviceid, &url.feature, &url.value, "");
    let mut response_builder = match data.msg {
        | Msg::Ok => HttpResponse::Ok(),
        | _ => HttpResponse::BadRequest(),
    };
    response_builder
        .content_type("text/html")
        .encoding(ContentEncoding::Gzip)
        .json(data)

}

///POST body
/// {
///    "filter": ["deviceid", "epoch", ...],
/// }
/// extract `Info` using serde
fn log_post_web(info: web::Json<PostFilter>, url: web::Path<Url>) -> HttpResponse {
    info!("{:?}", info);
    let data = time_series_api(&*SESSION, &url.deviceid, &url.epoch, &url.feature,  info.filter.clone());
    let mut response_builder = match data.msg {
        | Msg::Ok => HttpResponse::Ok(),
        | _ => HttpResponse::BadRequest(),
    };
    response_builder
        .content_type("text/html")
        .encoding(ContentEncoding::Gzip)
        .json(data)
}

///POST body
/// {
///    "clients": ["MAC", "MAC", ...],
/// }
/// extract `Info` using serde
fn clients_post_web(info: web::Json<PostInfo>, url: web::Path<PostUrl>) -> HttpResponse {
    info!("{:?}", info);
    let data = data_service_special_api(&*SESSION, &url.deviceid, "latest", &url.feature, info.clients.clone());
    let mut response_builder = match data.msg {
        | Msg::Ok => HttpResponse::Ok(),
        | _ => HttpResponse::BadRequest(),
    };
    response_builder
        .content_type("text/html")
        .encoding(ContentEncoding::Gzip)
        .json(data)
}

///POST body
/// {
///    "filter": ["deviceid", "epoch", ...],
/// }
/// extract `Info` using serde
fn filter_post_web(info: web::Json<PostFilter>, url: web::Path<FilterUrl>) -> HttpResponse {
    info!("{:?}", info);
    let data = filter_time_series_api(&*SESSION, &url.deviceid, &url.epoch, &url.feature, url.filter, info.filter.clone());
    let mut response_builder = match data.msg {
        | Msg::Ok => HttpResponse::Ok(),
        | _ => HttpResponse::BadRequest(),
    };
    response_builder
        .content_type("text/html")
        .encoding(ContentEncoding::Gzip)
        .json(data)
}

fn setup_logger(args: &str) -> Result<(), fern::InitError> {

    let mut settings = config::Config::default();
    settings.merge(File::with_name(args)).unwrap();
    let path = settings.get::<String>("log.path").unwrap();

    fern::Dispatch::new()
        .format(|out, message, record| {
            out.finish(format_args!(
                "{}[{}][{}] {}",
                chrono::Local::now().format("[%Y-%m-%d][%H:%M:%S]"),
                record.target(),
                record.level(),
                message
            ))
        })
        .level(log::LevelFilter::Info)
        .chain(fern::log_file(path.to_owned() + "data-service-rs.log")?)
        .apply()?;
    Ok(())
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
                .route("/{deviceid}/latest/{feature}", web::post().to(clients_post_web))
                .route("/{deviceid}/{epoch}/{feature}/{filter}", web::post().to(filter_post_web))
                .route("/{deviceid}/{epoch}/{feature}", web::post().to(log_post_web))
                .route("/{deviceid}/{epoch}/{feature}", web::get().to(time_series_web))
                .route("/{deviceid}/{epoch}/{feature}/", web::get().to(invalid_body_web))
                .route("/{deviceid}/{epoch}/{feature}/{filter}", web::get().to(filter_data_web)))
            .service(web::scope("/helper/1")
                .wrap(NormalizePath)
                .route("/{deviceid}/{feature}/{value}/{sub_value}", web::get().to(helper_read_epoch_value_web))
                .route("/{deviceid}/{feature}/{value}", web::get().to(helper_read_web)))
            .route("/health", web::to(|| HttpResponse::Ok().body("ok")))
    })
        .workers(web_worker)
        .bind("0.0.0.0:".to_owned() + &web_port)
        .unwrap()
        .run();

}
