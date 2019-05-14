#![allow(unused)]
extern crate cassandra_cpp_sys;
extern crate actix;
extern crate actix_web;

#[macro_use] extern crate log;
extern crate simplelog;

#[macro_use]
extern crate serde_derive;
extern crate serde;
extern crate serde_json;
extern crate serde_yaml;
extern crate chrono;
extern crate math;
extern crate time;

mod connect_util;
mod schema_util;
use crate::connect_util::*;
use crate::schema_util::*;
use std::collections::HashMap;
use simplelog::*;
use cassandra_cpp_sys::*;
use std::slice;
use std::mem;
use std::fs::File;
use std::fs;
use std::ffi::CString;
use std::collections::LinkedList;
use std::str::Utf8Error;
use std::ffi::CStr;
use std::str;
use std::str::FromStr;
use std::env;
use std::net::{Ipv4Addr, SocketAddrV4};

use serde::*;
use serde::{Deserialize, Serialize};
use serde_json::{Value, Map, Number, to_value};
use serde_json::json;
use log::Level;
use log::LevelFilter;
use time::Duration;
use chrono::prelude::*;
use math::round;
use actix_web::{
    error, http, middleware, server, App, AsyncResponder, Error, HttpMessage,
    HttpRequest, HttpResponse, Json, Responder,Path,
    Result,
};
use futures::future::{result, FutureResult};
use futures::{Future, Stream};
use futures::sync::mpsc;
use actix_web::http::{header, Method, StatusCode,};
use bytes::Bytes;
use futures::sink::Sink;
use actix_web::server::HttpServer;
use actix_web::http::header::ContentEncoding;

use std::thread;
use std::ptr::null;
use std::os::raw::c_char;
use std::io::prelude::*;

#[derive(Deserialize)]
struct Url {
    deviceid: String,
    epoch: String,
    feature: String,
}

const CASS_UUID_STRING_LENGTH: usize = 37;
const CASS_INET_STRING_LENGTH: usize = 46;

unsafe fn cassandra_use(session: &mut CassSession, primary_key: &str, co_primary_key: &str, feature_name: &str) -> HashMap<String, Value> {

    let feature = feature_replace(feature_name);

    let mut status_map = HashMap::new();
//    let mut status_map = serde_json::Map::new();
    let mut result_link = Vec::new();
    let mut status_link = Vec::new();

    // If status_bool is true, status_result is null
    let (status_result, status_bool) = time_status(co_primary_key);

    let (start, end, status) = split_once(co_primary_key);

    let start_timestamp = start.parse::<i64>().unwrap();
    let end_timestamp = end.parse::<i64>().unwrap();

    //Calculate the time interval passed in and cut. Returns the vec of day, start_time, and end_time.
    let (date, start_time, end_time) = get_time_slices( start_timestamp, end_timestamp);

    for (day, (start,end)) in date.iter().zip(start_time.iter().zip(end_time)) {
        let (cass_data, cass_status) = cassandra_connect(session, primary_key, &day, &feature, &start, &end);

        if cass_status.contains("ok") {
            result_link.extend(cass_data);
        } else {
            status_link.push(cass_status);
        }
    }

    if status_link.contains(&"unknown feature".to_string()) {
        error!("unknown feature");
        let data_to_json = to_value(result_link).expect("data type was vec");
        status_map.insert("msg".to_string(), Value::String("unknown feature".to_string()));
        status_map.insert("result".to_string(), Value::String("unknown feature".to_string()));
        status_map.insert("data".to_string(), data_to_json);
        return status_map
    } else if result_link.len() == 0 {
        let data_to_json = to_value(result_link).expect("data type was vec");
        status_map.insert("msg".to_string(), Value::String("ok".to_string()));
        status_map.insert("result".to_string(), Value::String("ok".to_string()));
        status_map.insert("data".to_string(), data_to_json);
        return status_map
    }

    let data_to_json = to_value(result_link).expect("data type was vec");
    status_map.insert("msg".to_string(), Value::String("ok".to_string()));
    status_map.insert("result".to_string(), Value::String("ok".to_string()));
    status_map.insert("data".to_string(), data_to_json);
    status_map

}

//return type is LinkedList
unsafe fn cassandra_connect(session: &mut CassSession, primary_key: &str, co_primary_key: &str, feature_name: &str, start_day: &str, end_day: &str)
    -> (Vec<HashMap<&'static str, Value>>, String) {

    let mut has_more_pages = true;
    let table_name = feature_to_table(feature_name);
    let table_bool = table_schema(session, "*", "example", table_name.as_str());

    //LinkedList,collect data for each query.
    let mut value_list = Vec::new();

    if table_bool == false {
        return (value_list, "unknown feature".to_string())
    }

    let query_check = table_check(feature_name, primary_key, co_primary_key, start_day, end_day);

    let statement = cass_statement_new(CString::new(query_check).unwrap().as_ptr(), 0);
    cass_statement_set_paging_size(statement, 100);

    while has_more_pages {
        let future = cass_session_execute(session, statement);

        match cass_future_error_code(future) {
            CASS_OK => {
                let result = cass_future_get_result(future);
                let iterator = cass_iterator_from_result(result);
                //looking for the value of column.The type of the return is Vec<String>
                let mut column = column_schema(session, "*", "example", table_name.as_str());
                cass_future_free(future);

                while cass_iterator_next(iterator) == cass_true {
                    //Gets the row at the result iterator's current position(type is memory address).
                    let row = cass_iterator_get_row(iterator);

                    //HashMap expected type is HashMap<Column, Data>
                    let mut map= HashMap::new();
                    let mut json_value = Value::Null;

                    for column_index in 0..column.len() {
                        //Get the column value at index for the specified row(type is memory address).
                        let value = cass_row_get_column(row, column_index);
                        //(type is memory address).
                        let items_iterator = cass_iterator_from_collection(value);

                        if items_iterator.is_null() {
                            //parse the memory address and generate a value based on the type(convert to json value).
                            json_value = parse_value(value);

                        } else {
                            //if the type belongs to collection(udt), parse the memory address(convert to json value).
                            json_value = json!(query_from_collection(items_iterator));

                        }
                        map.insert(column[column_index], json_value);
                    }
                    value_list.push(map);
                }
                match cass_result_has_more_pages(result) == cass_true {
                    true => {
                        cass_statement_set_paging_state(statement, result);
                    }
                    false => has_more_pages = false,
                }
                cass_iterator_free(iterator);
                cass_result_free(result);

            }
            _ => print_error(&mut *future)
        }
    }
    cass_statement_free(statement);
    (value_list, "ok".to_string())

}

unsafe fn query_from_collection(items_iterator : *mut CassIterator) -> Vec<Map<String, Value>> {

    let mut list_udt = Vec::new();

    while  cass_iterator_next(items_iterator) == cass_true {
        let items_value = cass_iterator_get_value(items_iterator);
        let udt_result = query_collection(items_value);
        list_udt.push(udt_result)
    }
    cass_iterator_free(items_iterator);
    list_udt

}

unsafe fn query_collection(items_value: *const CassValue) -> Map<String, Value> {

//    let mut map_udt = HashMap::new();
    let mut map_udt = serde_json::Map::new();

    match cass_value_type(items_value) {
        CASS_VALUE_TYPE_UDT => {
            let items_field = cass_iterator_fields_from_user_type(items_value);

            while cass_iterator_next(items_field) == cass_true {

                let mut item = mem::zeroed();
                let mut item_length = mem::zeroed();
                let items_number_value = cass_iterator_get_user_type_field_value(items_field);

                cass_iterator_get_user_type_field_name(items_field, &mut item, &mut item_length);
                let udt_name = raw2utf8(item, item_length).unwrap();
                let parse_value = parse_value(items_number_value);
                map_udt.insert(udt_name, parse_value);
            }

        }
        _ => {
            match cass_value_is_null(items_value) {
                cass_false => {
                    parse_value(items_value);
                    let parse_value = parse_value(items_value);
                },
                cass_true => error!("null"),
            }
        }
    }
    map_udt
}

unsafe fn parse_value(items_number_value : *const CassValue_) -> Value {

    match cass_value_type(items_number_value) {
        CASS_VALUE_TYPE_TEXT |
        CASS_VALUE_TYPE_ASCII |
        CASS_VALUE_TYPE_VARCHAR => {
            let mut text = mem::zeroed();
            let mut text_length = mem::zeroed();
            cass_value_get_string(items_number_value, &mut text, &mut text_length);
            let utf8_result = raw2utf8(text, text_length).unwrap();

            if utf8_result.len() == 0 {
                return Value::Null
            } else {
                return Value::String(utf8_result)
            }

        }
        CASS_VALUE_TYPE_VARINT => {
            let mut var = mem::zeroed();
            let mut var_length = mem::zeroed();

            cass_value_get_bytes(items_number_value, &mut var, &mut var_length);

            let mut slice = slice::from_raw_parts(var, var_length);
            let mut counter: i128 = 0;
            for slice_number in 0..var_length {
                let i = var_length - slice_number- 1;
                let pow = 256i128.pow(i as u32);
                let result = slice[slice_number] as i128 * pow;
                counter += result;
            }
            let val = Value::Number(counter.into());
            return val
        }
        CASS_VALUE_TYPE_BIGINT => {
            let mut b: i64 = 0;
            cass_value_get_int64(items_number_value, &mut b);
            let val = Value::Number(b.into());
            return val
        }
        CASS_VALUE_TYPE_INT => {
            let mut i: i32 = 0;
            cass_value_get_int32(items_number_value, &mut i);
            let val = Value::Number(i.into());
            return val
        }
        CASS_VALUE_TYPE_TIMESTAMP => {
            let mut t: i64 = 0;
            cass_value_get_int64(items_number_value, &mut t);
            let val = Value::Number(t.into());
            return val
        }
        CASS_VALUE_TYPE_BOOLEAN => {
            let mut b: cass_bool_t = mem::zeroed();
            cass_value_get_bool(items_number_value, &mut b);
            match b {
                cass_true => return Value::Bool(true),
                cass_false => return Value::Bool(false),
            }
        }
        CASS_VALUE_TYPE_UUID | CASS_VALUE_TYPE_TIMEUUID => {
            let mut u: CassUuid = mem::zeroed();
            cass_value_get_uuid(items_number_value, &mut u);
            let mut buf: Vec<c_char> = Vec::with_capacity(CASS_UUID_STRING_LENGTH);
            cass_uuid_string(u, buf.as_mut_ptr());
            let new_buf: String = CStr::from_ptr(buf.as_mut_ptr()).to_str().unwrap().into();
            return Value::String(new_buf)
        }
        CASS_VALUE_TYPE_INET => {
            let mut inet: CassInet = mem::zeroed();
            cass_value_get_inet(items_number_value, &mut inet);
            let mut buf: Vec<c_char> = Vec::with_capacity(CASS_INET_STRING_LENGTH);
            cass_inet_string(inet, buf.as_mut_ptr());
            let new_buf: String = CStr::from_ptr(buf.as_mut_ptr()).to_str().unwrap().into();
            return Value::String(new_buf)
        }
        CASS_VALUE_TYPE_LIST => {
            let mut list = Vec::new();
            let items_field = cass_iterator_from_collection(items_number_value);
            while cass_iterator_next(items_field) == cass_true {

                let items_number_value = cass_iterator_get_value(items_field);
                let parse_value = parse_value(items_number_value);
                list.push(parse_value);
            }
            cass_iterator_free(items_field);
            return Value::Array(list)
        }
        _ => return Value::Null
    }
}

//This function handles time-related status messages
fn time_status(co_primary_key: &str) -> (HashMap<&str, Value>, bool) {

    let mut status_link = Vec::new();

    let mut status_map = HashMap::new();

    let (start_time, end_time, status) = split_once(co_primary_key);

    if status.contains("time range should be integer!" ) {
        error!("{:?}", status);
        status_map.insert("msg", Value::String("time range should be integer!".to_string()));
        status_map.insert("result", Value::String("failed".to_string()));
        status_map.insert("data", Value::Array(status_link));
        return (status_map, false)
    } else if status.contains("Wrong time period, over one years") {
        let start_period = start_time.parse::<i64>().unwrap();
        let end_period = end_time.parse::<i64>().unwrap();
        let status_format = format!("Wrong time period, over one years - start: {:?}, end: {:?}", start_period, end_period);
        error!("{:?}", status);
        status_map.insert("msg", Value::String(status_format));
        status_map.insert("result", Value::String("failed".to_string()));
        status_map.insert("data", Value::Array(status_link));
        return (status_map, false)
    } else if status.contains("Wrong time period") {
        let start_period = start_time.parse::<i64>().unwrap();
        let end_period = end_time.parse::<i64>().unwrap();
        let status_format = format!("Wrong time period - start: {:?}, end: {:?}", start_period, end_period);
        error!("{:?}", status_format);
        status_map.insert("msg", Value::String(status_format));
        status_map.insert("result", Value::String("failed".to_string()));
        status_map.insert("data", Value::Array(status_link));
        return (status_map, false)
    }

    (status_map, true)
}

//Change the feature name obtained by url to the table name of cassandra
fn feature_to_table(feature_name: &str) -> String{
    //Use uuid as the primary key table
    let mut converted_with_uuid_table = vec!["periodical_application_usage_per_ip", "sta_mgnt", "log_mgnt",
                                             "periodical_cf-statistics_per_ip", "periodical_anti_virus_statistics_per_ip"];

    let mut check_bool = false;
    //Confirm that the query table conforms to the converted with date table.
    //If it matches, return the boolean value true.
    for table in converted_with_uuid_table.iter() {

        if &feature_name == table {
            check_bool = true;
        }
    }

    if check_bool == true {
        //Change the feature name obtained by url to the table name of cassandra
        let table_name = "t_".to_owned() + feature_name + "_date";
        return  table_name

    } else  {
        let table_name = "t_".to_owned() + feature_name ;
        return  table_name
    }
}

//Cut the received epoch range
fn split_once(in_string: &str) -> (String, String, &str) {

    let check = in_string.contains("-");

    if check {
        let mut splitter = in_string.splitn(2, '-');
        let first = splitter.next().unwrap();
        let second = splitter.next().unwrap();

        let first_bool = first.parse::<i64>().is_ok();
        let second_bool = second.parse::<i64>().is_ok();

        if first_bool == false || second_bool == false {
            return ("false".to_string(), "false".to_string(), "time range should be integer!")
        }

        let (period_check, status) = _time_range_check(&first, &second);

        if period_check == true {
            let s_epoch = _epoch_alignment(first);
            let e_epoch = _epoch_alignment(second);
            return (s_epoch, e_epoch, "Ok")

        } else if status.contains("time range should be integer!" ) {
            return (first.to_string(), second.to_string(), "time range should be integer!")

        } else if  status.contains("Wrong time period, over one years" ) {
            return (first.to_string(), second.to_string(), "Wrong time period, over one years")

        } else {
            return (first.to_string(), second.to_string(), "Wrong time period")
        }
    } else {
        error!("Wrong time period");
        return (in_string.to_string(), "Wrong".to_string(), "time period")
    }

    fn _time_range_check(start_time: &str, end_time: &str)  -> (bool, String){

        let s_epoch = _epoch_alignment(start_time);
        let e_epoch = _epoch_alignment(end_time);

        let start_period = s_epoch.parse::<i64>().unwrap();
        let end_period = e_epoch.parse::<i64>().unwrap();

        if start_period - end_period >= 0 {

            return (false, format!("Wrong time period - start: {:?}, end: {:?}", start_period, end_period))
        } else if end_period - start_period > (86400*366*1000) {

            return (false, format!("Wrong time period, over one years - start: {:?}, end: {:?}", start_period, end_period))
        }  else {

            return (true, "Ok".to_string())
        }

    }

    fn _epoch_alignment(epoch: &str) -> String {

        let epoch_len = epoch.to_string().len();

        match epoch_len {
            10 => {
                let epoch_process = epoch.to_owned() + "000";
                return epoch_process
            }
            13 => return epoch.to_string(),

            _ => return epoch.to_string()
        }

    }
}
//Calculate the time interval of a table with date (more than one day)
fn get_time_slices(start_time: i64, end_time: i64) -> (Vec<String>, Vec<String>, Vec<String>) {
    let start_day = start_time / 1000;
    let s_naive_datetime = NaiveDateTime::from_timestamp(start_day, 0);
    let start_datetime: DateTime<Utc> = DateTime::from_utc(s_naive_datetime, Utc);
    let str_start_day = start_datetime.format("%Y-%m-%d").to_string();

    let end_day = end_time / 1000;
    let e_naive_datetime = NaiveDateTime::from_timestamp(end_day, 0);
    let end_datetime: DateTime<Utc> = DateTime::from_utc(e_naive_datetime, Utc);
    let str_end_day = end_datetime.format("%Y-%m-%d").to_string();

    if str_start_day != str_end_day {
        let days = (end_time - start_time) / (86400 * 1000);
        let number_of_days = round::ceil(days as f64, 0) + 1.0;

        let mut time_range_start = "".to_string();
        let mut time_range_end = "".to_string();

        let mut start_vec = Vec::new();
        let mut end_vec = Vec::new();
        let mut day_vec = Vec::new();

        for i in 0..number_of_days as i64 {
            //not format
            let calendar_day :DateTime<Utc> = start_datetime + Duration::days(i);
            day_vec.push(calendar_day.format("%Y-%m-%d").to_string());

            if i == 0 {

                time_range_start = start_time.to_string();
                start_vec.push(time_range_start);
            } else {

                let timestamp = calendar_day.with_hour(0).unwrap().with_minute(0).unwrap().with_second(0).unwrap();
                let str_timestamp = timestamp.timestamp_millis().to_string();
                time_range_start = str_timestamp;
                start_vec.push(time_range_start);
            }

            if i == number_of_days as i64 - 1 {
                time_range_end = end_time.to_string();
                end_vec.push(time_range_end);
            } else {
                let timestamp = calendar_day.with_hour(23).unwrap().with_minute(59).unwrap().with_second(59).unwrap();
                let str_end_timestamp = timestamp.timestamp_millis();
                time_range_end = str_end_timestamp.to_string();
                end_vec.push(time_range_end);

                if str_end_timestamp > end_time {
                    time_range_end = str_end_timestamp.to_string();
                    println!("end = {:?}", time_range_end);
                    end_vec.push(time_range_end);
                    break
                }
            }

        }

        return (day_vec, start_vec, end_vec);
    } else {
        let mut start_vec = Vec::new();
        let mut end_vec = Vec::new();
        let mut day_vec = Vec::new();
        start_vec.push(start_time.to_string());
        end_vec.push(end_time.to_string());
        day_vec.push(str_start_day);

        return (day_vec, start_vec, end_vec);
    }
}

//The current function is to determine the query table, enter the primary key is a string or uuid.
fn table_check(feature_name: &str, primary_key: &str, co_primary_key: &str, start_day: &str, end_day: &str) -> String {

    //Use uuid as the primary key table
    let mut converted_with_uuid_table = vec!["periodical_application_usage_per_ip", "sta_mgnt", "log_mgnt",
                                             "periodical_cf_statistics_per_ip", "periodical_anti_virus_statistics_per_ip"];

    let mut check_bool = false;
    //Confirm that the query table conforms to the converted with date table.
    //If it matches, return the boolean value true.
    for table in converted_with_uuid_table.iter() {

        if &feature_name == table {
            check_bool = true;
        }
    }

    //Change the feature name obtained by url to the table name of cassandra
    let table_name = feature_to_table(feature_name);

    //If check_bool is true, it means the primary key is uuid
    if check_bool == true {
        //primary key is uuid
        return format!("SELECT * FROM example.{table_name} WHERE deviceid = {primary_key} AND date = '{co_primary_key}' \
        AND epoch >= {start_day} AND epoch <= {end_day}",
                       table_name=table_name, primary_key=primary_key, co_primary_key=co_primary_key, start_day=start_day, end_day=end_day);

    } else {
        //primary key is text
        return format!("SELECT * FROM example.{table_name} WHERE deviceid = '{primary_key}' AND epoch >= {start_day} AND epoch <= {end_day}",
                       table_name=table_name , primary_key=primary_key, start_day=start_day, end_day=end_day);
    }

}

fn feature_replace(feature_name: &str) -> String{

    if feature_name.contains("-") {
        let feature = feature_name.replace("-", "_");
        return feature.to_string()
    } else if feature_name.contains("_") {
        let feature = feature_name.replace("_", "-");
        return feature.to_string()
    } else {
        let feature = feature_name.to_lowercase();
        return feature.to_string()
    }
}

unsafe fn data_result(primary_key: &str, co_primary_key: &str, feature_name: &str) -> HashMap<String, Value>{
    let cluster = create_cluster();
    let session = &mut *cass_session_new();

    match connect_session(session, cluster) {
        Ok(_) => {}
        _ => {
            cass_cluster_free(cluster);
            cass_session_free(session);
            panic!();
        }
    }

    execute_query(session, "USE example").unwrap();
    let data= cassandra_use(session, primary_key, co_primary_key, feature_name);

    let close_future = cass_session_close(session);
    cass_future_wait(close_future);
    cass_future_free(close_future);

    cass_cluster_free(cluster);
    cass_session_free(session);
    data
}

fn async_handler(url: Path<Url>) -> HttpResponse {

    unsafe {

        let data= data_result(url.deviceid.as_str(), url.epoch.as_str(), url.feature.as_str());

        if data.get("msg").unwrap() == "unknown feature" ||
            data.get("msg").unwrap() == "Wrong time period, over one years" ||
            data.get("msg").unwrap() == "Wrong time period -" ||
            data.get("msg").unwrap() == "time range should be integer!" {

            HttpResponse::BadRequest()
                .content_type("text/html")
                .content_encoding(ContentEncoding::Gzip)
                .json(data)
        } else {
            HttpResponse::Ok()
                .content_type("text/html")
                .content_encoding(ContentEncoding::Gzip)
                .json(data)
        }
    }

}

fn main() {

    server::HttpServer::new(|| {
        App::new()
            .prefix("/data/1/")
            .default_encoding(ContentEncoding::Gzip)
            // async handler
            .resource("{deviceid}/{epoch}/{feature}", |r| r.method(Method::GET).with(async_handler))
    })
        .workers(8)
        .bind("0.0.0.0:3009")
        .unwrap()
        .run();

}
