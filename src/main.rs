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

#[macro_use] extern crate log;
extern crate simplelog;

mod schema_util;
use crate::schema_util::*;
use cassandra_cpp::*;
use futures::Future;
use std::fmt;
use chrono::prelude::*;
use math::round;
use time::Duration;
use serde::*;
use serde::{Deserialize, Serialize};
use serde_json::{Value as _Value, Map, Number, to_value, json};
use actix_web::{
    error, http, middleware, server, App, AsyncResponder, Error, HttpMessage,
    HttpRequest, HttpResponse, Json, Responder,Path,
    Result as _Result,
};
use std::collections::HashMap;
use actix_web::http::{ContentEncoding, Method};

#[derive(Deserialize)]
struct Url {
    deviceid: String,
    epoch: String,
    feature: String,
}

#[derive(Debug, Clone, Serialize)]
struct Status {
    msg: Msg,
    result: Msg,
    data: _Value,
}

#[derive(Debug, Clone, Copy,
PartialEq, Eq,)]
enum Msg {
    Failed,
    Ok,
    UnknownFeature,
    WrongTimePeriod(Option<WrongTimePeriodReason>),
    TimeRangeShouldBeInteger,
    InvalidTimeRange,
}
#[derive(Debug, Clone, Copy,
PartialEq, Eq,)]
enum WrongTimePeriodReason {
    OverOneYear,
}

#[derive(Debug)]
enum CassandraError {
    UnknownFeature,
}

impl Serialize for Msg {
    fn serialize<S>(&self, serializer: S) -> _Result<S::Ok, S::Error>
        where
            S: Serializer,
    {
        use self::Msg::*;
        use self::WrongTimePeriodReason::*;
        serializer.serialize_str(match *self {
            | Failed => "failed",
            | Ok => "ok",
            | UnknownFeature => "unknown feature",
            | WrongTimePeriod(Some(OverOneYear)) => "Wrong time period, over one years",
            | WrongTimePeriod(None) => "Wrong time period -",
            | TimeRangeShouldBeInteger => "time range should be integer!",
            | InvalidTimeRange => "invalid time range",
        })
    }
}

impl fmt::Display for CassandraError {
    fn fmt (&self, f: &mut fmt::Formatter)-> fmt::Result {
        write!(f, "unknown feature")
    }
}

const PAGE_SIZE: i32 = 100;

lazy_static! {
    static ref SESSION: Session = {
        Cluster::default()
            .set_contact_points("127.0.0.1")
                .expect("Failed to set_contact_points")
            .set_port(9042)
                .expect("Failed to set_port")
            .set_num_threads_io(10)
                .expect("Failed to set_num_threads_io")
            .connect()
                .expect("Failed to connect to the cluster")
    };
}

fn cassandra_use(session: &Session, primary_key: &str, co_primary_key: &str, feature_name: &str) -> Status {
    let feature = feature_replace(feature_name);
    let mut result_link = Vec::new();

    // If status_bool is true, status_result is null
    let (status_result, status_bool) = time_status(co_primary_key);

    if status_bool == false {
        return status_result
    }

    let (start, end, status) = split_once(co_primary_key);

    let start_timestamp = start.parse::<i64>().unwrap();
    let end_timestamp = end.parse::<i64>().unwrap();

    //Calculate the time interval passed in and cut. Returns the vec of day, start_time, and end_time.
    let (date, start_time, end_time) = get_time_slices( start_timestamp, end_timestamp);

    for (day, (start, end)) in date.iter().zip(start_time.iter().zip(end_time)) {
        match cassandra_connection(session, primary_key, &day, &feature, &start, &end) {
            Ok(cass_data) => {
                result_link.extend(cass_data)
            },
            Err(CassandraError::UnknownFeature) => {
                let data_to_json = to_value(result_link).expect("data type was vec");
                error!("unknown feature");
                return Status {
                    msg: Msg::UnknownFeature,
                    result: Msg::Failed,
                    data: data_to_json
                }
            }
        }
    }

    match result_link.len() {
        0 => {
            let data_to_json = to_value(result_link).expect("data type was vec");
            warn!("data is null");
            return Status {
                msg: Msg::Ok,
                result: Msg::Ok,
                data: data_to_json,
            };
        }
        _ => {
            let data_to_json = to_value(result_link).expect("data type was vec");
            return Status {
                msg: Msg::Ok,
                result: Msg::Ok,
                data: data_to_json,
            };
        }
    }

}

fn cassandra_connection(session: &Session, primary_key: &str, co_primary_key: &str, feature_name: &str, start_day: &str, end_day: &str)
                        ->  _Result<Vec<HashMap<&'static str, _Value>>, CassandraError,> {
    let mut has_more_pages = true;
    let table_name = feature_to_table(feature_name);
    session.execute(&stmt!("USE nebula_device_data")).wait().unwrap();
    let table_bool = table_schema(session, &table_name);
    let mut value_list = Vec::new();

    if table_bool == false {
        return Err(CassandraError::UnknownFeature)
    }

    let query_check = table_check(feature_name, primary_key, co_primary_key, start_day, end_day);
    let mut statement = stmt!(query_check.as_str());

    statement.set_paging_size(PAGE_SIZE).unwrap();

    while has_more_pages {
        let result = session.execute(&statement).wait().unwrap();
        let mut column = column_schema(session,"*",&table_name);

        for row in result.iter() {
            let mut map = HashMap::new();

            for column_index in 0..column.len() {

                let mut json_value = _Value::Null;
                let column_value = row.get_column(column_index as usize).unwrap();
                if column_value.get_set().is_ok() {
                    let items_iterator = column_value.get_set().unwrap();
                    let mut list_udt = Vec::new();

                    for items in items_iterator {

                        match items.get_type() {
                            ValueType::UDT => {
                                list_udt.push(query_collection(items));
                                json_value = json!(list_udt);
                            }
                            _ => {
                                match items.is_null() {
                                    false =>{
                                        parse_value(items);
                                    }
                                    true => error!("error")
                                }
                            }
                        }
                    }
                    //Not collections type
                } else {
                    json_value = parse_value(column_value);
                }
                let index = column_index as usize;
                map.insert(column[index].clone(),json_value);
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

fn query_collection(value: Value) -> Map<String, _Value> {
    let mut map_udt = Map::new();
    match value.get_type() {
        ValueType::UDT => {
            let mut udt_iterator = value.get_user_type().unwrap();

            for (udt_column, udt_value) in udt_iterator {
                let value = parse_value(udt_value);
                map_udt.insert(udt_column, value);
            }
        }
        _ => {
            if value.is_null() {
                let parse_value = parse_value(value);
            } else {
                error!("null")
            }
        }
    }
    map_udt
}

fn parse_value (value: Value) -> _Value {

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
                let valint = _Value::Number(counter.into());
                valint

            }
            ValueType::INT => {
                _Value::Number(value.get_i32().unwrap().into())
            }
            ValueType::DOUBLE|
            ValueType::FLOAT => {
                //'''Value::Number''' does not support floating point conversion
                let f = value.get_f64().unwrap();
                json!(f)
            }
            ValueType::TIMESTAMP |
            ValueType::BIGINT => {

                _Value::Number(value.get_i64().unwrap().into())
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
                _Value::String(i.to_string())
            }
            ValueType::LIST|
            ValueType::SET => {
                let mut list = Vec::new();
                let items_field = value.get_set().unwrap();
                for item in items_field {
                    let parse_value = parse_value(item);
                    list.push(parse_value);
                }
                _Value::Array(list)
            }
            _ => {
                error!("parse_value error");
                _Value::Null
            }
        }
    }

}

//This function handles time-related status messages
fn time_status(co_primary_key: &str) -> (Status, bool) {

    let (start_time, end_time, status_msg) = split_once(co_primary_key);
    let data: Vec<_Value> = Vec::new();

    if status_msg.contains("time range should be integer!" ) {
        let status = Status {
            msg: Msg::TimeRangeShouldBeInteger,
            result: Msg::Failed,
            data: _Value::Array(data)
        };
        error!("{:?}", status);
        return (status, false);

    } else if status_msg.contains("Wrong time period, over one years") {
        let status_format = format!("Wrong time period, over one years - start: {:?}, end: {:?}", start_time, end_time);
        error!("{:?}", status_format);
        use crate::WrongTimePeriodReason::OverOneYear;
        let status = Status {
            msg: Msg::WrongTimePeriod(Some(OverOneYear)),
            result: Msg::Failed,
            data: _Value::Array(data)
        };

        return (status, false);
    } else if status_msg.contains("Wrong time period") {
        let status_format = format!("Wrong time period - start: {:?}, end: {:?}", start_time, end_time);
        error!("{:?}", status_format);
        let status = Status {
            msg: Msg::WrongTimePeriod(None),
            result: Msg::Failed,
            data: _Value::Array(data)
        };
        return (status, false);
    } else if status_msg.contains("invalid time range") {
        error!("{:?}", status_msg);
        let status = Status {
            msg: Msg::InvalidTimeRange,
            result: Msg::Failed,
            data: _Value::Array(data)
        };
        return (status, false);
    }

    let status = Status {
        msg: Msg::Ok,
        result: Msg::Ok,
        data: _Value::Array(data)
    };

    (status, true)
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

        } else if  status.contains("Wrong time period, over one years" ) {
            return (first.to_string(), second.to_string(), "Wrong time period, over one years")

        } else {
            return (first.to_string(), second.to_string(), "Wrong time period")
        }
    } else {
        error!("invalid time range");
        return (in_string.to_string(), "Wrong".to_string(), "invalid time range")
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
        return format!("SELECT * FROM nebula_device_data.{table_name} WHERE deviceid = {primary_key} AND date = '{co_primary_key}' \
        AND epoch >= {start_day} AND epoch <= {end_day}",
                       table_name=table_name, primary_key=primary_key, co_primary_key=co_primary_key, start_day=start_day, end_day=end_day);

    } else {
        //primary key is text
        return format!("SELECT * FROM nebula_device_data.{table_name} WHERE deviceid = '{primary_key}' AND epoch >= {start_day} AND epoch <= {end_day}",
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

fn query_handler(url: Path<Url>) -> HttpResponse {

    let data =  cassandra_use(&*SESSION, &url.deviceid, &url.epoch, &url.feature);

    let mut response_builder = match data.msg {
        | Msg::Ok => HttpResponse::Ok(),
        | _ => HttpResponse::BadRequest(),
    };
    response_builder
        .content_type("text/html")
        .content_encoding(ContentEncoding::Gzip)
        .json(data)
}

fn main() {
    server::HttpServer::new(|| {
        App::new()
            .prefix("/data/1/")
            .default_encoding(ContentEncoding::Gzip)
            // async handler
            .resource("{deviceid}/{epoch}/{feature}", |r| r.method(Method::GET).with(query_handler))
    })
        .workers(4)
        .bind("0.0.0.0:3009")
        .unwrap()
        .run();

}
