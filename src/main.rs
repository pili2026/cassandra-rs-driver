#![allow(unused)]
extern crate cassandra_cpp_sys;
extern crate iron;
extern crate rand;
extern crate router;

#[macro_use]
extern crate log;
extern crate simplelog;

#[macro_use]
extern crate serde_derive;
extern crate serde;
extern crate serde_json;

mod examples_util;
use crate::examples_util::*;

use std::ffi::CString;
use std::mem;
use cassandra_cpp_sys::*;

use std::collections::LinkedList;
use std::str;
use std::slice;
use simplelog::*;
use std::fs::File;
use std::io::Read;

use iron::prelude::*;
use iron::status;
use iron::mime::Mime;
use rand::Rng;
use router::Router;
use serde::*;
use serde::{Deserialize, Serialize};
use serde_json::{Result, Value};
use std::str::Utf8Error;

const CASS_UUID_STRING_LENGTH: usize = 37;

fn select_condition(session: &mut CassSession, condition: &str, keyspace: &str ,table_name: &str, primary_key: &'static str, column_value: &str) -> String {

    let mut dl = LinkedList::new();

    unsafe {
        let query = format!("SELECT {condition} FROM {keyspace}.{table_name} WHERE {primary_key} = ?;",
                            condition=condition, keyspace=keyspace, table_name=table_name , primary_key=primary_key);
        let statement = cass_statement_new(CString::new(query).unwrap().as_ptr(), 1);
        let mut column = schema_meta(session, keyspace, table_name);

//        let mut u: CassUuid = mem::zeroed();
//        let mut us: [i8; CASS_UUID_STRING_LENGTH] = mem::zeroed();
//        cass_uuid_from_string(CString::new(column_value).unwrap().as_ptr(), &mut u);
//        cass_uuid_string(u, &mut *us.as_mut_ptr());
//
//        println!("{:?}", raw2utf8(u, *us.as_mut_ptr()));

        cass_statement_bind_string(statement, 0, CString::new(column_value).unwrap().as_ptr());

        let future = &mut *cass_session_execute(session, statement);
        cass_future_wait(future);

        match cass_future_error_code(future) {
            CASS_OK => {
                let result = cass_future_get_result(future);
                let iterator = cass_iterator_from_result(result);

                if cass_iterator_next(iterator) == cass_true {
                    let row = cass_iterator_get_row(iterator);
                    let value = cass_row_get_column(row, 1);
                    let items_iterator = cass_iterator_from_collection(value);

                    if items_iterator.is_null() {
                        warn!("Single type");
                        parse_value(value);
                        let return_value = parse_value(value);
                        println!("return =>{:?}", return_value);
                        dl.push_back(return_value);
                        info!("value: {:?}", dl);
//                        return dl
                    } else {
                        warn!("Collection type");
                        let mut udt_return = select_from_collection(items_iterator);
                        info!("list udt value: {:?}", udt_return);
                        return udt_return

                    };
                    cass_iterator_free(items_iterator);
                };
                cass_iterator_free(iterator);
                cass_result_free(result);
            }
            _ => print_error(future),
        }

        cass_future_free(future);
        cass_statement_free(statement);
    }
    "Ok".to_string()
}

unsafe fn select_from_collection(items_iterator : *mut CassIterator) -> String {

    let mut udt_value = Vec::new();
    let mut udt_name_type = Vec::new();
    let mut combine = Vec::new();

    while cass_iterator_next(items_iterator) == cass_true {
        let items_value = cass_iterator_get_value(items_iterator);

        match cass_value_type(items_value) {
            CASS_VALUE_TYPE_UDT => {
                let items_field = cass_iterator_fields_from_user_type(items_value);
                while cass_iterator_next(items_field) == cass_true{

                    let mut item = mem::zeroed();
                    let mut item_length = mem::zeroed();
                    let items_number_value = cass_iterator_get_user_type_field_value(items_field);

                    cass_iterator_get_user_type_field_name(items_field, &mut item, &mut item_length);
                    let udt_name = raw2utf8(item, item_length).unwrap();
                    warn!("UDT Name: {:?}", udt_name);
                    let return_value = parse_value(items_number_value);
                    println!("return =>{:?}", return_value);
                    udt_name_type.push(udt_name);
                    udt_value.push(return_value);

                    println!("udt_name_type => {:?}", udt_name_type);
                }

                for i in 0..udt_name_type.len() {
                    let tem = format!("{:?}:{:?}", udt_name_type[i], udt_value[i]);
                    combine.push(tem);
                }
                let combine_value =  combine.join(",");
                let combine_value_f = "{".to_string() + &combine_value+ &"}".to_string();
                println!("{:?}", combine_value_f);
                return combine_value_f

            }
            _ => {
                match cass_value_is_null(items_value) {
                    cass_false => {
                        parse_value(items_value);
                        let return_value = parse_value(items_value);
                        println!("return =>{:?}", return_value);
                        udt_value.push(return_value);
                        info!("value: {:?}", udt_value);
                    },
                    cass_true => error!("null"),
                }
            }
        }
    }

    "Ok".to_string()
}

unsafe fn schema_meta(session: &mut CassSession, keyspace: &str ,table_name: &str) ->Vec<String> {
    let mut list = Vec::new();
    let schema_meta = cass_session_get_schema_meta(session);
    let keyspace_meta = cass_schema_meta_keyspace_by_name(schema_meta, CString::new(keyspace).unwrap().as_ptr());
    let table_meta = cass_keyspace_meta_table_by_name(keyspace_meta, CString::new(table_name).unwrap().as_ptr());
    let iterator = cass_iterator_columns_from_table_meta(table_meta);

    while cass_iterator_next(iterator) == cass_true {
        let column_meta = cass_iterator_get_column_meta(iterator);

        let mut name = mem::zeroed();
        let mut name_length = mem::zeroed();

        cass_column_meta_name(column_meta, &mut name, &mut name_length);
        let meta_name = raw2utf8(name, name_length).unwrap();
        info!("Column \"{}\":", meta_name);
        list.push(meta_name);
    }

    cass_iterator_free(iterator);
    list
}

unsafe fn parse_value(items_number_value : *const CassValue_) -> String{

    match cass_value_is_null(items_number_value) {
        cass_false => {
            match cass_value_type(items_number_value) {
                CASS_VALUE_TYPE_TEXT |
                CASS_VALUE_TYPE_ASCII |
                CASS_VALUE_TYPE_VARCHAR => {
                    let mut text = mem::zeroed();
                    let mut text_length = mem::zeroed();
                    cass_value_get_string(items_number_value, &mut text, &mut text_length);
                    debug!("Value Type => {:?}", cass_value_type(items_number_value));
                    let utf8_result = raw2utf8(text, text_length).unwrap();
                    info!("\"{:?}\" ", utf8_result);

                    return utf8_result
                }
                CASS_VALUE_TYPE_VARINT => {
                    let mut var = mem::zeroed();
                    let mut var_length = mem::zeroed();

                    cass_value_get_bytes(items_number_value, &mut var, &mut var_length);

                    let mut slice = slice::from_raw_parts(var, var_length);
                    debug!("Value Type => {:?}", cass_value_type(items_number_value));

                    let mut counter: u128 = 0;
                    for slice_number in 0..var_length {
                        let i = var_length - slice_number- 1;
                        let hex_pow = 256u128.pow(i as u32);
                        let result = slice[slice_number] as u128 * hex_pow;
                        counter += result;
                    }
                    info!("{:?}", counter);

                    let counter_str = format!("{}", counter);
                    return counter_str
                }
                CASS_VALUE_TYPE_BIGINT => {
                    let mut i = mem::zeroed();
                    cass_value_get_int64(items_number_value, &mut i);
                    debug!("Value Type => {:?}", cass_value_type(items_number_value));
                    info!("{:?} ", i);

                    let bigint_str = format!("{}", i);
                    return bigint_str
                }
                CASS_VALUE_TYPE_INT => {
                    let mut i = mem::zeroed();
                    cass_value_get_int32(items_number_value, &mut i);
                    debug!("Value Type => {:?}", cass_value_type(items_number_value));
                    info!("{:?}", i);

                    let int_str = format!("{}", i);
                    return int_str
                }
                CASS_VALUE_TYPE_TIMESTAMP => {
                    let mut t = mem::zeroed();
                    cass_value_get_int64(items_number_value, &mut t);
                    debug!("Value Type => {:?}", cass_value_type(items_number_value));
                    info!("{:?}", t);

                    let time_str = format!("{}", t);
                    return time_str
                }
                CASS_VALUE_TYPE_BOOLEAN => {
                    let mut b: cass_bool_t = mem::zeroed();
                    cass_value_get_bool(items_number_value, &mut b);
                    debug!("Value Type => {:?}", cass_value_type(items_number_value));
                    info!("{:?}",
                          match b {
                              cass_true => return "true".to_string(),
                              cass_false => return "false".to_string(),
                          });

                }
                CASS_VALUE_TYPE_UUID => {
                    let mut u: CassUuid = mem::zeroed();
                    let mut us: [i8; CASS_UUID_STRING_LENGTH] = mem::zeroed();
                    cass_value_get_uuid(items_number_value, &mut u);

                    cass_uuid_string(u, &mut *us.as_mut_ptr());


                    debug!("Value Type => {:?}", cass_value_type(items_number_value));
                    info!("{:?}", "us - FIXME" /* us */);
                    println!("{:?}", *us.as_mut_ptr());//
                }
                CASS_VALUE_TYPE_INET => {
                    let mut inet = mem::zeroed();
                    debug!("Value Type => {:?}", cass_value_type(items_number_value));
                    cass_value_get_inet(items_number_value, &mut inet);
                    let mut vec = inet.address.to_vec();
                    vec.truncate(inet.address_length as usize);

                    let mut vec_str= Vec::new();
                    for num in vec {
                        let num_str = format!("{:?}", num);
                        vec_str.push(num_str);

                    }
                    let ipaddr = vec_str.join(".");
                    info!("{:?}", ipaddr);

                    return ipaddr

                }
                _ => error!("No match type: {:?}", cass_value_type(items_number_value))
            };
        }
        cass_true => error!("<null>"),
    }
    return "OK".to_string()
}

unsafe fn cass_connect() -> LinkedList<String> {
    let cluster = create_cluster();
    let session = &mut *cass_session_new();

    let uuid_gen = &mut *cass_uuid_gen_new();

    match connect_session(session, cluster) {
        Ok(_) => {}
        _ => {
            cass_cluster_free(cluster);
            cass_session_free(session);
            panic!();
        }
    }

    let mut cass_result = LinkedList::new();
    let cass_query = select_condition(session, "*", "examples", "new_user", "deviceid", "Kobe");
    cass_result.push_back(cass_query);

    let close_future = cass_session_close(session);
    cass_future_wait(close_future);
    cass_future_free(close_future);

    cass_cluster_free(cluster);
    cass_session_free(session);

    cass_uuid_gen_free(uuid_gen);
    cass_result
}

fn main() {

    CombinedLogger::init(
        vec![
            TermLogger::new(LevelFilter::Debug, Config::default()).unwrap(),
            WriteLogger::new(LevelFilter::Trace, Config::default(), File::create("my_rust_binary.log").unwrap()),
        ]
    ).unwrap();

    let mut cass = unsafe{cass_connect()};

    println!("{:?}", cass);

    let mut router = Router::new();
    let localhost = "localhost:3009".to_string();

    let handler = move |req: &mut Request| {
        // convert the response struct to JSON
        let out = serde_json::to_string(&cass).unwrap();
        let content_type = "application/json".parse::<Mime>().unwrap();
        Ok(Response::with((content_type, status::Ok, out)))
    };

    router.get("/", handler, "index");

    info!("Listening on {}", localhost);
    Iron::new(router).http(localhost).unwrap();

}