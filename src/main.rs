#![allow(dead_code)]
extern crate cassandra_cpp_sys;

#[macro_use]
extern crate log;
extern crate simplelog;

extern crate bytes;
extern crate hex;

mod examples_util;
use examples_util::*;
use std::ffi::CString;
use std::mem;
use cassandra_cpp_sys::*;

use std::str;
use std::slice;
use simplelog::*;
use std::fs::File;

const CASS_UUID_STRING_LENGTH: usize = 37;
static mut VAR: u128 = 0;

fn select_condition(session: &mut CassSession, condition: &str, keyspace: &str ,table_name: &str, primary_key: &'static str, column_value: &str) -> Result<(), CassError> {
    unsafe {
        let query = format!("SELECT {condition} FROM {keyspace}.{table_name} WHERE {primary_key} = ?;",
                            condition=condition, keyspace=keyspace, table_name=table_name , primary_key=primary_key);
        let statement = cass_statement_new(CString::new(query).unwrap().as_ptr(), 1);

        cass_statement_bind_string(statement, 0, CString::new(column_value).unwrap().as_ptr());

        let future = &mut *cass_session_execute(session, statement);
        cass_future_wait(future);

        match cass_future_error_code(future) {
            CASS_OK => {
                let result = cass_future_get_result(future);
                let iterator = cass_iterator_from_result(result);

                if cass_iterator_next(iterator) == cass_true {
                    let row = cass_iterator_get_row(iterator);
                    let value = cass_row_get_column(row, 0);
                    let items_iterator = cass_iterator_from_collection(value);

                    debug!("type => {:?}", cass_value_type(value));

                    if items_iterator.is_null() {
                        warn!("Single type");
                        print_value(value);

                    } else {
                        warn!("Collection type");
                        select_from_collection(items_iterator);

                    }
                    cass_iterator_free(items_iterator);

                };
                cass_iterator_free(iterator);
                cass_result_free(result);
            }
            _ => print_error(future),
        }

        cass_future_free(future);
        cass_statement_free(statement);

        Ok(())
    }
}

fn select_from_collection(items_iterator : *mut CassIterator) {
    unsafe {
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
                        warn!("UDT Name: {:?}", raw2utf8(item, item_length));
                        print_value(items_number_value);

                    }
                }
                _ => {
                    match cass_value_is_null(items_value) {
                        cass_false => print_value(items_value),
                        cass_true => error!("null"),
                    }
                }
            }
        }
    }
}

unsafe fn print_value(items_number_value : *const CassValue_) {
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
                    info!("\"{:?}\" ", raw2utf8(text, text_length));
                }
                CASS_VALUE_TYPE_VARINT => {
                    let mut var = mem::zeroed();
                    let mut var_length = mem::zeroed();

                    cass_value_get_bytes(items_number_value, &mut var, &mut var_length);

                    let mut slice = slice::from_raw_parts(var, var_length);
                    debug!("Value Type => {:?}", cass_value_type(items_number_value));

                    VAR = 0;
                    for slice_number in 0..var_length {
                        let i = var_length - slice_number- 1;
                        let hex_pow = 256u128.pow(i as u32);
                        let result = slice[slice_number] as u128 * hex_pow;
                        VAR += result;
                    }
                    info!("{:?}", VAR);
                }
                CASS_VALUE_TYPE_BIGINT => {
                    let mut i = mem::zeroed();
                    cass_value_get_int64(items_number_value, &mut i);
                    debug!("Value Type => {:?}", cass_value_type(items_number_value));
                    info!("{:?} ", i);
                }
                CASS_VALUE_TYPE_INT => {
                    let mut i = mem::zeroed();
                    cass_value_get_int32(items_number_value, &mut i);
                    debug!("Value Type => {:?}", cass_value_type(items_number_value));
                    info!("{:?}", i);
                }
                CASS_VALUE_TYPE_TIMESTAMP => {
                    let mut t = mem::zeroed();
                    cass_value_get_int64(items_number_value, &mut t);
                    debug!("Value Type => {:?}", cass_value_type(items_number_value));
                    info!("{:?}", t);
                }
                CASS_VALUE_TYPE_BOOLEAN => {
                    let mut b: cass_bool_t = mem::zeroed();
                    cass_value_get_bool(items_number_value, &mut b);
                    debug!("Value Type => {:?}", cass_value_type(items_number_value));
                    info!("{:?}",
                          match b {
                              cass_true => "true",
                              cass_false => "false",
                          });
                }
                CASS_VALUE_TYPE_UUID => {
                    let mut u: CassUuid = mem::zeroed();
                    let mut us: [i8; CASS_UUID_STRING_LENGTH] = mem::zeroed();

                    cass_value_get_uuid(items_number_value, &mut u);
                    cass_uuid_string(u, &mut *us.as_mut_ptr());

                    debug!("Value Type => {:?}", cass_value_type(items_number_value));
                    info!("{:?}", "us - FIXME" /* us */);
                }
                CASS_VALUE_TYPE_INET => {
                    let mut inet = mem::zeroed();
                    debug!("Value Type => {:?}", cass_value_type(items_number_value));
                    cass_value_get_inet(items_number_value, &mut inet);
                    let mut vec = inet.address.to_vec();
                    vec.truncate(inet.address_length as usize);
                    info!("{:?}", vec);

                }
                _ => error!("Error"),
            }
        }
        cass_true => error!("<null>"),
    }
}

fn main() {

    CombinedLogger::init(
        vec![
            TermLogger::new(LevelFilter::Debug, Config::default()).unwrap(),
            WriteLogger::new(LevelFilter::Trace, Config::default(), File::create("my_rust_binary.log").unwrap()),
        ]
    ).unwrap();

    unsafe {
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

        select_condition(session, "title", "test_ks", "user", "first_name", "Joe").unwrap();

        let close_future = cass_session_close(session);
        cass_future_wait(close_future);
        cass_future_free(close_future);

        cass_cluster_free(cluster);
        cass_session_free(session);

        cass_uuid_gen_free(uuid_gen);

    }
}