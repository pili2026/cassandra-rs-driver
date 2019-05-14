#![allow(dead_code)]
use cassandra_cpp_sys::*;
use std::mem;
use std::ffi::CString;
use std::slice;
use std::str;
use std::str::Utf8Error;


//Search all table names in the keyspace and compare them
pub unsafe fn table_schema(session: &mut CassSession, condition: &str, keyspace: &str ,table_name: &str) -> bool {
    let mut list = Vec::new();
    let schema_meta = cass_session_get_schema_meta(session);
    let keyspace_meta = cass_schema_meta_keyspace_by_name(schema_meta, CString::new(keyspace).unwrap().as_ptr());
    let iter = cass_iterator_tables_from_keyspace_meta(keyspace_meta);

    while cass_iterator_next(iter) == cass_true {
        let table_meta = cass_iterator_get_table_meta(iter);
        let mut name = mem::zeroed();
        let mut name_length = mem::zeroed();
        cass_table_meta_name(table_meta, &mut name, &mut name_length);
        let all_table = raw2utf8(name, name_length).unwrap();

        list.push(all_table);
    }
    cass_iterator_free(iter);
    cass_schema_meta_free(schema_meta);

    //Confirm that the query table conforms to the converted with date table.
    //If it matches, return the boolean value true.
    for table in list.iter() {
        if &table_name == table {
            return true;
        }
    }
    false
}

//query column return ,type is Vec
pub unsafe fn column_schema(session: &mut CassSession, condition: &'static str, keyspace: &str ,table_name: &str) ->Vec<&'static str> {
    let mut list: Vec<&str> = Vec::new();
    let schema_meta = cass_session_get_schema_meta(session);
    let keyspace_meta = cass_schema_meta_keyspace_by_name(schema_meta, CString::new(keyspace).unwrap().as_ptr());
    let table_meta = cass_keyspace_meta_table_by_name(keyspace_meta, CString::new(table_name).unwrap().as_ptr());
    let iterator = cass_iterator_columns_from_table_meta(table_meta);

    while cass_iterator_next(iterator) == cass_true {
        let column_meta = cass_iterator_get_column_meta(iterator);

        let mut name = mem::zeroed();
        let mut name_length = mem::zeroed();

        cass_column_meta_name(column_meta, &mut name, &mut name_length);

        let meta_name = _raw2utf8(name, name_length).unwrap();

        list.push(meta_name);
    }
    cass_iterator_free(iterator);
    cass_schema_meta_free(schema_meta);

    let mut check_bool = false;
    let mut condition_vec = Vec::new();

    for specified_condition in list.iter() {

        if specified_condition == &condition {
            check_bool = true;
            warn!("{:?}", check_bool);
            condition_vec.push(condition);
        }
    }

    if check_bool {
        return condition_vec
    }

    pub unsafe fn _raw2utf8(data: *const i8, length: usize) -> Result<&'static str, Utf8Error> {
        let slice = slice::from_raw_parts(data as *const u8, length as usize);
        str::from_utf8(slice).to_owned()
    }

    list
}
