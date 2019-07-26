use cassandra_cpp::*;

pub fn table_schema(session: &Session, table_name: &str) -> bool {
    let mut list = Vec::new();
    let schema_meta = session.get_schema_meta();
    let mut keyspace_meta = schema_meta.get_keyspace_by_name("nebula_device_data");
    let table_iter = keyspace_meta.table_iter();

    for iter in table_iter {
        list.push(iter.get_name());
    }

    for table in list.iter() {
        if &table_name == table {
            return true;
        }
    }

    false
}

pub fn column_schema(session: &Session, condition: &'static str, table_name: &str) -> Vec<String> {
    let mut list = Vec::new();
    let schema_meta = session.get_schema_meta();
    let mut keyspace_meta = schema_meta.get_keyspace_by_name("nebula_device_data");
    let table_meta = keyspace_meta.table_by_name(table_name).unwrap();
    let column_iter = table_meta.columns_iter();

    for iter in column_iter {
        list.push(iter.name());
    }

    let mut check_bool = false;
    let mut condition_vec = Vec::new();

    for specified_condition in list.iter() {

        if specified_condition == &condition {
            check_bool = true;
            warn!("column schema bool => {:?}", check_bool);
            condition_vec.push(condition.to_string());
        }
    }

    if check_bool {
        return condition_vec
    }
    list
}

pub fn column_schema_inspection(session: &Session, condition: Vec<&str>, table_name: &str) -> bool {
    let mut list = Vec::new();
    let schema_meta = session.get_schema_meta();
    let mut keyspace_meta = schema_meta.get_keyspace_by_name("nebula_device_data");
    let table_meta = keyspace_meta.table_by_name(table_name).unwrap();
    let column_iter = table_meta.columns_iter();

    for iter in column_iter {
        list.push(iter.name());
    }

    for condition_str in condition.iter() {
        let mut check_bool = false;

        let condition_low = condition_str.to_lowercase();
        for specified_condition in list.iter() {
            if &condition_low == specified_condition {
                check_bool = true
            }
        }
        if check_bool == false {
            return false
        }
    }

    true

}
