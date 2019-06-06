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

pub fn column_schema(session: &Session, condition: &'static str, table_name: &str) -> Vec<&'static str> {
    let mut list = Vec::new();
    let schema_meta = session.get_schema_meta();
    let mut keyspace_meta = schema_meta.get_keyspace_by_name("nebula_device_data");
    let table_meta = keyspace_meta.table_by_name(table_name).unwrap();
    let column_iter = table_meta.columns_iter();

    for iter in column_iter {
        list.push(iter.name().as_str());
    }

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
    list
}
