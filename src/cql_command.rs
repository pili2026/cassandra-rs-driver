use serde_json::Value as _Value;
use crate::function_util::split_once;


pub fn get_connectivity_before(dev: &str, epoch: &str, cnt: &str) -> String{
    return format!("select blobAsBigint(timestampAsBlob(epoch)) as epoch, outter_ipv4 from t_helper_connectivity where deviceid='{dev}' and entry_type='{cnt}' \
        and epoch < {epoch} order by epoch desc limit 1", dev=dev, epoch=epoch, cnt=cnt);

}

pub fn get_cql_string(fields: String, dev: &str, cnt: &str) -> String {
    return format!("select {fields} from t_helper_connectivity where deviceid='{dev}' and entry_type='{cnt}' \
                order by epoch desc limit 1", fields=fields ,dev=dev, cnt=cnt)
}

pub fn get_last_connect_cql(dev: &str) -> String {
    let fields = vec!["blobAsBigint(timestampAsBlob(epoch)) as epoch", "outter_ipv4"];
    let cnt = "connect";
    let cql_connect = get_cql_string(fields.join(","), dev, cnt);
    cql_connect
}

pub fn get_last_disconnect_cql(dev: &str) -> String {
    let fields = "blobAsBigint(timestampAsBlob(epoch)) as epoch".to_string();
    let cnt = "disconnect";
    let cql_disconnect = get_cql_string(fields, dev, cnt);
    return cql_disconnect
}

pub fn get_first_connect_cql(dev: &str) -> String {
    let cql_first_connect = format!("select blobAsBigint(timestampAsBlob(epoch)) as epoch, outter_ipv4 \
                from t_helper_connectivity where deviceid='{dev}' and entry_type='connect' \
                order by epoch asc limit 1;", dev=dev);
    return cql_first_connect
}

pub fn _get_connectivity_period_cql(dev: &str, period: &str) -> String{
    let (start, end, status) = split_once(period);
    let fields = "deviceid, blobasbigint(timestampasblob(epoch)) as epoch, entry_type, outter_ipv4";
    let cql_connectivity_period = format!("select {fields} from t_helper_connectivity where deviceid='{dev}' \
                    and entry_type in ('connect', 'disconnect') and epoch>={start} and epoch<{end}", fields=fields, dev=dev, start=start, end=end);

    return cql_connectivity_period
}


pub fn _is_connected(cnt_t: _Value, discnt_t: _Value) -> bool{
    let mut connectivity = true;
    if cnt_t.is_null() {
        connectivity = false
    } else if cnt_t.is_null() == false && discnt_t.is_null() == false {
        let cnt_time = serde_json::to_string(&cnt_t).unwrap();
        let discnt_time = serde_json::to_string(&discnt_t).unwrap();
        let cnt_i = cnt_time.parse::<i64>().unwrap();
        let discnt_i = discnt_time.parse::<i64>().unwrap();
        if cnt_i < discnt_i {
            connectivity = false
        }
    }
    connectivity
}
