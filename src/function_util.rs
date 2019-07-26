use time::Duration;
use chrono::prelude::*;
use math::round;
use crate::status_util::*;
use serde_json::Value as _Value;

//This function handles time-related status messages
pub fn time_status(co_primary_key: &str) -> (Status, bool) {

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
pub fn feature_to_table(feature_name: &str) -> String{
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
pub fn split_once(in_string: &str) -> (String, String, &str) {

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

        if in_string == "latest" {
            return (in_string.to_string(), "Ok".to_string(), "Ok")
        } else {
            error!("invalid time range");
            return (in_string.to_string(), "Wrong".to_string(), "invalid time range")
        }

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
pub fn get_time_slices(start_time: i64, end_time: i64) -> (Vec<String>, Vec<String>, Vec<String>) {
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
//                    println!("end = {:?}", time_range_end);
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
pub fn cql_syntax(table_name: &str, dev: &str, date: &str, start_day: &str, end_day: &str) -> String {

    //Use uuid as the primary key table
    let mut converted_with_uuid_table = vec!["t_periodical_application_usage_per_ip_date", "t_sta_mgnt_date", "t_log_mgnt_date",
                                             "t_periodical_cf_statistics_per_ip_date", "t_periodical_anti_virus_statistics_per_ip_date"];

    let mut check_bool = false;
    //Confirm that the query table conforms to the converted with date table.
    //If it matches, return the boolean value true.
    for table in converted_with_uuid_table.iter() {

        if &table_name == table {
            check_bool = true;
        }
    }


    //If check_bool is true, it means the primary key is uuid
    if check_bool == true {
        //primary key is uuid
        return format!("SELECT * FROM nebula_device_data.{table_name} WHERE deviceid = {dev} AND date = '{date}' \
        AND epoch >= {start_day} AND epoch < {end_day}",
                       table_name=table_name, dev=dev, date=date, start_day=start_day, end_day=end_day);

    } else {
        //primary key is text
        return format!("SELECT * FROM nebula_device_data.{table_name} WHERE deviceid = '{dev}' AND epoch >= {start_day} AND epoch < {end_day}",
                       table_name=table_name , dev=dev, start_day=start_day, end_day=end_day);
    }

}

//The current function is to determine the query table, enter the primary key is a string or uuid.
pub fn post_cql_syntax(table_name: &str, dev: &str, date: &str, start_day: &str, end_day: &str, field: &str) -> String {

    //Use uuid as the primary key table
    let mut converted_with_uuid_table = "t_log_mgnt_date";

    let mut check_bool = false;
    //Confirm that the query table conforms to the converted with date table.
    //If it matches, return the boolean value true.
    if table_name == converted_with_uuid_table {
        check_bool = true;
    }

    //If check_bool is true, it means the primary key is uuid
    if check_bool == true {
        //primary key is uuid

        match field {
            "" => {
                return format!("SELECT * FROM nebula_device_data.{table_name} WHERE deviceid = {dev} AND date = '{date}' \
                                AND epoch >= {start_day} AND epoch < {end_day}",
                                table_name=table_name, dev=dev, date=date, start_day=start_day, end_day=end_day);
            }
            _ => {
                return format!("SELECT {field} FROM nebula_device_data.{table_name} WHERE deviceid = {dev} AND date = '{date}' \
                                AND epoch >= {start_day} AND epoch < {end_day}",
                               field=field ,table_name=table_name, dev=dev, date=date, start_day=start_day, end_day=end_day);
            }
        }

    } else {

        match field {
            "" => {
                return format!("SELECT * FROM nebula_device_data.{table_name} WHERE deviceid = '{dev}' AND epoch >= {start_day} AND epoch < {end_day}",
                               table_name=table_name , dev=dev, start_day=start_day, end_day=end_day);
            }
            _ =>{
                return format!("SELECT {field} FROM nebula_device_data.{table_name} WHERE deviceid = '{dev}' AND epoch >= {start_day} AND epoch < {end_day}",
                               field=field ,table_name=table_name , dev=dev, start_day=start_day, end_day=end_day);
            }
        }

    }
}

pub fn feature_replace(feature_name: &str) -> String{

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
