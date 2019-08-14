use serde::*;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use actix_web::Result;
use std::fmt;

#[derive(Deserialize)]
pub struct Url {
    pub(crate) deviceid: String,
    pub(crate) epoch: String,
    pub(crate) feature: String,
}

#[derive(Deserialize)]
pub struct FilterUrl {
    pub(crate) deviceid: String,
    pub(crate) epoch: String,
    pub(crate) feature: String,
    pub(crate) filter: i32,
}

#[derive(Deserialize)]
pub struct HelperEpochUrl {
    pub(crate) deviceid: String,
    pub(crate) feature: String,
    pub(crate) value: String,
    pub(crate) sub_value: String,
}

#[derive(Deserialize)]
pub struct HelperUrl {
    pub(crate) deviceid: String,
    pub(crate) feature: String,
    pub(crate) value: String,
}

#[derive(Deserialize)]
pub struct PostUrl {
    pub(crate) deviceid: String,
    //    pub(crate) latest: String,
    pub(crate) feature: String,
}


#[derive(Debug, Clone, Serialize)]
pub struct Status {
    pub(crate) msg: Msg,
    pub(crate) result: Msg,
    pub(crate) data: Value,
}

#[derive(Debug, Deserialize)]
pub struct PostInfo {
    pub(crate) clients: Vec<String>,
}

#[derive(Debug, Deserialize)]
pub struct PostFilter {
    pub(crate) filter: Vec<String>,
}

#[derive(Debug, Clone, Copy,
PartialEq, Eq,)]
pub enum Msg {
    Failed,
    Ok,
    UnknownFeature,
    WrongTimePeriod(Option<WrongTimePeriodReason>),
    TimeRangeShouldBeInteger,
    InvalidTimeRange,
    InvalidRequest,
    EpochLastStatus,
    UuidFormedBadly,
    MacFormedBadly,
    UnknownFilter,
    NotEnoughValues,

}
#[derive(Debug, Clone, Copy,
PartialEq, Eq,)]
pub enum WrongTimePeriodReason {
    OverOneYear,
}

#[derive(Debug)]
pub enum CassandraError {
    UnknownFeature,
}

impl Serialize for Msg {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
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
            | InvalidRequest => "invalid request!",
            | EpochLastStatus => "the epoch of last connect / disconnect before query is incorrect",
            | UuidFormedBadly => "badly formed hexadecimal UUID string",
            | MacFormedBadly => "badly formed hexadecimal MAC string",
            | UnknownFilter => "unknown filter field",
            | NotEnoughValues => "not enough values to unpack",
        })
    }
}

impl fmt::Display for CassandraError {
    fn fmt (&self, f: &mut fmt::Formatter)-> fmt::Result {
        write!(f, "unknown feature")
    }
}
