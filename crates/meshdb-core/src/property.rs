use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// A Cypher duration — months, days, seconds, nanoseconds. Matches
/// the Bolt 4.4 Duration struct (tag `0x45`) field-for-field so no
/// conversion is needed at the wire boundary. Stored as four
/// separate components because the calendar units (`months`, `days`)
/// can't be reduced to seconds without knowing a reference date,
/// while the exact units (`seconds`, `nanos`) can.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Duration {
    pub months: i64,
    pub days: i64,
    pub seconds: i64,
    pub nanos: i32,
}

/// A Cypher spatial point. Stored as raw coordinates plus an EPSG
/// SRID tag — the SRID determines the coordinate reference system
/// (CRS) and whether `z` is present:
///
/// | SRID | CRS name        | Dims | Axes                        |
/// |------|-----------------|------|-----------------------------|
/// | 7203 | `cartesian`     | 2    | x, y                        |
/// | 9157 | `cartesian-3d`  | 3    | x, y, z                     |
/// | 4326 | `wgs-84`        | 2    | longitude (x), latitude (y) |
/// | 4979 | `wgs-84-3d`     | 3    | longitude, latitude, height |
///
/// The 2D/3D distinction is carried by `z.is_some()` — the SRID is
/// required to agree with that (a 2D SRID with `Some(z)`, or a 3D
/// SRID with `None`, is an invariant violation caller-side).
///
/// Matches the Bolt 4.4 Point2D (tag `0x58`) / Point3D (tag `0x59`)
/// struct shape so the wire conversion is a straight field copy.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct Point {
    pub srid: i32,
    pub x: f64,
    pub y: f64,
    pub z: Option<f64>,
}

/// EPSG SRID for the Cartesian 2D coordinate system.
pub const SRID_CARTESIAN_2D: i32 = 7203;
/// EPSG SRID for the Cartesian 3D coordinate system.
pub const SRID_CARTESIAN_3D: i32 = 9157;
/// EPSG SRID for WGS-84 (geographic 2D).
pub const SRID_WGS84_2D: i32 = 4326;
/// EPSG SRID for WGS-84 3D (geographic + height).
pub const SRID_WGS84_3D: i32 = 4979;

impl Point {
    /// The CRS name for this point's SRID, matching Neo4j's output of
    /// `point.crs` — e.g. `"cartesian"`, `"wgs-84-3d"`. Returns
    /// `"unknown"` for any SRID outside the four recognised values.
    pub fn crs_name(&self) -> &'static str {
        match self.srid {
            SRID_CARTESIAN_2D => "cartesian",
            SRID_CARTESIAN_3D => "cartesian-3d",
            SRID_WGS84_2D => "wgs-84",
            SRID_WGS84_3D => "wgs-84-3d",
            _ => "unknown",
        }
    }

    /// True when the SRID is a WGS-84 (geographic) CRS.
    pub fn is_geographic(&self) -> bool {
        matches!(self.srid, SRID_WGS84_2D | SRID_WGS84_3D)
    }

    /// True when the point has a z coordinate.
    pub fn is_3d(&self) -> bool {
        self.z.is_some()
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", content = "value")]
pub enum Property {
    Null,
    String(String),
    Int64(i64),
    Float64(f64),
    Bool(bool),
    List(Vec<Property>),
    Map(HashMap<String, Property>),
    /// UTC epoch **nanoseconds** with an optional timezone offset
    /// and optional IANA region name. The nanos value is always UTC;
    /// `tz_offset_secs` is the offset at this instant (used for the
    /// `+HH:MM` / `Z` suffix), and `tz_name` — when set — is the
    /// zone identifier (e.g. `"Europe/Stockholm"`) that produced
    /// that offset, rendered as a `[Region/City]` suffix.
    DateTime {
        nanos: i128,
        tz_offset_secs: Option<i32>,
        tz_name: Option<String>,
    },
    /// Local (naive) datetime as epoch nanoseconds.
    /// Formatters omit any timezone suffix.
    LocalDateTime(i128),
    /// Days since the UNIX epoch (1970-01-01, UTC). `i64` gives
    /// roughly ±25 billion years — wide enough to round-trip the
    /// ±999999999 year range the TCK exercises at its extremes.
    /// Maps to Bolt `Date` (struct tag `0x44`), which is also i64.
    Date(i64),
    /// A Cypher duration value — see [`Duration`].
    Duration(Duration),
    /// Time of day as nanoseconds since midnight, with an optional
    /// timezone offset in seconds. Distinct from `DateTime` so the
    /// formatter can produce `'12:31:14.645876123'` instead of a
    /// full date-time string.
    Time {
        nanos: i64,
        tz_offset_secs: Option<i32>,
    },
    /// A spatial point — see [`Point`]. Encoded on the Bolt wire as
    /// either a `Point2D` (tag `0x58`) or `Point3D` (tag `0x59`)
    /// struct depending on whether `z` is set.
    Point(Point),
}

impl Property {
    pub fn type_name(&self) -> &'static str {
        match self {
            Property::Null => "Null",
            Property::String(_) => "String",
            Property::Int64(_) => "Int64",
            Property::Float64(_) => "Float64",
            Property::Bool(_) => "Bool",
            Property::List(_) => "List",
            Property::Map(_) => "Map",
            Property::DateTime { .. } => "DateTime",
            Property::LocalDateTime(_) => "LocalDateTime",
            Property::Date(_) => "Date",
            Property::Duration(_) => "Duration",
            Property::Time { .. } => "Time",
            Property::Point(_) => "Point",
        }
    }
}

impl From<String> for Property {
    fn from(v: String) -> Self {
        Property::String(v)
    }
}

impl From<&str> for Property {
    fn from(v: &str) -> Self {
        Property::String(v.to_string())
    }
}

impl From<i64> for Property {
    fn from(v: i64) -> Self {
        Property::Int64(v)
    }
}

impl From<i32> for Property {
    fn from(v: i32) -> Self {
        Property::Int64(v as i64)
    }
}

impl From<f64> for Property {
    fn from(v: f64) -> Self {
        Property::Float64(v)
    }
}

impl From<bool> for Property {
    fn from(v: bool) -> Self {
        Property::Bool(v)
    }
}

impl<T: Into<Property>> From<Vec<T>> for Property {
    fn from(v: Vec<T>) -> Self {
        Property::List(v.into_iter().map(Into::into).collect())
    }
}
