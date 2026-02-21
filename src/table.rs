pub struct SqlColumn {
    pub name: String,
    pub sql_type: SqlType,
    pub is_indexed: bool,
    pub not_null: bool,
}

pub enum SqlType {
    // Numeric
    SmallInt,
    Integer,
    BigInt,
    Decimal(Option<usize>, Option<usize>),
    Numeric(Option<usize>, Option<usize>),
    Real,
    DoublePrecision,

    // String/Text
    Char(usize),
    VarChar(Option<usize>),
    Text,

    // Binary
    ByteA,

    // Date/Time
    Timestamp(Option<usize>),
    Timestamptz(Option<usize>),
    Date,
    Time(Option<usize>),
    Interval { fields: IntervalField, precision: Option<usize> },

    // Boolean
    Boolean,

    // Network
    Inet,
    Cidr,
    MacAddr,

    // Semi-Structured
    Json,
    Jsonb,
    Uuid,

    // Serial
    SmallSerial,
    Serial,
    BigSerial,
}

#[derive(Clone)]
pub enum IntervalField {
    Year, Month, Day, Hour, Minute, Second,
    YearToMonth,
    DayToHour, DayToMinute, DayToSecond,
    HourToMinute, HourToSecond,
    MinuteToSecond,
    None
}
