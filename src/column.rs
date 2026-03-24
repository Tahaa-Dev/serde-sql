#[derive(Default, Clone, Debug)]
pub struct SqlIndexColumn {
    pub name: Option<String>,
    pub opclass: Option<String>,
    pub sort_order: Option<IndexSortOrder>,
    pub null_order: Option<IndexNullOrder>,
    pub method: Option<IndexMethod>,
    pub is_concurrent: bool,
    pub is_unique: bool,
    pub included_cols: Option<Vec<String>>,
    pub predicate: Option<String>,
}

#[derive(Clone, Copy, Debug)]
pub enum IndexMethod {
    BTree { fillfactor: Option<u8> },

    Hash { fillfactor: Option<u8> },

    Gin { fastupdate: Option<bool>, gin_pending_list_limit: Option<u32> },

    Gist { fillfactor: Option<u8>, buffering: Option<GistBufMode> },

    Brin { pages_per_range: Option<u32>, autosummarize: Option<bool> },

    SpGist { fillfactor: Option<u8> },

    Other,
}

impl IndexMethod {
    pub(crate) fn set_fillfactor(&mut self, value: u8) -> Result<(), ()> {
        match self {
            Self::BTree { fillfactor } => *fillfactor = Some(value),
            Self::Hash { fillfactor } => *fillfactor = Some(value),
            Self::Gist { fillfactor, buffering: _ } => {
                *fillfactor = Some(value)
            }
            Self::SpGist { fillfactor } => *fillfactor = Some(value),
            _ => return Err(()),
        }

        Ok(())
    }

    pub(crate) fn set_fastupdate(&mut self, value: bool) -> Result<(), ()> {
        match self {
            Self::Gin { fastupdate, gin_pending_list_limit: _ } => {
                *fastupdate = Some(value)
            }
            _ => return Err(()),
        }

        Ok(())
    }

    pub(crate) fn set_gin_pending_list_limit(
        &mut self,
        value: u32,
    ) -> Result<(), ()> {
        match self {
            Self::Gin { fastupdate: _, gin_pending_list_limit } => {
                *gin_pending_list_limit = Some(value)
            }
            _ => return Err(()),
        }

        Ok(())
    }

    pub(crate) fn set_buffering(
        &mut self,
        value: GistBufMode,
    ) -> Result<(), ()> {
        match self {
            Self::Gist { fillfactor: _, buffering } => *buffering = Some(value),
            _ => return Err(()),
        }

        Ok(())
    }

    pub(crate) fn set_pages_per_range(&mut self, value: u32) -> Result<(), ()> {
        match self {
            Self::Brin { pages_per_range, autosummarize: _ } => {
                *pages_per_range = Some(value)
            }
            _ => return Err(()),
        }

        Ok(())
    }

    pub(crate) fn set_autosummarize(&mut self, value: bool) -> Result<(), ()> {
        match self {
            Self::Brin { pages_per_range: _, autosummarize } => {
                *autosummarize = Some(value)
            }
            _ => return Err(()),
        }

        Ok(())
    }
}

#[derive(Clone, Copy, Debug)]
pub enum GistBufMode {
    On,
    Off,
    Auto,
}

#[derive(Clone, Copy, Debug)]
pub enum IndexSortOrder {
    Asc,
    Desc,
}

#[derive(Clone, Copy, Debug)]
pub enum IndexNullOrder {
    NullsFirst,
    NullsLast,
}

#[derive(Clone, Debug)]
pub struct SqlColumn {
    pub sql_type: SqlType,
    pub index: Option<SqlIndexColumn>,
    pub not_null: bool,
    pub is_primary_key: bool,
    pub default: Option<String>,
    pub check: Option<String>,
    pub foreign_key: Option<ForeignKey>,
}

#[derive(Clone, Debug, PartialEq)]
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

    Unknown(String),

    Array(Box<SqlType>, usize),
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum IntervalField {
    Year,
    Month,
    Day,
    Hour,
    Minute,
    Second,
    YearToMonth,
    DayToHour,
    DayToMinute,
    DayToSecond,
    HourToMinute,
    HourToSecond,
    MinuteToSecond,
    None,
}

#[derive(Clone, Debug, PartialEq)]
pub struct ForeignKey {
    pub table: String,
    pub column: Option<String>,
}
