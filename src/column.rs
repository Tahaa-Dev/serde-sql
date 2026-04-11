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

#[derive(Clone, Copy, Debug, PartialEq)]
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

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum GistBufMode {
    On,
    Off,
    Auto,
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum IndexSortOrder {
    Asc,
    Desc,
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum IndexNullOrder {
    NullsFirst,
    NullsLast,
}

#[derive(Clone, Debug, Default)]
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

impl Default for SqlType {
    fn default() -> Self {
        Self::Unknown(String::new())
    }
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

#[derive(Clone, Debug, PartialEq, Default)]
pub struct ForeignKey {
    pub table: String,
    pub column: Option<String>,
    pub on_delete: Option<FkAction>,
    pub on_update: Option<FkAction>,
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum FkAction {
    NoAction,
    Restrict,
    Cascade,
    SetNull,
    SetDefault,
}

#[derive(Clone, Debug, PartialEq)]
pub enum PrimaryKey {
    Single(String),
    Composite(Vec<String>),
}

#[derive(Clone, Debug, PartialEq)]
pub struct ExcludeColumn {
    pub name: String,
    pub operator: ExcludeOperator,
    pub method: Option<IndexMethod>,
    pub opclass: Option<String>,
    pub sort_order: Option<IndexSortOrder>,
    pub null_order: Option<IndexNullOrder>,
    pub included_cols: Option<Vec<String>>,
    pub predicate: Option<String>,
}

#[derive(Clone, Debug, PartialEq)]
pub enum ExcludeOperator {
    Equal,
    NotEqual,
    LessThan,
    LessThanOrEqual,
    GreaterThan,
    GreaterThanOrEqual,
    Overlap,
    Contains,
    ContainedBy,
    StrictlyLeftOf,
    StrictlyRightOf,
    DoesNotExtendToTheRightOf,
    DoesNotExtendToTheLeftOf,
    IsAdjacentTo,
    SameAs,
    StrictlyBelow,
    StrictlyAbove,
    DoesNotExtendAbove,
    DoesNotExtendBelow,
    IsBelow,
    IsAbove,
    Intersects,
    IsHorizontal,
    IsVertical,
    IsPerpendicular,
    IsParallel,
    ContainsGeometric,
    TextSearchMatch,
    ContainedWithinOrEquals,
    ContainsOrEquals,
    StartsWith,
    Unknown(String),
}

impl std::fmt::Display for ExcludeOperator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let symbol = match self {
            Self::Equal => "=",
            Self::NotEqual => "<>",
            Self::LessThan => "<",
            Self::LessThanOrEqual => "<=",
            Self::GreaterThan => ">",
            Self::GreaterThanOrEqual => ">=",
            Self::Overlap => "&&",
            Self::Contains => "@>",
            Self::ContainedBy => "<@",
            Self::StrictlyLeftOf => "<<",
            Self::StrictlyRightOf => ">>",
            Self::DoesNotExtendToTheRightOf => "&<",
            Self::DoesNotExtendToTheLeftOf => "&>",
            Self::IsAdjacentTo => "-|-",
            Self::SameAs => "~=",
            Self::StrictlyBelow => "<<|",
            Self::StrictlyAbove => "|>>",
            Self::DoesNotExtendAbove => "&<|",
            Self::DoesNotExtendBelow => "|&>",
            Self::IsBelow => "<^",
            Self::IsAbove => "^>",
            Self::Intersects => "?#",
            Self::IsHorizontal => "?-",
            Self::IsVertical => "?|",
            Self::IsPerpendicular => "?-|",
            Self::IsParallel => "?||",
            Self::ContainsGeometric => "~",
            Self::TextSearchMatch => "@@",
            Self::ContainedWithinOrEquals => "<<=",
            Self::ContainsOrEquals => ">>=",
            Self::StartsWith => "^@",
            Self::Unknown(s) => s.as_str(),
        };
        f.write_str(symbol)
    }
}

impl ExcludeOperator {
    pub fn from_string(s: &str) -> Self {
        match s {
            "=" => Self::Equal,
            "<>" => Self::NotEqual,
            "<" => Self::LessThan,
            "<=" => Self::LessThanOrEqual,
            ">" => Self::GreaterThan,
            ">=" => Self::GreaterThanOrEqual,
            "&&" => Self::Overlap,
            "@>" => Self::Contains,
            "<@" => Self::ContainedBy,
            "<<" => Self::StrictlyLeftOf,
            ">>" => Self::StrictlyRightOf,
            "&<" => Self::DoesNotExtendToTheRightOf,
            "&>" => Self::DoesNotExtendToTheLeftOf,
            "-|-" => Self::IsAdjacentTo,
            "~=" => Self::SameAs,
            "<<|" => Self::StrictlyBelow,
            "|>>" => Self::StrictlyAbove,
            "&<|" => Self::DoesNotExtendAbove,
            "|&>" => Self::DoesNotExtendBelow,
            "<^" => Self::IsBelow,
            "^>" => Self::IsAbove,
            "?#" => Self::Intersects,
            "?-" => Self::IsHorizontal,
            "?|" => Self::IsVertical,
            "?-|" => Self::IsPerpendicular,
            "?||" => Self::IsParallel,
            "~" => Self::ContainsGeometric,
            "@@" => Self::TextSearchMatch,
            "<<=" => Self::ContainedWithinOrEquals,
            ">>=" => Self::ContainsOrEquals,
            "^@" => Self::StartsWith,
            _ => Self::Unknown(s.to_string()),
        }
    }
}
