use core::fmt;

pub trait TrErrTag
where
    Self: Clone + Copy + Eq + Ord + PartialEq + PartialOrd
        + fmt::Debug + fmt::Display,
{}

pub trait TrReadErrTag<T>
where
    Self: TrErrTag,
    T: TrErrTag,
{
    fn into_read_err_tag(self) -> T;
}

pub trait TrWriteErrTag<T>
where
    Self: TrErrTag,
    T: TrErrTag,
{
    fn into_write_err_tag(self) -> T;
}

pub trait TrErrorWrapper<E, T>
where
    Self: core::error::Error
        + AsRef<Self::PropagatedError>
        + AsMut<Self::PropagatedError>,
    E: TrTaggedError<T>,
    T: TrErrTag,
{
    type PropagatedError: core::error::Error;

    fn propagated_err(self) -> Result<Self::PropagatedError, Self>
    where
        Self: Sized;
}

pub trait TrTaggedError<TyTag>
where
    Self: core::error::Error,
    TyTag: TrErrTag,
{
    fn err_tag(&self) -> TyTag;
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum IoErrTag {
    Read(ReadErrTag),
    Write(WriteErrTag),
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum ReadErrTag {
    /// No more data availale and the read end is closed
    Closing,

    /// Temporarily no available data in buffer, should retry later
    Drained,

    /// There is data to read but cannot satisfy the demand.
    Unsatisfied,

    /// Operation cancelled by token during async task running.
    Cancelled,

    /// Error is from propagated.
    Propagated,

    /// Error exists but we don't know it
    Unknown,
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum WriteErrTag {
    /// The more space available for writing and the write end is closed.
    Closing,

    /// Operation cancelled by token during async task running.
    Cancelled,

    /// Error is propagated.
    Propagated,

    /// Error exists but we don't know it, should no longer trying.
    Unknown,

    /// Temporarily no available space in buffer to write, should retry later.
    Stuffed,

    /// There is data to read but cannot satisfy the demand.
    Unsatisfied,
}

impl IoErrTag {
    pub const fn should_terminate(&self) -> bool {
        match self {
            IoErrTag::Read(r) => r.should_terminate(),
            IoErrTag::Write(w) => w.should_terminate(),
        }
    }
}
impl TrErrTag for IoErrTag {}
impl From<ReadErrTag> for IoErrTag {
    fn from(value: ReadErrTag) -> Self {
        IoErrTag::Read(value)
    }
}
impl From<WriteErrTag> for IoErrTag {
    fn from(value: WriteErrTag) -> Self {
        IoErrTag::Write(value)
    }
}

impl core::fmt::Display for IoErrTag {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            IoErrTag::Read(r) => r.fmt(f),
            IoErrTag::Write(w) => w.fmt(f),
        }
    }
}

impl ReadErrTag {
    pub const fn should_terminate(&self) -> bool {
        matches!(
            self,
            Self::Closing | Self::Cancelled | Self::Propagated | Self::Unknown
        )
    }
}
impl TrErrTag for ReadErrTag {}
impl TrReadErrTag<Self> for ReadErrTag  {
    fn into_read_err_tag(self) -> Self {
        self
    }
}
impl core::fmt::Display for ReadErrTag {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ReadErrTag::Closing     => write!(f, "ReadErrTag::Closing"),
            ReadErrTag::Cancelled   => write!(f, "ReadErrTag::Cancelled"),
            ReadErrTag::Propagated  => write!(f, "ReadErrTag::Propagated"),
            ReadErrTag::Unknown     => write!(f, "ReadErrTag::Unknown"),
            // retriable
            ReadErrTag::Drained     => write!(f, "ReadErrTag::Drained"),
            ReadErrTag::Unsatisfied => write!(f, "ReadErrTag::Unsatisfied"),
        }
    }
}

impl WriteErrTag {
    pub const fn should_terminate(&self) -> bool {
        matches!(
            self,
            Self::Closing | Self::Cancelled | Self::Propagated | Self::Unknown
        )
    }
}
impl TrErrTag for WriteErrTag {}
impl TrWriteErrTag<Self> for WriteErrTag {
    fn into_write_err_tag(self) -> Self {
        self
    }
}
impl core::fmt::Display for WriteErrTag {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            WriteErrTag::Closing     => write!(f, "WriteErrTag::Closing"),
            WriteErrTag::Cancelled   => write!(f, "WriteErrTag::Cancelled"),
            WriteErrTag::Propagated  => write!(f, "WriteErrTag::Propagated"),
            WriteErrTag::Unknown     => write!(f, "WriteErrTag::Unknown"),
            // -- retriable
            WriteErrTag::Stuffed     => write!(f, "WriteErrTag::Stuffed"),
            WriteErrTag::Unsatisfied => write!(f, "WriteErrTag::Unsatisfied"),
        }
    }
}

#[derive(Debug)]
pub struct TaggedError<TyErr, TyTag>
where
    TyErr: core::error::Error,
    TyTag: TrErrTag,
{
    err_: TyErr,
    tag_: TyTag,
}

impl<TyErr, TyTag> TaggedError<TyErr, TyTag>
where
    TyErr: core::error::Error,
    TyTag: TrErrTag,
{
    pub const fn new(err: TyErr, tag: TyTag) -> Self {
        TaggedError { err_: err, tag_: tag }
    }

    pub const fn tag(&self) -> TyTag {
        self.tag_
    }

    pub const fn err(&self) -> &TyErr {
        &self.err_
    }
}

impl<TyErr, TyTag> From<(TyErr, TyTag)> for TaggedError<TyErr, TyTag>
where
    TyErr: core::error::Error,
    TyTag: TrErrTag,
{
    fn from(value: (TyErr, TyTag)) -> Self {
        let (err, tag) = value;
        TaggedError { err_: err, tag_: tag }
    }
}

impl<TyErr, TyTag> core::fmt::Display for TaggedError<TyErr, TyTag>
where
    TyErr: core::error::Error,
    TyTag: TrErrTag,
{
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        core::fmt::Display::fmt(&self.err_, f)
    }
}

impl<TyErr, TyTag> core::error::Error for TaggedError<TyErr, TyTag>
where
    TyErr: core::error::Error,
    TyTag: TrErrTag,
{
    fn source(&self) -> Option<&(dyn core::error::Error + 'static)> {
        self.err_.source()
    }
}

impl<TyErr, TyTag> TrTaggedError<TyTag> for TaggedError<TyErr, TyTag>
where
    TyErr: core::error::Error,
    TyTag: TrErrTag,
{
    fn err_tag(&self) -> TyTag {
        self.tag_
    }
}

impl<TyErr, TyTag> AsRef<TyErr> for TaggedError<TyErr, TyTag>
where
    TyErr: core::error::Error,
    TyTag: TrErrTag,
{
    fn as_ref(&self) -> &TyErr {
        &self.err_
    }
}

impl<TyErr, TyTag> AsMut<TyErr> for TaggedError<TyErr, TyTag>
where
    TyErr: core::error::Error,
    TyTag: TrErrTag,
{
    fn as_mut(&mut self) -> &mut TyErr {
        &mut self.err_
    }
}

impl<TyErr, TyTag> TrErrorWrapper<TaggedError<TyErr, TyTag>, TyTag> for
    TaggedError<TyErr, TyTag>
where
    TyErr: core::error::Error,
    TyTag: TrErrTag,
{
    type PropagatedError = TyErr;

    fn propagated_err(self) -> Result<Self::PropagatedError, Self>
    where
        Self: Sized
    {
        Result::Ok(self.err_)
    }
}
