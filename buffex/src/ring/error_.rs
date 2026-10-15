//! `CircularBuff` 的错误类型。
//!
//! * [`TxError`]——写（生产）端的错误，携带失败时写位置/空间快照；
//! * [`RxError`]——读（消费）端的错误，携带失败时读位置/数据快照；
//! * [`EndError`]——端访问错误：主动端不对外暴露，尝试访问时的错误。

use core::fmt;

use abs_buff::error::{ReadErrTag, TrTaggedError, WriteErrTag};

/// 写（生产）端错误。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProducerError<S> {
    /// 缓冲区已满（或可写空间不足 `Demand` 下限），携带当前写位置。
    Stuffed(S),
    /// 写端已关闭，不再接受数据。
    Closing,
    /// 调用者主动取消
    Cancelled,
    /// 请求不可能满足
    Unsatisfiable,
}

impl<S: fmt::Debug> fmt::Display for ProducerError<S> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ProducerError::Stuffed(p) => write!(f, "ProducerError::Stuffed(at: {p:?})"),
            ProducerError::Closing => write!(f, "ProducerError::Closing"),
            ProducerError::Cancelled => write!(f, "ProducerError::Cancelled"),
            ProducerError::Unsatisfiable => write!(f, "ProducerError::Unsatisfiable"),
        }
    }
}

impl<S: fmt::Debug> core::error::Error for ProducerError<S> {}

impl<S: fmt::Debug> TrTaggedError<WriteErrTag> for ProducerError<S> {
    fn err_tag(&self) -> WriteErrTag {
        match self {
            ProducerError::Closing | ProducerError::Cancelled
                => WriteErrTag::Closing,
            ProducerError::Unsatisfiable => WriteErrTag::Unknown,
            ProducerError::Stuffed(_) => WriteErrTag::Stuffed,
        }
    }
}

/// 读（消费）端错误。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConsumerError<S> {
    /// 缓冲区已空（或可读数据不足 `Demand` 下限），携带当前读位置。
    Drained(S),
    /// 读端已关闭，不再有数据。
    Closing,
    /// 调用者主动取消
    Cancelled,
    /// 请求不可能满足
    Unsatisfiable,
}

impl<S: fmt::Debug> fmt::Display for ConsumerError<S> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ConsumerError::Drained(p) => write!(f, "ConsumerError::Drained(at: {p:?})"),
            ConsumerError::Closing => write!(f, "ConsumerError::Closing"),
            ConsumerError::Cancelled => write!(f, "ConsumerError::Cancelled"),
            ConsumerError::Unsatisfiable => write!(f, "ConsumerError::Unsatisfiable"),
        }
    }
}

impl<S: fmt::Debug> core::error::Error for ConsumerError<S> {}

impl<S: fmt::Debug> TrTaggedError<ReadErrTag> for ConsumerError<S> {
    fn err_tag(&self) -> ReadErrTag {
        match self {
            ConsumerError::Closing | ConsumerError::Cancelled
                => ReadErrTag::Closing,
            ConsumerError::Unsatisfiable => ReadErrTag::Unknown,
            ConsumerError::Drained(_) => ReadErrTag::Drained,
        }
    }
}
