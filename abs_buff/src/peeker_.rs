use abs_cancel::TrMayCancel;
use anylr::SomeOf;

use crate::{
    buffer::TrBuffSegmRef,
    error::{ReadErrTag, TrTaggedError},
};

pub trait TrBuffTryPeek<T = u8> {
    type SegmPeek<'f>: TrBuffSegmRef<'f, T> where Self: 'f;

    type Err: TrTaggedError<ReadErrTag>;

    fn try_peek<'f>(&'f mut self) -> SomeOf<Self::SegmPeek<'f>, Self::Err>;
}

/// A kind of buffer that owns the memory for peeking the received data without
/// consuming them.
///
/// This design is to keep compatible with `io_uring` and polling model.
pub trait TrBuffPeek<T = u8>
where
    Self: TrBuffTryPeek<T>,
{
    type PeekAsync<'f>: TrMayCancel<'f, MayCancelOutput =
        SomeOf<Self::SegmPeek<'f>, Self::Err>>
    where
        Self: 'f;

    /// Lend some slices for peeking. The number and the length of the slices
    /// to peek are decided by the buffer.
    ///
    /// The left side of SomeOf is `TrBuffSegmRef` encapsulating some buffers.
    /// That means the call may result in more than one buffer available.
    fn peek_async<'f>(
        &'f mut self,
    ) -> Self::PeekAsync<'f>;
}
