use abs_cancel::TrMayCancel;
use anylr::SomeOf;

use crate::{
    Demand,
    buffer::TrBuffSegmRef,
    error::{ReadErrTag, TrTaggedError},
};

pub trait TrBuffTryRead<T = u8> {
    type SegmRef<'f>: TrBuffSegmRef<'f, T> where Self: 'f;

    type Err: TrTaggedError<ReadErrTag>;

    fn try_read<'f>(
        &'f mut self,
        demand: &'f Demand<usize>,
    ) -> SomeOf<Self::SegmRef<'f>, Self::Err>;
}

/// A kind of buffer that owns the memory for reading data by lending some
/// segments to the consumer.
///
/// This design is to keep compatible with `io_uring` and polling model.
pub trait TrBuffRead<T = u8>
where
    Self: TrBuffTryRead<T>,
{
    type ReadAsync<'f>: TrMayCancel<'f, MayCancelOutput =
        SomeOf<Self::SegmRef<'f>, Self::Err>>
    where
        Self: 'f;

    /// Emits borrowed segment which carries the buffered items. The amount of items
    /// can be specified by the parameter `demand`.
    fn read_async<'f>(
        &'f mut self,
        demand: &'f Demand<usize>,
    ) -> Self::ReadAsync<'f>;
}
