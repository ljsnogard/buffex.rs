use abs_cancel::TrMayCancel;
use anylr::SomeOf;

use crate::{
    Demand,
    buffer::TrBuffSegmMut,
    error::{TrTaggedError, WriteErrTag},
};

pub trait TrBuffTryWrite<T = u8> {
    type SegmMut<'f>: TrBuffSegmMut<'f, T> where Self: 'f;

    type Err: TrTaggedError<WriteErrTag>;

    fn try_write<'f>(
        &'f mut self,
        demand: &'f Demand<usize>,
    ) -> SomeOf<Self::SegmMut<'f>, Self::Err>;
}

/// A kind of buffer that owns the memory for writing data by lending some
/// segments to the producer.
///
/// This design is to keep compatible with `io_uring` and polling model.
pub trait TrBuffWrite<T = u8>
where
    Self: TrBuffTryWrite<T>,
{
    type WriteAsync<'f>: TrMayCancel<'f, MayCancelOutput =
        SomeOf<Self::SegmMut<'f>, Self::Err>>
    where
        Self: 'f;

    /// Lend some segments for writing in an async manner. The total amount of
    /// items is specified by the parameter `demand`.
    fn write_async<'f>(
        &'f mut self,
        demand: &'f Demand<usize>,
    ) -> Self::WriteAsync<'f>;
}

pub trait TrBuffTryFlush<T = u8>
where
    Self: TrBuffTryWrite<T>,
{
    fn try_flush(&mut self) -> Result<(), Self::Err>;

    /// An idempotent way to tell the other side that, the write end is sealed
    /// and will sends no more data.
    fn close(&mut self) -> Result<(), Self::Err>;
}


/// A kind of buffer that has downstream consumer.
pub trait TrBuffFlush<T = u8>
where
    Self: TrBuffWrite<T> + TrBuffTryFlush<T>,
{
    type FlushAsync<'f>: TrMayCancel<'f, MayCancelOutput =
        Result<(), <Self as TrBuffTryWrite<T>>::Err>>
    where
        Self: 'f;

    /// Make sure all the data in the buffer is delivered to the stream consumer.
    fn flush_async(&mut self) -> Self::FlushAsync<'_>;

    type CloseAsync<'f>: TrMayCancel<'f, MayCancelOutput =
        Result<(), <Self as TrBuffTryWrite<T>>::Err>>
    where
        Self: 'f;

    fn close_async(&mut self) -> Self::CloseAsync<'_>;
}
