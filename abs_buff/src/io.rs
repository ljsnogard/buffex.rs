use core::{
    error,
    future,
    marker::PhantomData,
    mem::{self, MaybeUninit},
    ptr,
};

use abs_cancel::TrMayCancel;
use anylr::SomeOf;

use crate::error::{ReadErrTag, TrTaggedError, WriteErrTag};

/// A device that will produce data. And the data shall be buffered when taking
/// taking out of them from this device.
pub trait TrInput<T = u8> {
    type ReadAsync<'f>: TrMayCancel<'f, MayCancelOutput = SomeOf<usize, Self::Err>>
    where
        Self: 'f,
        T: 'f;

    type Err: TrTaggedError<ReadErrTag>;

    /// Read data from this input device and into the specified target buffer.
    ///
    /// ## Safety
    ///
    /// - It's the responsibility of the implementation providers to guarantee that,
    ///   data written into the `target` must be memory-aligned for type `T`;
    ///
    /// - It's the responsibility of the caller to guarantee that, conversion from
    ///   `MaybeUninit<T>` to `T` is sound;
    ///
    /// - For example, if `T: Clone` is satisfied, implementaion provider to move
    ///   a `t` of `T` into `target`, should do `target[0].write(t.clone())`; caller
    ///   should do `let t = target[0].assume_init()`;
    fn read_async<'f>(
        &'f mut self,
        target: &'f mut [MaybeUninit<T>],
    ) -> Self::ReadAsync<'f>;
}

/// A device that will consume data. And the data shall be offered with
/// a buffer.
pub trait TrOutput<T = u8> {
    type WriteAsync<'f>: TrMayCancel<'f, MayCancelOutput = SomeOf<usize, Self::Err>>
    where
        Self: 'f,
        T: 'f;

    type Err: TrTaggedError<WriteErrTag>;

    /// Move data from the specified source into this output device
    fn write_async<'f>(
        &'f mut self,
        source: &'f [MaybeUninit<T>],
    ) -> Self::WriteAsync<'f>;

    /// Clone data from the specified source buffer into this output device
    fn write_cloned_async<'a>(
        &'a mut self,
        source: &'a [T],
    ) -> Self::WriteAsync<'a>
    where
        T: Clone,
    {
        if mem::size_of::<T>() == 0 {
            // Handle ZSTs separately, as copying them is unnecessary and UB
            return self.write_async(&[]);
        }
        unsafe {
            let src_head = &source[0] as *const T as *const MaybeUninit<T>;
            let slice = ptr::slice_from_raw_parts(src_head, source.len());
            self.write_async(&*slice)
        }
    }
}

#[derive(Clone, Copy, Debug, Default)]
pub struct Blackhole;

impl crate::buffer::TrProducerState for Blackhole {}
impl crate::buffer::TrConsumerState for Blackhole {}

impl<T> TrOutput<T> for Blackhole {
    type WriteAsync<'f> = BlackholeIoAsync<'f, T>
    where
        Self: 'f,
        T: 'f;
    type Err = BlackholeIoError;

    fn write_async<'f>(&'f mut self, _: &'f [MaybeUninit<T>]) -> Self::WriteAsync<'f> {
        BlackholeIoAsync(PhantomData)
    }
}

impl<T> TrInput<T> for Blackhole {
    type ReadAsync<'f> = BlackholeIoAsync<'f, T>
    where
        Self: 'f,
        T: 'f;
    type Err = BlackholeIoError;

    fn read_async<'f>(&'f mut self, _: &'f mut [MaybeUninit<T>]) -> Self::ReadAsync<'f> {
        BlackholeIoAsync(PhantomData)
    }
}

/// An error telling user that trying to operate IO on an null device
#[derive(Clone, Copy, Debug, Default)]
pub struct BlackholeIoError;

pub struct BlackholeIoAsync<'a, T>(PhantomData<fn(&'a ()) -> T>);

impl core::fmt::Display for BlackholeIoError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(f, "[This is a blackhole!]")
    }
}

impl error::Error for BlackholeIoError {}

impl TrTaggedError<ReadErrTag> for BlackholeIoError {
    fn err_tag(&self) -> ReadErrTag {
        ReadErrTag::Closing
    }
}

impl TrTaggedError<WriteErrTag> for BlackholeIoError {
    fn err_tag(&self) -> WriteErrTag {
        WriteErrTag::Closing
    }
}

impl<'a, T> future::IntoFuture for BlackholeIoAsync<'a, T> {
    type IntoFuture = future::Ready<Self::Output>;
    type Output = SomeOf<usize, BlackholeIoError>;

    fn into_future(self) -> Self::IntoFuture {
        future::ready::<SomeOf<usize, BlackholeIoError>>(SomeOf::new_right(BlackholeIoError))
    }
}

impl<'a, T> TrMayCancel<'a> for BlackholeIoAsync<'a, T>
where
    T: 'a,
{
    type MayCancelOutput = <Self as IntoFuture>::Output;

    type MayCancelFuture<'f, C> = <Self as IntoFuture>::IntoFuture
    where
        'f: 'a,
        Self: 'f,
        C: 'a + abs_cancel::TrCancellationToken,;

    fn may_cancel_with<C>(self, _: C) -> Self::MayCancelFuture<'a, C>
    where
        C: 'a + abs_cancel::TrCancellationToken,
    {
        future::ready(SomeOf::new_right(BlackholeIoError))
    }
}
