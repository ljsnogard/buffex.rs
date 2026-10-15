use std::{mem::MaybeUninit, slice};

use abs_buff::{
    error::{TaggedError, WriteErrTag},
    gen_may_cancel_future,
    io::TrOutput,
    x_deps::{abs_cancel, anylr},
};
use abs_cancel::TrCancellationToken;
use anylr::SomeOf;

pub struct WriteAsOutput<'a, W>(&'a mut W)
where
    W: tokio::io::AsyncWrite + Unpin;

impl<'a, W> WriteAsOutput<'a, W>
where
    W: tokio::io::AsyncWrite + Unpin,
{
    pub const fn new(w: &'a mut W) -> Self {
        WriteAsOutput(w)
    }

    pub fn write_async<'f>(
        &'f mut self,
        source: &'f [MaybeUninit<u8>],
    ) -> OutputWriteAsync<'f, 'f, W> {
        OutputWriteAsync::new(self.0, source)
    }
}

impl<'a, W> TrOutput<u8> for WriteAsOutput<'a, W>
where
    W: tokio::io::AsyncWrite + Unpin,
{
    type WriteAsync<'f> = OutputWriteAsync<'f, 'f, W> where Self: 'f, u8: 'f;
    type Err = TaggedError<std::io::Error, WriteErrTag>;

    #[inline]
    fn write_async<'f>(
        &'f mut self,
        source: &'f [MaybeUninit<u8>],
    ) -> Self::WriteAsync<'f> {
        WriteAsOutput::write_async(self, source)
    }
}

#[gen_may_cancel_future(OutputWrite, pub)]
async fn output_write_impl_async_<'f, W, C>(
    output: &'f mut W,
    source: &'f [MaybeUninit<u8>],
    _token: C,
) -> SomeOf<usize, TaggedError<std::io::Error, WriteErrTag>>
where
    W: tokio::io::AsyncWrite + Unpin,
    C: TrCancellationToken,
{
    let size = source.len();
    let buff = source.as_ptr() as *const _ as *const u8;
    let buff = unsafe { slice::from_raw_parts(buff, size) };
    <W as tokio::io::AsyncWriteExt>::write(output, buff)
        .await
        .map_err(|e| (e, WriteErrTag::Propagated).into())
        .into()
}
