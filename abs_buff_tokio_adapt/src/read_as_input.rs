use std::{io, mem::MaybeUninit, slice};

use abs_buff::{
    error::{ReadErrTag, TaggedError},
    gen_may_cancel_future,
    io::TrInput,
    x_deps::{abs_cancel, anylr},
};
use abs_cancel::TrCancellationToken;
use anylr::SomeOf;

pub struct ReadAsInput<'a, R>(&'a mut R)
where
    R: tokio::io::AsyncRead + Unpin;

impl<'a, R> ReadAsInput<'a, R>
where
    R: tokio::io::AsyncRead + Unpin,
{
    pub const fn new(w: &'a mut R) -> Self {
        ReadAsInput(w)
    }

    pub fn read_async<'f>(
        &'f mut self,
        target: &'f mut [MaybeUninit<u8>],
    ) -> InputReadAsync<'f, 'f, R> {
        InputReadAsync::new(self.0, target)
    }
}

impl<'a, R> TrInput<u8> for ReadAsInput<'a, R>
where
    R: tokio::io::AsyncRead + Unpin,
{
    type ReadAsync<'f> = InputReadAsync<'f, 'f, R> where Self: 'f, u8: 'f;

    type Err = TaggedError<std::io::Error, ReadErrTag>;

    #[inline]
    fn read_async<'f>(
        &'f mut self,
        target: &'f mut [MaybeUninit<u8>],
    ) -> Self::ReadAsync<'f> {
        ReadAsInput::read_async(self, target)
    }
}

/// [`TrInput::read_async`] 的实现。
///
/// # EOF 的表达
///
/// tokio 的 `AsyncRead` 用「读到 0」表示流结束，但 `abs_buff` 的输入搬移**不允许**
/// 「返回 0 个且无错误」——`move_items_from_input_async` 内层会
/// `assert!(*cc > 0 || x.contains_right())`（提交 `e4092c8`），因为那正是它过去死循环的
/// 输入。因此这里把「读到 0」翻译成带 [`ReadErrTag::Closing`]（终止性标签）的错误；
/// 段级搬移据此正常收尾，而不是 panic 或空转。
#[gen_may_cancel_future(InputRead, pub)]
async fn input_read_impl_async_<'f, R, C>(
    input: &'f mut R,
    target: &'f mut [MaybeUninit<u8>],
    _token: C,
) -> SomeOf<usize, TaggedError<std::io::Error, ReadErrTag>>
where
    R: tokio::io::AsyncRead + Unpin,
    C: TrCancellationToken,
{
    let size = target.len();
    let buff = target.as_mut_ptr() as *mut u8;
    let buff = unsafe { slice::from_raw_parts_mut(buff, size) };
    let got = <R as tokio::io::AsyncReadExt>::read(input, buff).await;
    match got {
        // 读到 0 = 流结束（tokio 约定）；`abs_buff` 要求以错误表达，否则会 panic。
        Result::Ok(0) => SomeOf::new_right(TaggedError::new(
            io::Error::from(io::ErrorKind::UnexpectedEof),
            ReadErrTag::Closing,
        )),
        Result::Ok(n) => SomeOf::new_left(n),
        Result::Err(e) => SomeOf::new_right((e, ReadErrTag::Propagated).into()),
    }
}
