//! 设备级适配：把 smol 的 [`AsyncRead`] / [`AsyncWrite`] 接成 `abs_buff` 的
//! [`TrInput`] / [`TrOutput`]。
//!
//! # 形态：只有借用
//!
//! | 类型 | 方向 | 设备 |
//! | --- | --- | --- |
//! | [`ReadAsInput`] | 设备 → `TrInput` | 借用 `&mut R` |
//! | [`WriteAsOutput`] | `TrOutput` → 设备 | 借用 `&mut W` |
//!
//! 与 `buffex_tokio_adapt` 的设备层一致：搬运泵在适配器内部，设备由适配器拥有，泵每一轮
//! 借一次即可；调用方写自己的泵时也是同样的用法。因此没有「拥有设备」的变体，也没有
//! 中转缓冲——两侧都是零拷贝。
//!
//! # 零拷贝，与其中一处刻意的 `unsafe`
//!
//! * **写向**（段 → 设备）：[`AsyncWrite::poll_write`] 收 `&[u8]`，而段里的字节由调用方
//!   保证已初始化，于是 [`as_init_bytes_`] 直接把切片借给设备——零拷贝、无需 `unsafe`
//!   之外的任何处理（那一次转换本身就是安全的，注释在函数上）。
//! * **读向**（设备 → 段）：[`AsyncRead::poll_read`] 收 `&mut [u8]`（**已初始化**的可写
//!   内存），而环的写段是借来的 `&mut [MaybeUninit<u8>]`。若走中转缓冲，每一批数据都要
//!   多一次「设备 → 中转 → 段」的往返拷贝；本 crate 选择**不做中转**，用
//!   [`assume_init_bytes_mut_`] 把写段的窗口**原样**借给设备。
//!
//! 第二处的代价是一处 `unsafe`：构造指向尚未初始化内存的 `&mut [u8]` 处于 Rust 引用
//! 有效性的边界上（引用按定义应指向有效值，而「未初始化的 `u8`」不是有效值）。本 crate
//! 明知这一点而仍然采用它，理由与边界条件全部写在 [`assume_init_bytes_mut_`] 的文档里：
//! 设备只会**写**这段内存、返回前不会被任何代码读取，因此实际执行中不会读到未初始化
//! 字节；换来的是读方向与写方向一样完全不经过中转缓冲。
//!
//! # EOF 与设备违约的翻译
//!
//! * `abs_buff` 的输入搬移**不允许**「返回 0 个且无错误」（内层会
//!   `assert!(cc > 0 || x.contains_right())`），而 `poll_read` 用 `Ok(0)` 表示 EOF。
//!   本模块把 `Ok(0)` 翻成带 [`ReadErrTag::Closing`] 标签的错误（`Closing` 是
//!   **终止性**标签，段级搬移据此收尾而不是空转）。
//! * 写侧：非空缓冲上的 `Ok(0)` 是设备违约，`abs_buff` 的输出搬移对它同样会空转
//!   （见 `buffex_tokio_adapt` 的 `ZeroWriteAsErr`）。这里在**设备边界**把它翻成
//!   [`io::ErrorKind::WriteZero`]，适配器便可按规则记错误、终止收尾。

use core::{future::poll_fn, mem::MaybeUninit, pin::Pin};

use std::io;

use abs_buff::{
    error::{ReadErrTag, TaggedError, WriteErrTag},
    gen_may_cancel_future,
    io::{TrInput, TrOutput},
    x_deps::{abs_cancel, anylr::SomeOf},
};
use buffex::x_deps::abs_buff;
use futures_lite::io::{AsyncRead, AsyncWrite};

/// 借用设备的读适配：把 `&mut R`（smol 读设备）适配为 [`TrInput<u8>`]。
///
/// 设备读到的字节**直接落在**环的写段里，不经过任何中转缓冲。
///
/// # Examples
///
/// ```no_run
/// use buffex_smol_adapt::ReadAsInput;
/// use futures_lite::io::Cursor;
///
/// # async fn demo(dev: &mut Cursor<Vec<u8>>) {
/// let mut input = ReadAsInput::new(dev);
/// let _ = &mut input;
/// # }
/// ```
pub struct ReadAsInput<'a, R>(&'a mut R)
where
    R: AsyncRead + Unpin;

impl<'a, R> ReadAsInput<'a, R>
where
    R: AsyncRead + Unpin,
{
    /// 借用给定的 smol 读设备。
    pub const fn new(reader: &'a mut R) -> Self {
        ReadAsInput(reader)
    }

    /// 读入口，等价于 [`TrInput::read_async`]。
    pub fn read_async<'f>(
        &'f mut self,
        target: &'f mut [MaybeUninit<u8>],
    ) -> SmolInputReadAsync<'f, 'f, R> {
        SmolInputReadAsync::new(self.0, target)
    }
}

impl<R> TrInput<u8> for ReadAsInput<'_, R>
where
    R: AsyncRead + Unpin,
{
    type ReadAsync<'f>
        = SmolInputReadAsync<'f, 'f, R>
    where
        Self: 'f,
        u8: 'f;

    type Err = TaggedError<io::Error, ReadErrTag>;

    fn read_async<'f>(&'f mut self, target: &'f mut [MaybeUninit<u8>]) -> Self::ReadAsync<'f> {
        ReadAsInput::read_async(self, target)
    }
}

/// [`ReadAsInput::read_async`] 的 step 函数。
#[gen_may_cancel_future(SmolInputRead, pub, new(pub(crate)))]
async fn smol_input_read_impl_async_<'f, R, C>(
    input: &'f mut R,
    target: &'f mut [MaybeUninit<u8>],
    _token: C,
) -> SomeOf<usize, TaggedError<io::Error, ReadErrTag>>
where
    R: AsyncRead + Unpin,
    C: abs_cancel::TrCancellationToken,
{
    if target.is_empty() {
        // 空目标：没有可写窗口，也没读任何字节。这是「没有请求」而不是「读到 0」，
        // 因此不能当成 EOF；段级搬移的调用方不会传空目标（它只在 `take > 0` 时调用）。
        return SomeOf::new_left(0usize);
    }
    // SAFETY: 满足 `assume_init_bytes_mut_` 的三条要求——
    // ① 窗口随后就交给设备写，返回前没有任何代码读取它；
    // ② 设备报 `Ok(n)` 时前 n 个字节必定已初始化，我们只把「前 n 个」计入已搬量；
    // ③ `window` 的借用在本函数内结束，不会外泄到段以外。
    let window = unsafe { assume_init_bytes_mut_(target) };
    match poll_read_(input, window).await {
        // 读到 0 = EOF：`abs_buff` 要求以（终止性标签的）错误表达，否则段级搬移会 panic。
        Result::Ok(0) => SomeOf::new_right(TaggedError::new(
            io::Error::from(io::ErrorKind::UnexpectedEof),
            ReadErrTag::Closing,
        )),
        Result::Ok(n) => SomeOf::new_left(n),
        Result::Err(err) => SomeOf::new_right(TaggedError::new(err, ReadErrTag::Propagated)),
    }
}

/// 借用设备的写适配：把 `&mut W`（smol 写设备）适配为 [`TrOutput<u8>`]。
///
/// 源切片的字节由调用方保证已初始化，因此这里**直接把切片借给设备**（零拷贝），
/// 不经过任何中转缓冲。
///
/// # Examples
///
/// ```no_run
/// use buffex_smol_adapt::WriteAsOutput;
/// use futures_lite::io::Cursor;
///
/// # async fn demo(dev: &mut Cursor<Vec<u8>>) {
/// let mut output = WriteAsOutput::new(dev);
/// let _ = &mut output;
/// # }
/// ```
pub struct WriteAsOutput<'a, W>(&'a mut W)
where
    W: AsyncWrite + Unpin;

impl<'a, W> WriteAsOutput<'a, W>
where
    W: AsyncWrite + Unpin,
{
    /// 借用给定的 smol 写设备。
    pub const fn new(writer: &'a mut W) -> Self {
        WriteAsOutput(writer)
    }

    /// 写入口，等价于 [`TrOutput::write_async`]。
    pub fn write_async<'f>(
        &'f mut self,
        source: &'f [MaybeUninit<u8>],
    ) -> SmolOutputWriteAsync<'f, 'f, W> {
        SmolOutputWriteAsync::new(self.0, source)
    }
}

impl<W> TrOutput<u8> for WriteAsOutput<'_, W>
where
    W: AsyncWrite + Unpin,
{
    type WriteAsync<'f>
        = SmolOutputWriteAsync<'f, 'f, W>
    where
        Self: 'f,
        u8: 'f;

    type Err = TaggedError<io::Error, WriteErrTag>;

    fn write_async<'f>(&'f mut self, source: &'f [MaybeUninit<u8>]) -> Self::WriteAsync<'f> {
        WriteAsOutput::write_async(self, source)
    }
}

/// [`WriteAsOutput::write_async`] 的 step 函数。
#[gen_may_cancel_future(SmolOutputWrite, pub, new(pub(crate)))]
async fn smol_output_write_impl_async_<'f, W, C>(
    output: &'f mut W,
    source: &'f [MaybeUninit<u8>],
    _token: C,
) -> SomeOf<usize, TaggedError<io::Error, WriteErrTag>>
where
    W: AsyncWrite + Unpin,
    C: abs_cancel::TrCancellationToken,
{
    if source.is_empty() {
        return SomeOf::new_left(0usize);
    }
    match poll_write_(output, as_init_bytes_(source)).await {
        // 非空缓冲上的 0 字节是设备违约：若如实返回 `Left(0)`，上游的输出搬移会空转。
        Result::Ok(0) => SomeOf::new_right(TaggedError::new(
            io::Error::new(
                io::ErrorKind::WriteZero,
                "设备在非空缓冲上写入 0 字节（上游会因此空转）",
            ),
            WriteErrTag::Propagated,
        )),
        Result::Ok(n) => SomeOf::new_left(n),
        Result::Err(err) => SomeOf::new_right(TaggedError::new(err, WriteErrTag::Propagated)),
    }
}

/// 把 `&[MaybeUninit<u8>]` 视作已初始化的 `&[u8]`。
///
/// # Safety
///
/// 调用方（段搬移 / 本模块的写适配）保证 `source` 的所有字节都已初始化；
/// `MaybeUninit<u8>` 与 `u8` 布局相同、对齐相同（均为 1）。
fn as_init_bytes_(source: &[MaybeUninit<u8>]) -> &[u8] {
    // SAFETY: 见本函数文档。
    unsafe { core::slice::from_raw_parts(source.as_ptr() as *const u8, source.len()) }
}

/// 把环的**未初始化写窗口**借给设备当作可写字节——读方向零拷贝的关键。
///
/// 这是本 crate 唯一一处「明知处于语言边界而仍然采用」的转换，因此把取舍写全：
///
/// # 为什么需要它
///
/// [`AsyncRead::poll_read`] 的签名是 `&mut [u8]`，要求可写内存**已经初始化**。而
/// `buffex` 的写段给出的正是 `&mut [MaybeUninit<u8>]`——设计上就是「待写入的未初始化
/// 内存」。标准库目前没有稳定的「未初始化读缓冲」抽象（`ReadBuf` 一类只存在于
/// nightly 的 `core_io`），因此要零拷贝就只剩这一条路。
///
/// # Safety
///
/// 调用方必须同时保证：
///
/// 1. 返回的切片只被交给「**只写**这段内存」的设备（本 crate 的设备边界是
///    `AsyncRead::poll_read`，按契约它只会写入），且在设备写入之前不会有任何代码读取
///    它是未初始化的字节；
/// 2. 设备返回 `Ok(n)` 时，前 `n` 个字节必定已经完成初始化；使用方只把前 `n` 个字节
///    计入「已搬入量」，`buffex` 的段也只按该量 `advance_write`；
/// 3. 返回的切片不得离开当次设备调用（调用点把它的生命周期限制在 step 函数内部）。
///
/// # 已知边界（诚实记录）
///
/// 按 Rust 引用的有效性定义，`&mut [u8]` 应当指向有效的 `u8`；「未初始化的 `u8`」不是
/// 有效值，因此**构造**这个引用的瞬间处于灰区（这也是 `MaybeUninit` 存在的原因）。
/// 本 crate 的处置是：把这条路限制在**一个**函数里、把三条不变式写在调用点上，并用
/// 「设备只写、返回前不读」把实际行为约束成「不会有未初始化字节被当作 `u8` 读取」。
/// 若上游将来提供「未初始化读缓冲」的签名，本函数应当被替换掉。
unsafe fn assume_init_bytes_mut_(target: &mut [MaybeUninit<u8>]) -> &mut [u8] {
    // SAFETY: 见本函数文档的三条要求；`MaybeUninit<u8>` 与 `u8` 布局相同、对齐相同。
    unsafe { core::slice::from_raw_parts_mut(target.as_mut_ptr().cast::<u8>(), target.len()) }
}

/// 等一次设备读（`Pending` 即挂起，由设备的 waker 唤醒）。
async fn poll_read_<R>(dev: &mut R, buf: &mut [u8]) -> io::Result<usize>
where
    R: AsyncRead + Unpin,
{
    poll_fn(|cx| Pin::new(&mut *dev).poll_read(cx, buf)).await
}

/// 等一次设备写（`Pending` 即挂起，由设备的 waker 唤醒）。
async fn poll_write_<W>(dev: &mut W, buf: &[u8]) -> io::Result<usize>
where
    W: AsyncWrite + Unpin,
{
    poll_fn(|cx| Pin::new(&mut *dev).poll_write(cx, buf)).await
}

#[cfg(test)]
mod tests_ {
    use futures_lite::io::Cursor;

    use super::*;

    /// 读适配的零拷贝往返与 EOF 语义。
    /// - 测试目标：`ReadAsInput` 把设备字节**直接**写进调用方的 `MaybeUninit` 窗口；
    ///   设备读到末尾时给出带 `Closing` 标签的错误（而不是 `Left(0)`）。
    /// - 测试手段：用 `Cursor<Vec<u8>>` 作读设备（futures-lite 已为它实现 `AsyncRead`），
    ///   目标窗口 8 字节；先读一次拿 3 字节，再读一次拿 EOF。
    /// - 判定标准：第一次 `Left(3)` 且窗口前 3 字节等于源数据；第二次 `Right` 且标签为
    ///   `ReadErrTag::Closing`。
    #[test]
    fn read_as_input_roundtrip_and_eof_() {
        futures_lite::future::block_on(async {
            let mut dev = Cursor::new(vec![7u8, 8, 9]);
            let mut input = ReadAsInput::new(&mut dev);
            let mut target = [MaybeUninit::<u8>::uninit(); 8];

            let x = input.read_async(&mut target).await;
            assert_eq!(x.pick_left(), Some(3usize), "应读到 3 字节");
            // SAFETY: 上面已确认前 3 个字节被初始化。
            let got: Vec<u8> = target[..3]
                .iter()
                .map(|m| unsafe { m.assume_init() })
                .collect();
            assert_eq!(got, vec![7u8, 8, 9]);

            let x = input.read_async(&mut target).await;
            let err = x.pick_right().expect("读到末尾应给出错误");
            assert_eq!(err.tag(), ReadErrTag::Closing, "EOF 应带终止性标签 Closing");
        });
    }

    /// 写适配的零拷贝往返与「非空缓冲写 0」的违约翻译。
    /// - 测试目标：`WriteAsOutput` 把源切片原样写给设备；设备在非空缓冲上返回 0 时
    ///   翻成 `WriteZero` 错误，避免上游空转。
    /// - 测试手段：用 `Cursor<Vec<u8>>` 作写设备写 2 字节并取回内容；再用一个「永远
    ///   只写 0 字节」的设备验证错误翻译。
    /// - 判定标准：设备内容等于源字节；零进展设备得到 `WriteErrTag::Propagated` 且错误
    ///   种类为 `WriteZero`。
    #[test]
    fn write_as_output_translates_zero_write_() {
        use abs_buff::error::TrErrorWrapper;

        futures_lite::future::block_on(async {
            let mut dev = Cursor::new(Vec::<u8>::new());
            let src = [MaybeUninit::new(1u8), MaybeUninit::new(2u8)];
            {
                let mut output = WriteAsOutput::new(&mut dev);
                let x = output.write_async(&src).await;
                assert_eq!(x.pick_left(), Some(2usize), "应写出 2 字节");
            }
            assert_eq!(dev.into_inner(), vec![1u8, 2], "设备应收到源字节");

            // 违约设备：非空缓冲上恒返回 `Ok(0)`。
            let mut bad = ZeroWriter;
            let mut output = WriteAsOutput::new(&mut bad);
            let x = output.write_async(&src).await;
            let err = x.pick_right().expect("非空缓冲写 0 应翻成错误");
            assert_eq!(err.tag(), WriteErrTag::Propagated);
            assert_eq!(
                err.propagated_err().expect("应带内层 io 错误").kind(),
                io::ErrorKind::WriteZero
            );
        });
    }

    /// 恒返回「非空缓冲写 0」的违约写设备，用于验证错误翻译。
    struct ZeroWriter;

    impl AsyncWrite for ZeroWriter {
        fn poll_write(
            self: Pin<&mut Self>,
            _cx: &mut core::task::Context<'_>,
            _buf: &[u8],
        ) -> core::task::Poll<io::Result<usize>> {
            core::task::Poll::Ready(Result::Ok(0))
        }

        fn poll_flush(
            self: Pin<&mut Self>,
            _cx: &mut core::task::Context<'_>,
        ) -> core::task::Poll<io::Result<()>> {
            core::task::Poll::Ready(Result::Ok(()))
        }

        fn poll_close(
            self: Pin<&mut Self>,
            _cx: &mut core::task::Context<'_>,
        ) -> core::task::Poll<io::Result<()>> {
            core::task::Poll::Ready(Result::Ok(()))
        }
    }
}
