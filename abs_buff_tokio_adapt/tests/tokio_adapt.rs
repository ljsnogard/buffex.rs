//! `ReadAsInput` / `WriteAsOutput` 的单元测试与真实 TCP 回环测试。

use std::{mem::MaybeUninit, pin::Pin};

use abs_buff::{
    Demand,
    buffer::{SegmMut, SegmReclaim, SegmRef},
    x_deps::abs_cancel::{NonCancellableToken, TrMayCancel},
};
use abs_buff_testkit::read_initialized;
use abs_buff_tokio_adapt::{ReadAsInput, WriteAsOutput};
use tokio::{
    io::AsyncReadExt,
    net::{TcpListener, TcpStream},
};


/// 单元测试：`ReadAsInput` 能把 `AsyncRead` 的数据读入 `SegmMut`。
#[tokio::test]
async fn read_as_input_fills_segment() {
    const ARR_SIZE: usize = 8;
    let mut storage = [MaybeUninit::<u8>::uninit(); ARR_SIZE];
    let mut consumed = 0usize;
    let mut segm = SegmMut::new(
        &mut storage[..],
        SegmReclaim::new(Pin::new(&mut consumed)),
    );

    let mut data: &[u8] = b"hello";
    let mut input = ReadAsInput::new(&mut data);

    let res = segm
        .move_items_from_input_async(&mut input, &Demand::less_than(ARR_SIZE))
        .may_cancel_with(NonCancellableToken::new())
        .await;

    let n = res.pick_left().expect("read should succeed");
    assert_eq!(n, 5);
    assert_eq!(segm.least_count(), 3);

    drop(segm);
    assert_eq!(consumed, 5);
    assert_eq!(read_initialized(&storage, n), b"hello");
}

/// 单元测试：`WriteAsOutput` 能把 `SegmRef` 中的数据写入 `AsyncWrite`。
#[tokio::test]
async fn write_as_output_drains_segment() {
    let mut data = [10u8, 20, 30, 40];
    let mut consumed = 0usize;
    let mut segm = SegmRef::new(
        data.as_mut_slice(),
        SegmReclaim::new(Pin::new(&mut consumed)),
    );

    let (mut writer, mut reader) = tokio::io::duplex(64);
    let mut output = WriteAsOutput::new(&mut writer);

    let res = segm
        .move_items_to_output_async(&mut output, &Demand::less_than(4))
        .may_cancel_with(NonCancellableToken::new())
        .await;

    let n = res.pick_left().expect("write should succeed");
    assert_eq!(n, 4);
    assert_eq!(segm.least_count(), 0);

    drop(segm);
    assert_eq!(consumed, 4);

    let mut buf = [0u8; 4];
    reader.read_exact(&mut buf).await.expect("read from duplex");
    assert_eq!(buf, [10, 20, 30, 40]);
}

/// 真实 TCP 回环：客户端用 `WriteAsOutput` 发送，服务端用 `ReadAsInput` 接收；
/// 随后服务端用 `WriteAsOutput` 回复，客户端用 `ReadAsInput` 读取。
#[tokio::test]
async fn real_tcp_roundtrip_with_adapters() {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("local addr");

    let server = async move {
        let (mut stream, _) = listener.accept().await.expect("accept");

        // 接收请求：ReadAsInput -> SegmMut
        let mut request_storage = [MaybeUninit::<u8>::uninit(); 1024];
        let mut request_consumed = 0usize;
        let mut request_segm = SegmMut::new(
            &mut request_storage[..],
            SegmReclaim::new(Pin::new(&mut request_consumed)),
        );
        let mut input = ReadAsInput::new(&mut stream);
        let res = request_segm
            .move_items_from_input_async(&mut input, &Demand::less_than(4))
            .may_cancel_with(NonCancellableToken::new())
            .await;
        let n = res.pick_left().expect("server read request");
        drop(request_segm);
        let request = read_initialized(&request_storage, n);

        // 发送响应：SegmRef -> WriteAsOutput
        let mut response = request
            .iter()
            .map(|b| b.wrapping_add(1))
            .collect::<Vec<u8>>();
        let response_len = response.len();
        let mut response_consumed = 0usize;
        let mut response_segm = SegmRef::new(
            response.as_mut_slice(),
            SegmReclaim::new(Pin::new(&mut response_consumed)),
        );
        let mut output = WriteAsOutput::new(&mut stream);
        let res = response_segm
            .move_items_to_output_async(
                &mut output,
                &Demand::less_than(response_len),
            )
            .may_cancel_with(NonCancellableToken::new())
            .await;
        let written = res.pick_left().expect("server write response");
        assert_eq!(written, response_len);
        drop(response_segm);

        request
    };

    let client = async move {
        let mut client = TcpStream::connect(addr).await.expect("connect");

        // 发送请求：SegmRef -> WriteAsOutput
        let payload = b"ping".to_vec();
        let payload_len = payload.len();
        let mut send_consumed = 0usize;
        let mut send_segm = SegmRef::new(
            &payload[..],
            SegmReclaim::new(Pin::new(&mut send_consumed)),
        );
        let mut send_output = WriteAsOutput::new(&mut client);
        let res = send_segm
            .move_items_to_output_async(
                &mut send_output,
                &Demand::less_than(payload_len),
            )
            .may_cancel_with(NonCancellableToken::new())
            .await;
        let sent = res.pick_left().expect("client send");
        assert_eq!(sent, payload_len);
        drop(send_segm);

        // 接收响应：ReadAsInput -> SegmMut
        let mut response_storage = [MaybeUninit::<u8>::uninit(); 1024];
        let mut response_consumed = 0usize;
        let mut response_segm = SegmMut::new(
            &mut response_storage[..],
            SegmReclaim::new(Pin::new(&mut response_consumed)),
        );
        let mut response_input = ReadAsInput::new(&mut client);
        let res = response_segm
            .move_items_from_input_async(
                &mut response_input,
                &Demand::less_than(4),
            )
            .may_cancel_with(NonCancellableToken::new())
            .await;
        let n = res.pick_left().expect("client read response");
        drop(response_segm);
        let response = read_initialized(&response_storage, n);

        assert_eq!(response, b"qjoh");
        response
    };

    let (server_request, client_response) = tokio::join!(server, client);
    assert_eq!(server_request, b"ping");
    assert_eq!(client_response, b"qjoh");
}

/// `TrInput` 协议：设备读到 0（EOF）时不允许返回「0 个且无错误」。
/// - 测试目标：`ReadAsInput` 在底层 `AsyncRead` 结束时，返回的结果必须满足
///   `abs_buff` 的输入搬移契约（`assert!(cc > 0 || x.contains_right())`，提交
///   `e4092c8`）——即不能是裸的「0 个」。
/// - 测试手段：用 `tokio::io::empty()`（立即 EOF）当设备，把 `ReadAsInput` 接到一个
///   可写段上跑 `move_items_from_input_async`；同时直接观察 `ReadAsInput::read_async`
///   的返回值。
/// - 判断：搬移不 panic（当前实现会触发上游断言），且 `read_async` 的结果不是裸的
///   `Left(0)`。
#[tokio::test]
async fn read_as_input_reports_eof_with_error() {
    let mut dev = tokio::io::empty();
    let mut input = ReadAsInput::new(&mut dev);
    let mut target = [MaybeUninit::<u8>::uninit(); 8];

    // 直接看裸结果：EOF 时不能是 `Left(0)`。
    let raw = input.read_async(&mut target).await;
    assert!(
        !matches!(raw.as_ref().pick_left(), Option::Some(&0)) || raw.as_ref().pick_right().is_some(),
        "TrInput 协议要求 EOF 以错误表达，不能返回裸的 0：{raw:?}"
    );

    // 再跑一遍段级搬移：旧实现会在这里触发上游断言。
    let mut storage = [MaybeUninit::<u8>::uninit(); 8];
    let mut consumed = 0usize;
    let mut segm = SegmMut::new(
        &mut storage[..],
        SegmReclaim::new(Pin::new(&mut consumed)),
    );
    let mut input2 = ReadAsInput::new(&mut dev);
    let res = segm
        .move_items_from_input_async(&mut input2, &Demand::less_than(8))
        .may_cancel_with(NonCancellableToken::new())
        .await;
    assert!(
        res.as_ref().pick_right().is_some(),
        "EOF 应当以错误形式返回：{res:?}"
    );
}
