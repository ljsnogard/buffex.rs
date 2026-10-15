# buffex_iroh

Thin `TrBuffRead` / `TrBuffWrite` adapters over iroh QUIC streams.

iroh's `RecvStream` / `SendStream` do not expose their internal buffers as
abs_buff segments, so this crate places a `buffex::ring` ring between the user
and the QUIC stream:

- `IrohReader` wraps a `RecvStream` as the ring's **active producer**; the user
  consumes the buffered data through `TrBuffRead` / `TrBuffTryRead`.
- `IrohWriter` wraps a `SendStream` as the ring's **active consumer**; the user
  produces data through `TrBuffWrite` / `TrBuffTryWrite`.

## Implementation: a thin wrapper

iroh's `RecvStream` / `SendStream` are tokio `AsyncRead` / `AsyncWrite` devices
(the `SendStream`'s `poll_shutdown` *is* `finish()`), so this crate no longer
implements the "ring ↔ device" pump itself: it hands the device to the generic
[`buffex_tokio_adapt::BuffRead`] / [`buffex_tokio_adapt::BuffWrite`], which are
already written against `buffex::ring`, and only forwards the `abs_buff` read /
write traits plus iroh-shaped construct / finish entry points.

Earlier revisions depended on the now-removed `buffex::circular_buff`
(`CircularBuffBuilder` / `DevProducer` / `DevConsumer`).

## No background task (no spawn)

This crate **does not spawn any task**: data movement is driven on demand by the
caller.

- **Read**: network data is pulled only when the user operates — `try_read`
  drives one pull round first (non-blocking), `read_async` keeps driving while
  it waits for the ring to fill. Network errors are reported via `take_error`;
  EOF / errors are surfaced as `Closing` on the read side.
- **Write**: `try_write` / `write_async` push one round "ring → device" before
  lending a segment; a segment's drop only **commits into the ring**. Finishing
  therefore requires awaiting `close_async()`: it flushes the remainder and
  `shutdown`s the device, i.e. `finish()`es the stream so the peer sees EOF.

Because there is no background task, the caller has to keep driving, and all
operations must run inside an active Tokio runtime context (the QUIC machinery
is driven by the runtime). Note also that `Endpoint::close()` does not wait for
data already handed to the QUIC layer to reach the peer — keep the connection
alive until the peer has consumed the stream.

## Example

```rust,ignore
use abs_buff::{Demand, TrBuffRead, TrBuffWrite};
use buffex_iroh::{IrohReader, IrohWriter};
use iroh::endpoint::{RecvStream, SendStream};

async fn copy_recv_to_send(recv: RecvStream, send: SendStream) {
    let mut reader = IrohReader::try_new(recv, 8192).await.expect("valid capacity");
    let mut writer = IrohWriter::try_new(send, 8192).await.expect("valid capacity");

    loop {
        let demand = Demand::no_more_than(4096);
        let Some(mut rseg) = TrBuffRead::read_async(&mut reader, &demand).await.pick_left() else {
            break;
        };
        // move data from rseg to writer ...
        drop(rseg);
    }
    // 收尾：把环里残留的数据冲刷出去并 finish() 流。
    let _ = writer.close_async().await;
}
```

[`buffex_tokio_adapt::BuffRead`]: ../buffex_tokio_adapt/src/read_.rs
[`buffex_tokio_adapt::BuffWrite`]: ../buffex_tokio_adapt/src/write_.rs
