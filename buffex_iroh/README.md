# buffex_iroh

Thin `TrBuffRead` / `TrBuffWrite` adapters over iroh QUIC streams.

iroh's `RecvStream` / `SendStream` do not expose their internal buffers as
abs_buff segments, so this crate places a `buffex::circular_buff` buffer
between the user and the QUIC stream:

- `IrohReader` wraps a `RecvStream` as the buffer's **active producer**; the
  user consumes the buffered data through `TrBuffRead` / `TrBuffTryRead`.
- `IrohWriter` wraps a `SendStream` as the buffer's **active consumer**; the
  user produces data through `TrBuffWrite` / `TrBuffTryWrite`.

## No background task (no spawn)

This crate **does not spawn any task**: data movement is driven by
`circular_buff`'s synchronous hook pumps.

- **Write**: the segment drop commit pumps the data into the QUIC stream
  synchronously (blocking write — data is never lost); `shutdown` flushes the
  remainder and `finish()`es the stream.
- **Read**: network data is pulled only when the user operates — when the
  producer end is active, `try_read` / `read_async` automatically drive one
  pull round first (non-blocking); `read_async` loops drive + yield until
  data / EOF / error.
- Network errors are reported via `take_error`; EOF / errors are surfaced as
  `Closing` on the read side.

Note: because there is no background task, **the write path blocks the current
thread until the network write completes**, and all operations must run inside
an active Tokio runtime context (the QUIC machinery is driven by the runtime).

## Example

```rust,ignore
use abs_buff::{Demand, TrBuffRead, TrBuffWrite};
use buffex_iroh::{IrohReader, IrohWriter};
use iroh::endpoint::{RecvStream, SendStream};

async fn copy_recv_to_send(mut recv: RecvStream, mut send: SendStream) {
    let mut reader = IrohReader::new(recv, 8192);
    let mut writer = IrohWriter::new(send, 8192);

    loop {
        let Some(mut rseg) = TrBuffRead::read_async(&mut reader, &Demand::less_than(4096)).await.pick_left() else {
            break;
        };
        // move data from rseg to writer ...
        drop(rseg);
    }
}
```
