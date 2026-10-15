// to enable no hand-written poll
#![feature(impl_trait_in_assoc_type)]
#![feature(impl_restriction)]
#![feature(unboxed_closures)]
#![feature(try_trait_v2)]
#![feature(min_specialization)]
#![no_std]

// We always pull in `std` during tests, because it's just easier
// to write tests when you can assume you're on a capable platform
#[cfg(test)]
extern crate std;

pub use gen_mcf2::gen_may_cancel_future;

pub mod buffer;
pub mod error;
pub mod io;
pub mod pipelining;

mod demand_;
mod peeker_;
mod reader_;
mod slice_impl_;
mod writer_;

pub use demand_::{Demand, DiffT};
pub use peeker_::{TrBuffPeek, TrBuffTryPeek};
pub use reader_::{TrBuffRead, TrBuffTryRead};
/// 立即就绪的 `Future`，产出 `SomeOf<S, E>`；实现 `TrInput` / `TrOutput` 时
/// 可直接用它作为 `ReadAsync<'f>` / `WriteAsync<'f>` 的具体类型。
pub use slice_impl_::ReadySegm;
pub use writer_::{TrBuffTryWrite, TrBuffWrite};

pub mod x_deps {
    pub use abs_cancel;
    pub use anylr;
    pub use funty;
    pub use gen_mcf2;
}
