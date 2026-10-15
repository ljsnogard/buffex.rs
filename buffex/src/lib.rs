// to enable no hand-written poll

#![feature(impl_trait_in_assoc_type)]
// #![feature(impl_restriction)]
#![feature(unboxed_closures)]

#![no_std]
// #![cfg_attr(test, feature(try_trait_v2))]

// We always pull in `std` during tests, because it's just easier
// to write tests when you can assume you're on a capable platform
#[cfg(test)]
extern crate std;

// `channels` 用 `alloc` 侧容器承载**等待队列**（`atomic_sync` 的协作式锁 / 自旋
// 锁内部会分配等待节点），消息本身仍存放在 `circular_buff` 的环形缓冲里。
// `alloc` 只是语言层面的 crate，最终是否需要分配器由链接方决定。
extern crate alloc;

// pub mod channels;
// pub mod circular_buff;
pub mod ring;

#[cfg(test)]
mod test_support_;

pub mod x_deps {
    pub use abs_async_iter;
    pub use abs_buff;
    pub use abs_buff::x_deps::{abs_cancel, anylr};
    pub use atomic_sync;
    pub use atomic_sync::x_deps::{abs_sync, atomex};
    pub use atomex::x_deps::funty;
}
