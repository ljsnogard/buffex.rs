use core::{
    borrow::{Borrow, BorrowMut},
    mem::MaybeUninit,
    slice,
};

#[repr(transparent)]
pub struct AsBuff<'a, T>(&'a [MaybeUninit<T>]);

impl<'a, T> AsBuff<'a, T> {
    pub const fn new(buff: &'a [MaybeUninit<T>]) -> Self {
        AsBuff(buff)
    }
}

impl<'a, T> core::ops::Deref for AsBuff<'a, T> {
    type Target = [MaybeUninit<T>];

    fn deref(&self) -> &Self::Target {
        self.0
    }
}

impl<'a, T> Borrow<[MaybeUninit<T>]> for AsBuff<'a, T> {
    fn borrow(&self) -> &[MaybeUninit<T>] {
        self.0
    }
}

// -- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
// Conversion for AsBuff<'a, T>
// -- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

impl<'a, T, const N: usize> From<&'a [T; N]> for AsBuff<'a, T> {
    fn from(value: &'a [T; N]) -> Self {
        let data = value.as_ptr() as *const MaybeUninit<T>;
        let len = value.len();
        AsBuff::new(unsafe { slice::from_raw_parts(data, len) })
    }
}

impl<'a, T> From<&'a [T]> for AsBuff<'a, T> {
    fn from(value: &'a [T]) -> Self {
        let data = value.as_ptr() as *const MaybeUninit<T>;
        let len = value.len();
        AsBuff::new(unsafe { slice::from_raw_parts(data, len) })
    }
}

impl<'a, T, const N: usize> From<&'a [MaybeUninit<T>; N]> for AsBuff<'a, T> {
    fn from(value: &'a [MaybeUninit<T>; N]) -> Self {
        let data = value.as_ptr();
        let len = value.len();
        AsBuff::new(unsafe { slice::from_raw_parts(data, len) })
    }
}

impl<'a, T, const N: usize> From<&'a MaybeUninit<[T; N]>> for AsBuff<'a, T> {
    fn from(value: &'a MaybeUninit<[T; N]>) -> Self {
        let data = unsafe { &value.assume_init_ref()[0] } as *const T as *const MaybeUninit<T>;
        let len = N;
        AsBuff::new(unsafe { slice::from_raw_parts(data, len) })
    }
}

impl<'a, T> From<&'a [MaybeUninit<T>]> for AsBuff<'a, T> {
    fn from(value: &'a [MaybeUninit<T>]) -> Self {
        AsBuff::new(value)
    }
}

// -- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
// AsBuffMut
// -- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

#[repr(transparent)]
pub struct AsBuffMut<'a, T>(&'a mut [MaybeUninit<T>]);

impl<'a, T> AsBuffMut<'a, T> {
    pub const fn new(buff: &'a mut [MaybeUninit<T>]) -> Self {
        AsBuffMut(buff)
    }
}

impl<'a, T> core::ops::Deref for AsBuffMut<'a, T> {
    type Target = [MaybeUninit<T>];

    fn deref(&self) -> &Self::Target {
        self.0
    }
}

impl<'a, T> core::ops::DerefMut for AsBuffMut<'a, T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.0
    }
}

impl<'a, T> Borrow<[MaybeUninit<T>]> for AsBuffMut<'a, T> {
    fn borrow(&self) -> &[MaybeUninit<T>] {
        self.0
    }
}

impl<'a, T> BorrowMut<[MaybeUninit<T>]> for AsBuffMut<'a, T> {
    fn borrow_mut(&mut self) -> &mut [MaybeUninit<T>] {
        self.0
    }
}

// -- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
// Conversion for AsBuffMut<'a, T>
// -- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

impl<'a, T, const N: usize> From<&'a mut [T; N]> for AsBuffMut<'a, T> {
    fn from(value: &'a mut [T; N]) -> Self {
        let data = value.as_mut_ptr() as *mut MaybeUninit<T>;
        let len = value.len();
        AsBuffMut::new(unsafe { slice::from_raw_parts_mut(data, len) })
    }
}

impl<'a, T> From<&'a mut [T]> for AsBuffMut<'a, T> {
    fn from(value: &'a mut [T]) -> Self {
        let data = value.as_mut_ptr() as *mut MaybeUninit<T>;
        let len = value.len();
        AsBuffMut::new(unsafe { slice::from_raw_parts_mut(data, len) })
    }
}

impl<'a, T, const N: usize> From<&'a mut [MaybeUninit<T>; N]> for AsBuffMut<'a, T> {
    fn from(value: &'a mut [MaybeUninit<T>; N]) -> Self {
        let data = value.as_mut_ptr();
        let len = value.len();
        AsBuffMut::new(unsafe { slice::from_raw_parts_mut(data, len) })
    }
}

impl<'a, T, const N: usize> From<&'a mut MaybeUninit<[T; N]>> for AsBuffMut<'a, T> {
    fn from(value: &'a mut MaybeUninit<[T; N]>) -> Self {
        let data = unsafe { &mut value.assume_init_mut()[0] } as *mut T as *mut MaybeUninit<T>;
        let len = N;
        AsBuffMut::new(unsafe { slice::from_raw_parts_mut(data, len) })
    }
}

impl<'a, T> From<&'a mut [MaybeUninit<T>]> for AsBuffMut<'a, T> {
    fn from(value: &'a mut [MaybeUninit<T>]) -> Self {
        AsBuffMut::new(value)
    }
}
