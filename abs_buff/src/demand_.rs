use core::{
    cmp,
    ops::{Bound, RangeBounds},
};

use funty::{Integral, Unsigned};

/// `Demand` 中「差值 / 区间长度」所在的类型：即 `abs_diff` 的返回类型。
///
/// 单独起名是为了在类型层面把「一个取值」与「两个取值的差」区分开：`BaseAndLen`
/// 的第一个字段是取值，第二个字段是差值。`Demand` 只接受无符号整数，因此对实际
/// 用到的 `T`，`DiffT<T>` 与 `T` 同型。
///
/// ## Example
/// ```
/// use abs_buff::{Demand, DiffT};
///
/// // {3, ..., 8} 共 6 个取值。
/// let count: DiffT<usize> = Demand::between(3usize, 8).len();
/// assert_eq!(count, 6);
/// ```
pub type DiffT<T> = <T as Integral>::Unsigned;

/// 描述一次操作可接受的「数量」范围。
///
/// 取值集合一律是左闭右开区间或单边约束，`min()` / `max()` 给出**含端点**的约束边界：
///
/// * [`Demand::exactly`]：恰好 n 个；
/// * [`Demand::no_more_than`]：最多 n 个（含端点）；
/// * [`Demand::at_least`]：至少 n 个；
/// * [`Demand::between`]：a 到 b（两端都含）；
/// * [`Demand::less_than`]：严格小于 n 个。
///
/// 取值集合允许是**空集**（目前只有 `less_than(0)` 会构造出它）：空集的 `min()` /
/// `max()` 都返回 `None`、`len()` 返回 0、[`Demand::is_empty`] 返回 `true`、与任何
/// 需求求交都返回 `None`。消费侧若要把「空需求」当成「搬 0 个」，必须显式调用
/// `is_empty()`——`min()` / `max()` 的 `None` 承载不了这一区别。
#[derive(Clone, Debug)]
pub struct Demand<T = usize>(DemandRange<T>)
where
    T: Unsigned + Integral<Unsigned = T>;

impl<T> Demand<T>
where
    T: Unsigned + Integral<Unsigned = T>,
{
    /// 由 [`RangeBounds`] 构造需求；空区间或边界溢出时返回 `Err`（原样带回边界引用）。
    ///
    /// 区间的**开闭语义照 `RangeBounds` 原样**，映射到内部表示时上界会换算成「含端点的
    /// 最大取值」：
    ///
    /// * 无上界（`..`、`5..`）映射为 [`Demand::at_least`]，不再借用 `T::MAX` 充数，
    ///   因此也不会出现 `MAX + 1` 这样的上溢；
    /// * `5..10` 映射为 `{5, ..., 9}`，`5..=10` 映射为 `{5, ..., 10}`；
    /// * 空区间（`1..1`、`..0`）与边界溢出（起点为 `Excluded(T::MAX)`）返回 `Err`。
    ///
    /// ## Example
    /// ```
    /// use abs_buff::Demand;
    ///
    /// // 左闭右开：5..10 → {5, ..., 9}
    /// let a = Demand::try_from_usize_range(&(5usize..10)).unwrap();
    /// assert_eq!(a.min(), Option::Some(5));
    /// assert_eq!(a.max(), Option::Some(9));
    /// assert_eq!(a.len(), 5);
    ///
    /// // 无上界 → {5, ...}
    /// let b = Demand::try_from_usize_range(&(5usize..)).unwrap();
    /// assert_eq!(b.max(), Option::None);
    /// ```
    pub fn try_from_usize_range(
        range: &impl RangeBounds<T>,
    ) -> Result<Self, (Bound<&T>, Bound<&T>)> {
        use Bound::*;

        let start_bound = range.start_bound();
        let end_bound = range.end_bound();

        // 起点换算成「含端点的下界」；`Excluded(MAX)` 上溢 → `None` → Err。
        // 无下界隐含从 0 起算，因此取 `T::ZERO` 而不是 `T::MIN`。
        let start = match start_bound {
            Included(&v) => Option::Some(v),
            Excluded(&v) => v.checked_add(T::ONE),
            Unbounded => Option::Some(T::ZERO),
        };
        // 上界换算成「含端点的最大取值」：
        // * `None`            —— 无上界（映射为 `AtLeast`）；
        // * `Some(None)`      —— 空区间（例如 `..0`，`Excluded(0)` 减不出值）；
        // * `Some(Some(u))`   —— 含端点的最大取值 u。
        let last: Option<Option<T>> = match end_bound {
            Included(&v) => Option::Some(Option::Some(v)),
            Excluded(&v) => Option::Some(v.checked_sub(T::ONE)),
            Unbounded => Option::None,
        };

        match (start, last) {
            (Option::Some(l), Option::Some(Option::Some(u))) if l <= u => {
                Ok(Demand::between(l, u))
            }
            (Option::Some(l), Option::None) => Ok(Demand::at_least(l)),
            _ => Err((start_bound, end_bound)),
        }
    }

    /// 构造「只允许一个取值」的需求：集合 `{val}`。
    ///
    /// 内部表示为 `[val, val + 1)`（起点 + 长度 1），因此 `val` 取到 `usize::MAX`
    /// 也能精确表示——这正是用「起点 + 长度」取代「上下限」的动机。
    ///
    /// ## Example
    /// ```
    /// use abs_buff::Demand;
    ///
    /// let a = Demand::exactly(2usize);
    /// assert_eq!(a.min(), Option::Some(2));
    /// assert_eq!(a.max(), Option::Some(2));
    /// assert_eq!(a.len(), 1);
    ///
    /// // 取值上限本身同样能精确表示
    /// let m = Demand::exactly(usize::MAX);
    /// assert_eq!(m.min(), Option::Some(usize::MAX));
    /// assert_eq!(m.max(), Option::Some(usize::MAX));
    /// assert_eq!(m.len(), 1);
    /// ```
    pub const fn exactly(val: T) -> Self {
        Demand(DemandRange::BaseAndLen(val, T::ONE))
    }

    /// 构造「取值落在 `[min(a, b), max(a, b)]`」的需求：**两端都含**，参数顺序无关。
    ///
    /// `a == b` 时退化为只含一个取值的需求（等价于 [`Demand::exactly`]）。
    ///
    /// `{T::MIN, ..., T::MAX}`（即全体取值）的开区间上界是 `T::MAX + 1`，长度装不进
    /// 差值域，因此退化为 [`Demand::at_least`]——两者本来就是同一个集合（见 dev-notes
    /// 的 D12：集合视角下 `max()` 为 `None`，但 `len()` 饱和为差值域上限）。
    ///
    /// ## Example
    /// ```
    /// use abs_buff::Demand;
    ///
    /// // 参数顺序无关
    /// let a = Demand::between(10usize, 1);
    /// assert_eq!(a.min(), Option::Some(1));
    /// assert_eq!(a.max(), Option::Some(10));
    /// assert_eq!(a.len(), 10);
    ///
    /// // 退化成一个取值
    /// let b = Demand::between(10usize, 10);
    /// assert_eq!(b.min(), Option::Some(10));
    /// assert_eq!(b.max(), Option::Some(10));
    /// assert_eq!(b.len(), 1);
    ///
    /// // 上探到取值上限：集合 {0, ..., MAX} 等价于"无上界"
    /// let c = Demand::between(0usize, usize::MAX);
    /// assert_eq!(c.min(), Option::Some(0));
    /// assert_eq!(c.max(), Option::None);
    /// ```
    pub fn between(a: T, b: T) -> Self {
        use DemandRange::*;

        let (lo, hi) = if a <= b { (a, b) } else { (b, a) };
        // `abs_diff` 天然避免 `hi - lo` 的下溢，并给出「差值域」上的长度。
        let diff = hi.abs_diff(lo);
        if diff < T::MAX {
            Demand(BaseAndLen(lo, diff + T::ONE))
        } else {
            // 只有 `{T::ZERO, ..., T::MAX}` 会走到这里：`diff + ONE` 恰好越过差值域。
            Demand(AtLeast(lo))
        }
    }

    /// 构造「至少 val」的需求：集合 `{val, val + 1, ...}`，无上界。
    ///
    /// ## Example
    /// ```
    /// use abs_buff::Demand;
    ///
    /// let a = Demand::at_least(2usize);
    /// assert_eq!(a.min(), Option::Some(2));
    /// assert_eq!(a.max(), Option::None);
    /// ```
    pub const fn at_least(val: T) -> Self {
        Demand(DemandRange::AtLeast(val))
    }

    /// 构造「最多 val」的需求：集合 `{0, 1, ..., val}`；上界**含端点**，下界无约束。
    ///
    /// 搬移/读写实现可以直接把它的 `max()` 当作"最多能搬多少"来用。
    ///
    /// 注意 `no_more_than(0)` 不是空集：它允许取值 0（`len() == 1`）。
    ///
    /// ## Example
    /// ```
    /// use abs_buff::Demand;
    ///
    /// let a = Demand::no_more_than(2usize);
    /// assert_eq!(a.min(), Option::None);
    /// assert_eq!(a.max(), Option::Some(2));
    /// assert_eq!(a.len(), 3);
    /// ```
    pub const fn no_more_than(val: T) -> Self {
        Demand(DemandRange::NoMoreThan(val))
    }

    /// 构造「严格小于 val」的需求：集合 `{0, 1, ..., val - 1}`。
    ///
    /// `val == 0` 时取值集合为空集（没有任何数量严格小于 0）：`min()` / `max()` 都是
    /// `None`、`len()` 为 0、[`Demand::is_empty`] 为 `true`。
    ///
    /// ## Example
    /// ```
    /// use abs_buff::Demand;
    ///
    /// let a = Demand::less_than(3usize);
    /// assert_eq!(a.min(), Option::None);
    /// assert_eq!(a.max(), Option::Some(2));
    /// assert_eq!(a.len(), 3);
    ///
    /// // `less_than(0)` 是空集
    /// let e = Demand::less_than(0usize);
    /// assert!(e.is_empty());
    /// assert_eq!(e.len(), 0);
    /// ```
    pub fn less_than(val: T) -> Self {
        if val > T::ZERO {
            Demand(DemandRange::NoMoreThan(val - T::ONE))
        } else {
            Demand(DemandRange::Empty)
        }
    }

    /// 取值集合的**含端点**最小值；`None` 表示「无下界」（隐含从 0 起算）或空集。
    ///
    /// 例如 `exactly(2)`、`between(1, 5)`、`at_least(3)` 返回各自的 `Some(下界)`，
    /// 而 `no_more_than(4)` 返回 `None`。
    pub const fn min(&self) -> Option<T> {
        use DemandRange::*;

        match self.0 {
            AtLeast(l) => Option::Some(l),
            BaseAndLen(l, _) => Option::Some(l),
            // `NoMoreThan` 无下界；`Empty` 没有任何取值。
            _ => Option::None,
        }
    }

    /// 取值集合的**含端点**最大值；`None` 表示「无上界」或空集。
    ///
    /// 注意这是含端点的最大取值，而不是开区间上界：`exactly(2)` 与 `no_more_than(2)`
    /// 都返回 `Some(2)`。
    pub fn max(&self) -> Option<T> {
        use DemandRange::*;

        match self.0 {
            NoMoreThan(u) => Option::Some(u),
            // `[b, b + l)` 的最大取值是 `b + (l - 1)`。`BaseAndLen` 的不变量保证
            // `l >= 1` 且 `b + (l - 1) <= T::MAX`（`exactly(T::MAX)` 正落在 `T::MAX`），
            // 因此 `checked_*` 不会在合法状态上失败，只把「越界」显式表达为 `None`。
            BaseAndLen(b, l) => l.checked_sub(T::ONE).and_then(|d| b.checked_add(d)),
            // `AtLeast` 无上界；`Empty` 没有任何取值。
            _ => Option::None,
        }
    }

    /// 取值集合是否为空集。
    ///
    /// 目前只有 [`Demand::less_than`] 的 `less_than(0)` 会构造出空集。`is_empty()` 与
    /// `len() == 0` 恒等价：`no_more_than(0)` 允许取值 0，**不是**空集。
    pub const fn is_empty(&self) -> bool {
        matches!(self.0, DemandRange::Empty)
    }

    /// 求两个需求的交集：同时被两者允许的取值集合；无交集返回 `None`。
    ///
    /// 两集合**恰好相接**时（例如 `{5}` 与 `{0, ..., 4}`）交集为空，返回 `None`；
    /// 任一操作数是空集时同样返回 `None`（空集与任何集合的交集都是空集），不会
    /// 返回「长度为 0 的区间」，也不会 panic。
    ///
    /// ## Example
    /// ```
    /// use abs_buff::Demand;
    ///
    /// // {3, ..., 8} ∩ {5, ..., 10} = {5, ..., 8}
    /// let a = Demand::between(3usize, 8);
    /// let b = Demand::between(5usize, 10);
    /// let c = a.compromise(&b).unwrap();
    /// assert_eq!(c.min(), Option::Some(5));
    /// assert_eq!(c.max(), Option::Some(8));
    /// assert_eq!(c.len(), 4);
    ///
    /// // {5} ∩ {0, ..., 4} = ∅：恰好相接
    /// let e = Demand::exactly(5usize);
    /// let u = Demand::no_more_than(4usize);
    /// assert!(e.compromise(&u).is_none());
    ///
    /// // ∅ ∩ 任何需求 = ∅
    /// assert!(Demand::less_than(0usize).compromise(&u).is_none());
    /// ```
    pub fn compromise(&self, other: &Self) -> Option<Self> {
        use DemandRange::*;

        match (self.0, other.0) {
            // 空集与任何集合的交集都是空集。
            (Empty, _) | (_, Empty) => Option::None,

            // 无上界 + 无上界 → 取较大的下界
            (AtLeast(a), AtLeast(b)) => Option::Some(Demand::at_least(cmp::max(a, b))),

            // 无下界 + 无下界 → 取较小的上界
            (NoMoreThan(a), NoMoreThan(b)) => {
                Option::Some(Demand::no_more_than(cmp::min(a, b)))
            }

            // `{a, ...} ∩ {0, ..., b} = {a, ..., b}`（`a <= b` 才非空）。两种参数顺序
            // 绑定相同，合并为一个分支。长度 `b - a + 1` 只有 `a == 0 && b == MAX` 会
            // 溢出，而那个集合 `{0, ..., MAX}` 与"无上界"是同一个集合。
            (AtLeast(a), NoMoreThan(b)) | (NoMoreThan(b), AtLeast(a)) if a <= b => {
                if a == T::ZERO && b == T::MAX {
                    Option::Some(Demand(AtLeast(T::ZERO)))
                } else {
                    Option::Some(Demand(BaseAndLen(a, b.abs_diff(a) + T::ONE)))
                }
            }

            // `{a, ...} ∩ [c, c + d)`：同样把两种参数顺序合成一个分支。
            // case 分析集中在一处，且只在 `a > c` 时才做减法，
            // 因此不会像 `c - a` 那样在无符号类型上下溢。
            (AtLeast(a), BaseAndLen(c, d)) | (BaseAndLen(c, d), AtLeast(a)) => {
                if a <= c {
                    // 右侧 `[c, c + d)` 整体落在 `{a, ...}` 内。
                    Option::Some(Demand(BaseAndLen(c, d)))
                } else {
                    // 交集是 `[a, c + d)`，长度 = `d - (a - c)`。
                    let offset = a.abs_diff(c);
                    if offset < d {
                        Option::Some(Demand(BaseAndLen(a, d - offset)))
                    } else {
                        // `a >= c + d`：无交集。
                        Option::None
                    }
                }
            }

            // `{0, ..., b} ∩ [c, c + d) = [c, min(c + d, b + 1))`（`c <= b` 才非空）。
            // 长度用饱和加法：`b == MAX && c == 0` 时真实长度 `MAX + 1` 无法表示，
            // 但随后与 `d` 取小，结果仍然正确。
            (NoMoreThan(b), BaseAndLen(c, d)) if c <= b => {
                Option::Some(Demand(BaseAndLen(
                    c,
                    cmp::min(d, b.abs_diff(c).saturating_add(T::ONE)),
                )))
            }
            (BaseAndLen(c, d), NoMoreThan(b)) if c <= b => {
                Option::Some(Demand(BaseAndLen(
                    c,
                    cmp::min(d, b.abs_diff(c).saturating_add(T::ONE)),
                )))
            }

            // 半开区间 + 半开区间
            (BaseAndLen(a, b), BaseAndLen(c, d)) => {
                // lhs: [a, a + b)
                // rhs: [c, c + d)
                // 不计算 a + b / c + d，避免上溢。
                let (base, len) = if a >= c {
                    // 交集起点是 a。
                    // 需要 a 落在 rhs 内：a < c + d
                    // 等价于 a - c < d，且 a >= c 保证 a - c 不会下溢。
                    let offset = a.abs_diff(c);
                    if offset >= d {
                        return Option::None;
                    }

                    // rhs 从 a 开始还剩 d - (a - c) 个元素。
                    (a, cmp::min(b, d - offset))
                } else {
                    // 交集起点是 c。
                    // 需要 c 落在 lhs 内：c < a + b
                    // 等价于 c - a < b，且 c > a 保证 c - a 不会下溢。
                    let offset = c.abs_diff(a);
                    if offset >= b {
                        return Option::None;
                    }
                    // lhs 从 c 开始还剩 b - (c - a) 个元素。
                    (c, cmp::min(d, b - offset))
                };

                // `BaseAndLen` 的不变量保证 `b, d >= 1`，且上面的 `< d` / `< b` 判断
                // 保证差值后仍 `>= 1`，因此这里不会构造出长度为 0 的区间。
                Option::Some(Demand(BaseAndLen(base, len)))
            }

            // 无下界/无上界/空集的各种组合都已在上面覆盖，其余情况无交集
            // （`NoMoreThan ∩ BaseAndLen` 中 `c > b` 的两种顺序落在这里）。
            _ => Option::None,
        }
    }
}

impl<T> Demand<T>
where
    T: Unsigned + Integral<Unsigned = T>,
    DiffT<T>: Into<usize>,
{
    /// 允许的取值个数（取值集合的元素个数）。
    ///
    /// 返回的是「差值域」上的个数：`exactly(2)` → 1，`no_more_than(2)` → 3，
    /// `between(1, 100)` → 100，空集 → 0。当集合的元素个数超出差值域时按差值域上限
    /// 饱和：`at_least(5)` 的元素个数是 `MAX - 5 + 1`（`MAX` 取差值域上限），
    /// `no_more_than(T::MAX)` 的 `T::MAX + 1` 个取值饱和为 `T::MAX`。
    ///
    /// 注意 `at_least(T::MAX)` 只含一个取值 `{T::MAX}`，因此返回 1。
    ///
    /// 本方法仅当差值域可以无损转入 `usize` 时才提供（例如 `u8` / `u16` / `usize`），
    /// 以便调用方直接把它当 `usize` 使用。
    pub fn len(&self) -> DiffT<T> {
        use DemandRange::*;

        match self.0 {
            // {l, ..., MAX}：个数 = MAX - l + 1，l == 0 时上溢 → 饱和
            AtLeast(l) =>
                if l > T::ZERO {
                    T::MAX.abs_diff(l) + T::ONE
                } else {
                    T::MAX
                },
            // {0, ..., u}：个数 = u + 1，u == MAX 时上溢 → 饱和
            NoMoreThan(u) =>
                if u < T::MAX {
                    u.abs_diff(T::ZERO) + T::ONE
                } else {
                    T::MAX
                },
            // [b, b + l)：个数就是 l
            BaseAndLen(_, l) => l,
            // 空集：一个取值都没有
            Empty => T::ZERO,
        }
    }
}

/// 取值区间：表示一组被允许的「数量」，一律是左闭右开区间语义下的集合。
///
/// 区间不直接存上下限，而是「起点 + 长度」或单边约束。原因：`exactly(usize::MAX)`
/// 这类集合的开区间上界是 `usize::MAX + 1`，用 `usize` 存不下；改成「起点 + 长度」后
/// `{usize::MAX}` 就是 `(usize::MAX, 1)`，可以精确表示。
#[derive(Clone, Copy, Debug)]
pub(crate) enum DemandRange<T>
where
    T: Unsigned + Integral<Unsigned = T>,
{
    /// 空集：不含任何取值；`min()` / `max()` 都为 `None`。
    ///
    /// 单独列一个变体（而不是用「长度为 0 的 `BaseAndLen`」充数）是为了保住
    /// `BaseAndLen` 的 `len >= 1` 不变量，并让空集的 `min()` 也如实返回 `None`。
    Empty,

    /// `{l, l + 1, ..., T::MAX}`：只有下界，无上界
    AtLeast(T),

    /// `{0, 1, ..., u}`：只有上界（含端点），下界隐含为 0
    NoMoreThan(T),

    /// `[base, base + len)`：起点 + 长度；不变量 `len >= 1`，空集一律用 [`Self::Empty`]
    /// 表达，不构造长度为 0 的区间
    BaseAndLen(T, DiffT<T>),
}

#[cfg(test)]
mod try_from_usize_range_tests_ {
    use core::ops::{Bound, RangeBounds};

    use crate::Demand;

    /// 校验 `RangeBounds` → `Demand` 的映射：左闭右开照原样，上界换算成含端点最大值。
    /// - 测试目标：各类 Rust 区间都能映射成正确的取值集合；无上界区间映射为"无上界"，
    ///   而不是拿 `usize::MAX` 充数（后者会让 `{0, ...}` 在上界加一时溢出）。
    /// - 测试手段：对每个区间构造需求，读出 `(min, max, len)`，与手算结果比对；其中
    ///   `max` 是**含端点**最大取值，`None` 表示该侧无约束。
    /// - 判定标准：三元组逐一相等；无上界、上探 `usize::MAX` 的行分别给出文档化的结果。
    #[test]
    fn ranges_map_to_expected_sets() {
        let max = usize::MAX;

        /// 断言区间的映射结果 `(下界, 上界, 长度)`。
        fn check_<TyRange: RangeBounds<usize>>(
            range: TyRange,
            expect: (Option<usize>, Option<usize>, usize),
            name: &str,
        ) {
            let d = Demand::try_from_usize_range(&range)
                .unwrap_or_else(|_| panic!("{name}：应当能构造出需求"));
            assert_eq!(
                (d.min(), d.max(), d.len()),
                expect,
                "{name}: (min, max, len) 与预期不符"
            );
        }

        check_(.., (Option::Some(0), Option::None, max), "..");
        check_(5usize.., (Option::Some(5), Option::None, max - 4), "5..");
        check_(..10usize, (Option::Some(0), Option::Some(9), 10), "..10");
        check_(..=10usize, (Option::Some(0), Option::Some(10), 11), "..=10");
        check_(5usize..10, (Option::Some(5), Option::Some(9), 5), "5..10");
        check_(5usize..=10, (Option::Some(5), Option::Some(10), 6), "5..=10");
        check_(5usize..=5, (Option::Some(5), Option::Some(5), 1), "5..=5");
        check_(0usize..1, (Option::Some(0), Option::Some(0), 1), "0..1");
        check_(0usize..=0, (Option::Some(0), Option::Some(0), 1), "0..=0");
        check_(max..=max, (Option::Some(max), Option::Some(max), 1), "MAX..=MAX");
        check_(max.., (Option::Some(max), Option::None, 1), "MAX..");

        // `0..=MAX` 是唯一无法用"起点 + 长度"表示的集合（长度 MAX + 1），
        // 它与"无上界"等价，因此 max 为 None 而长度饱和到 MAX。
        let full = Demand::try_from_usize_range(&(0usize..=max)).unwrap();
        assert_eq!((full.min(), full.max()), (Option::Some(0), Option::None));
        assert_eq!(full.len(), max);
    }

    /// 空区间、非法边界与边界溢出必须返回 `Err`，并原样带回两端的引用。
    /// - 测试目标：`try_from_usize_range` 对空集合与溢出边界报错，而不是 panic 或静默构造
    ///   出空需求。
    /// - 测试手段：`1..1`、`5..5`、`..0`、`1..0`，以及自定义的
    ///   `(Excluded(10), Included(5))` 与起点 `Excluded(usize::MAX)`（加一溢出）。
    /// - 判定标准：全部返回 `Err`；`Err` 中带回的边界与输入一致。
    #[test]
    fn empty_or_overflowing_ranges_return_err() {
        assert!(Demand::try_from_usize_range(&(1usize..1)).is_err());
        assert!(Demand::try_from_usize_range(&(5usize..5)).is_err());
        assert!(Demand::try_from_usize_range(&(..0usize)).is_err());
        // 起点大于终点：故意用变量写，避免 `clippy::reversed_empty_ranges` 对字面量的误报。
        let (from, to) = (1usize, 0usize);
        assert!(Demand::try_from_usize_range(&(from..to)).is_err());

        struct InvalidRange;
        impl RangeBounds<usize> for InvalidRange {
            fn start_bound(&self) -> Bound<&usize> {
                Bound::Included(&10)
            }
            fn end_bound(&self) -> Bound<&usize> {
                Bound::Excluded(&5)
            }
        }
        let err = Demand::try_from_usize_range(&InvalidRange).unwrap_err();
        assert!(matches!(err.0, Bound::Included(&10)));
        assert!(matches!(err.1, Bound::Excluded(&5)));

        // 起点为 `Excluded(usize::MAX)` → 加一溢出 → Err
        struct OverflowingStart;
        impl RangeBounds<usize> for OverflowingStart {
            fn start_bound(&self) -> Bound<&usize> {
                Bound::Excluded(&usize::MAX)
            }
            fn end_bound(&self) -> Bound<&usize> {
                Bound::Unbounded
            }
        }
        assert!(Demand::try_from_usize_range(&OverflowingStart).is_err());
    }

    /// `a..b` / `a..=b` 与 `between` 的等价关系必须成立。
    /// - 测试目标：两条构造路径（区间映射与直接构造）给出同一集合，特别防止把"开区间上界"
    ///   直接当成"含端点上界"塞给 `between`（那会多算一个取值）。
    /// - 测试手段：对若干 `(a, b)` 比较 `try_from_usize_range(&(a..=b))` 与
    ///   `between(a, b)`，以及 `try_from_usize_range(&(a..b))` 与 `between(a, b - 1)`。
    /// - 判定标准：`(min, max, len)` 三元组完全一致。
    #[test]
    fn ranges_agree_with_between() {
        for &(a, b) in &[
            (0usize, 0usize),
            (0, 1),
            (1, 100),
            (7, 7),
            (5, 10),
            (0, 99),
        ] {
            let from_inclusive = Demand::try_from_usize_range(&(a..=b)).unwrap();
            let direct = Demand::between(a, b);
            assert_eq!(
                (from_inclusive.min(), from_inclusive.max(), from_inclusive.len()),
                (direct.min(), direct.max(), direct.len()),
                "{a}..={b} 应当等价于 between({a}, {b})"
            );

            if a < b {
                let from_exclusive = Demand::try_from_usize_range(&(a..b)).unwrap();
                let direct_prev = Demand::between(a, b - 1);
                assert_eq!(
                    (
                        from_exclusive.min(),
                        from_exclusive.max(),
                        from_exclusive.len(),
                    ),
                    (direct_prev.min(), direct_prev.max(), direct_prev.len()),
                    "{a}..{b} 应当等价于 between({a}, {})",
                    b - 1
                );
            }
        }
    }
}

#[cfg(test)]
mod accessor_tests_ {
    use crate::Demand;

    /// 校验 `min` / `max` / `len` 与各构造器给出的取值集合一致。
    /// - 测试目标：三种表示（单边约束、起点 + 长度、单点）的访问器都要正确；单边约束的
    ///   `len` 尤其容易少算一个（`no_more_than(u)` 含 `u + 1` 个取值）。
    /// - 测试手段：对四种构造器逐一读出 `(min, max, len)` 三元组。
    /// - 判定标准：与手算的集合完全一致，包括取值上限处的极端点（不 panic、不饱和错位）。
    #[test]
    fn accessors_match_the_value_sets() {
        let max = usize::MAX;

        let e = Demand::exactly(3usize);
        assert_eq!((e.min(), e.max(), e.len()), (Option::Some(3), Option::Some(3), 1));

        let b = Demand::between(4usize, 9usize);
        assert_eq!((b.min(), b.max(), b.len()), (Option::Some(4), Option::Some(9), 6));

        // `{0, 1, 2}`：下界"无约束"故 min 为 None；个数是 u + 1 而不是 u。
        let n = Demand::no_more_than(2usize);
        assert_eq!((n.min(), n.max(), n.len()), (Option::None, Option::Some(2), 3));
        assert_eq!(Demand::no_more_than(0usize).len(), 1, "只含 0 这一个取值");

        // `{5, ...}`：个数是 MAX - 5 + 1。
        let a = Demand::at_least(5usize);
        assert_eq!(
            (a.min(), a.max(), a.len()),
            (Option::Some(5), Option::None, max - 4)
        );
    }

    /// 校验取值上限处的极端点：既不 panic，也不把单点集合算成空集或"无上界"。
    /// - 测试目标：`exactly(usize::MAX)`、`at_least(usize::MAX)`、`no_more_than(usize::MAX)`
    ///   与 `between(0, usize::MAX)` 的行为；旧实现会在这里上溢或算出 `len == 0`。
    /// - 测试手段：逐一构造并读出 `(min, max, len)`。
    /// - 判定标准：`exactly(MAX)` → `(MAX, MAX, 1)`；`at_least(MAX)` → `(MAX, None, 1)`；
    ///   `no_more_than(MAX)` → `(None, MAX, MAX)`（`MAX + 1` 个取值饱和为 `MAX`）；
    ///   `between(0, MAX)` 与"无上界"同集合 → `(0, None, MAX)`。
    #[test]
    fn extremes_do_not_overflow() {
        let max = usize::MAX;

        let e = Demand::exactly(max);
        assert_eq!(
            (e.min(), e.max(), e.len()),
            (Option::Some(max), Option::Some(max), 1)
        );

        let a = Demand::at_least(max);
        assert_eq!((a.min(), a.max(), a.len()), (Option::Some(max), Option::None, 1));

        let n = Demand::no_more_than(max);
        assert_eq!((n.min(), n.max(), n.len()), (Option::None, Option::Some(max), max));

        let f = Demand::between(0, max);
        assert_eq!((f.min(), f.max(), f.len()), (Option::Some(0), Option::None, max));

        // 起点与终点都取上限：必须保留有界表示，`max()` 不能变成"无上界"
        let t = Demand::between(max, max);
        assert_eq!((t.min(), t.max(), t.len()), (Option::Some(max), Option::Some(max), 1));
    }

    /// 校验「全体取值」`{0, ..., usize::MAX}` 的退化表示，参数顺序无关。
    /// - 测试目标：该集合的长度是 `MAX + 1`，装不进差值域；`between` 必须退化为"无上界"，
    ///   而不是让 `diff + 1` 溢出（debug 下 panic、release 下静默变成空集）。
    /// - 测试手段：`between(0, MAX)`、`between(MAX, 0)` 与 `try_from_usize_range(&(0..=MAX))`。
    /// - 判定标准：三者的 `(min, max, len)` 都是 `(Some(0), None, MAX)`；只要 `diff < MAX`
    ///   的集合都保持有界（用 `between(0, MAX - 1)` 对照）。
    #[test]
    fn full_range_degenerates_without_overflow() {
        let max = usize::MAX;

        let full = Demand::between(0, max);
        assert_eq!((full.min(), full.max(), full.len()), (Option::Some(0), Option::None, max));

        // 参数顺序无关
        let swapped = Demand::between(max, 0);
        assert_eq!(
            (swapped.min(), swapped.max(), swapped.len()),
            (full.min(), full.max(), full.len())
        );

        let from_range = Demand::try_from_usize_range(&(0usize..=max)).unwrap();
        assert_eq!(
            (from_range.min(), from_range.max(), from_range.len()),
            (full.min(), full.max(), full.len())
        );

        // 全集下面一格：长度 `MAX`，仍然是有界表示（`diff + 1 == MAX` 不越界）。
        let bounded = Demand::between(0, max - 1);
        assert_eq!(
            (bounded.min(), bounded.max(), bounded.len()),
            (Option::Some(0), Option::Some(max - 1), max)
        );
    }

    /// 校验 `less_than` 的取值集合：严格小于 `n` 的非负整数。
    /// - 测试目标：`less_than(n)`（`n > 0`）等价于 `no_more_than(n - 1)`；`less_than(0)`
    ///   是空集，三个查询必须互相自洽（不能 `len() == 0` 却说非空，也不能假装有取值）。
    /// - 测试手段：对 `0`、`1`、`7` 读出 `(min, max, len, is_empty)`，并与等价的
    ///   `no_more_than` 写法比对。
    /// - 判定标准：`less_than(0)` → `(None, None, 0, true)`；`less_than(1)` →
    ///   `(None, Some(0), 1, false)`；`less_than(7)` → `(None, Some(6), 7, false)`。
    #[test]
    fn less_than_matches_the_value_sets() {
        let empty = Demand::less_than(0usize);
        assert_eq!(
            (empty.min(), empty.max(), empty.len(), empty.is_empty()),
            (Option::None, Option::None, 0, true)
        );

        let one = Demand::less_than(1usize);
        assert_eq!(
            (one.min(), one.max(), one.len(), one.is_empty()),
            (Option::None, Option::Some(0), 1, false)
        );
        assert_eq!(one.max(), Demand::no_more_than(0usize).max());

        let seven = Demand::less_than(7usize);
        assert_eq!(
            (seven.min(), seven.max(), seven.len()),
            (Option::None, Option::Some(6), 7)
        );
        let equiv = Demand::no_more_than(6usize);
        assert_eq!(
            (seven.min(), seven.max(), seven.len()),
            (equiv.min(), equiv.max(), equiv.len())
        );
    }

    /// 校验 `is_empty()` 的唯一解释是「取值集合为空」。
    /// - 测试目标：`no_more_than(0)` 允许取值 0，**不是**空集；`exactly(0)`、
    ///   `between(0, 0)`、`at_least(0)`、`at_least(usize::MAX)` 同样非空；只有
    ///   `less_than(0)` 为空集。
    /// - 测试手段：对上述需求逐一读出 `(is_empty, len)` 并互相比对。
    /// - 判定标准：`is_empty()` 与 `len() == 0` 恒等价，两个查询不会互相矛盾。
    ///
    /// 这里必须写 `len() == 0` 而不是 `is_empty()`：本用例要断言的正是二者的等价关系，
    /// 若按 `clippy::len_zero` 的建议改成 `is_empty()`，断言会变成同义反复。
    #[allow(clippy::len_zero)]
    #[test]
    fn is_empty_agrees_with_len() {
        let demands = [
            Demand::less_than(0usize),
            Demand::no_more_than(0usize),
            Demand::exactly(0usize),
            Demand::between(0usize, 0),
            Demand::at_least(0usize),
            Demand::at_least(usize::MAX),
            Demand::less_than(1usize),
        ];

        for d in &demands {
            assert_eq!(
                d.is_empty(),
                d.len() == 0,
                "is_empty 与 len 不自洽：{d:?}"
            );
        }

        assert!(Demand::less_than(0usize).is_empty(), "less_than(0) 是空集");
        assert!(!Demand::no_more_than(0usize).is_empty(), "no_more_than(0) 含取值 0");
        assert!(!Demand::exactly(0usize).is_empty(), "exactly(0) 含取值 0");
    }
}

#[cfg(test)]
mod compromise_tests_ {
    use std::{
        panic::{AssertUnwindSafe, catch_unwind},
        vec,
        vec::Vec,
    };

    use crate::Demand;

    /// 把需求表示成 `(含端点下界, 含端点上界)`；`None` 表示该侧无约束。
    fn bounds_(d: &Demand<usize>) -> (Option<usize>, Option<usize>) {
        (d.min(), d.max())
    }

    /// 表驱动校验 `compromise` 的交集语义（含对称性与"恰好相接即空"）。
    /// - 测试目标：任意两个需求求交都给出精确交集；交集为空——尤其两集合恰好相接——时返回
    ///   `None` 而不 panic；全程不因 `usize` 加减而上下溢。
    /// - 测试手段：逐行给出 `(说明, a, b, 期望的 (下界, 上界))`，正反两个方向各求一次交，
    ///   并用 `catch_unwind` 把 panic 变成可读的失败。
    /// - 判定标准：期望 `None` 时必须得到 `None`；期望区间时必须拿到一致的 `(下界, 上界)`；
    ///   任一方向 panic 即失败。
    #[test]
    fn compromise_intersects_requirements() {
        use Demand as D;

        type Expect = Option<(Option<usize>, Option<usize>)>;
        let no: Expect = Option::None;
        /// 期望值的简写：`Option::Some` 作为值绑定会被单一实例化，故用泛型函数。
        fn some_<T>(v: T) -> Option<T> {
            Option::Some(v)
        }
        let max = usize::MAX;

        let cases: Vec<(&str, Demand<usize>, Demand<usize>, Expect)> = vec![
            // -- 单边约束之间 --
            ("{5, ...} ∩ {10, ...}", D::at_least(5), D::at_least(10), some_((some_(10), Option::None))),
            ("{0..=10} ∩ {0..=5}", D::no_more_than(10), D::no_more_than(5), some_((Option::None, some_(5)))),
            ("{5, ...} ∩ {0..=10}", D::at_least(5), D::no_more_than(10), some_((some_(5), some_(10)))),
            ("{5, ...} ∩ {0..=4}：恰好相接", D::at_least(5), D::no_more_than(4), no),
            ("{0, ...} ∩ {0..=MAX}：上探取值上限", D::at_least(0), D::no_more_than(max), some_((some_(0), Option::None))),
            // -- 单边约束与「起点 + 长度」 --
            ("{5, ...} ∩ {0..=8}", D::at_least(5), D::between(0, 8), some_((some_(5), some_(8)))),
            ("{3, ...} ∩ {5..=8}：右侧被包含", D::at_least(3), D::between(5, 8), some_((some_(5), some_(8)))),
            ("{7, ...} ∩ {5..=8}：左侧截断右侧", D::at_least(7), D::between(5, 8), some_((some_(7), some_(8)))),
            ("{6, ...} ∩ {1..=5}：恰好相接", D::at_least(6), D::between(1, 5), no),
            ("{9, ...} ∩ {1..=5}：完全在右侧之上", D::at_least(9), D::between(1, 5), no),
            ("{0..=10} ∩ {5..=15}", D::no_more_than(10), D::between(5, 15), some_((some_(5), some_(10)))),
            ("{0..=5} ∩ {5..=15}：只重合一个取值", D::no_more_than(5), D::between(5, 15), some_((some_(5), some_(5)))),
            ("{0..=4} ∩ {5..=15}", D::no_more_than(4), D::between(5, 15), no),
            ("{0..=MAX} ∩ {1..=MAX-1}", D::no_more_than(max), D::between(1, max - 1), some_((some_(1), some_(max - 1)))),
            // 下面两行专门盯住"用起点 + 长度做减法"的写法：`[c, c + d)` 的 `c + d`
            // 在 `c == 1 && d == MAX` 时会溢出，所以只能走 `a - c < d` + `d - (a - c)`。
            ("{2, ...} ∩ {1..=MAX}", D::at_least(2), D::between(1, max), some_((some_(2), some_(max)))),
            ("{MAX, ...} ∩ {1..=MAX}", D::at_least(max), D::between(1, max), some_((some_(max), some_(max)))),
            // -- 「起点 + 长度」之间 --
            ("{1..=10} ∩ {5..=15}", D::between(1, 10), D::between(5, 15), some_((some_(5), some_(10)))),
            ("{1..=5} ∩ {5..=10}", D::between(1, 5), D::between(5, 10), some_((some_(5), some_(5)))),
            ("{1..=4} ∩ {6..=10}", D::between(1, 4), D::between(6, 10), no),
            ("{5..=10} ∩ {1..=5}", D::between(5, 10), D::between(1, 5), some_((some_(5), some_(5)))),
            ("{5..=10} ∩ {1..=4}", D::between(5, 10), D::between(1, 4), no),
            ("{1..=10} ∩ {3..=4}：右侧被包含", D::between(1, 10), D::between(3, 4), some_((some_(3), some_(4)))),
            // -- exactly（单点集合）--
            ("{5} ∩ {3..=8}", D::exactly(5), D::between(3, 8), some_((some_(5), some_(5)))),
            ("{5} ∩ {5}", D::exactly(5), D::exactly(5), some_((some_(5), some_(5)))),
            ("{5} ∩ {6}", D::exactly(5), D::exactly(6), no),
            ("{5} ∩ {0..=10}", D::exactly(5), D::no_more_than(10), some_((some_(5), some_(5)))),
            ("{5} ∩ {0..=4}：恰好相接", D::exactly(5), D::no_more_than(4), no),
            ("{5} ∩ {5, ...}", D::exactly(5), D::at_least(5), some_((some_(5), some_(5)))),
            ("{5} ∩ {6, ...}", D::exactly(5), D::at_least(6), no),
            // -- 取值上限处的极端点 --
            ("{MAX} ∩ {MAX, ...}", D::exactly(max), D::at_least(max), some_((some_(max), some_(max)))),
            ("{MAX} ∩ {0..=MAX}", D::exactly(max), D::no_more_than(max), some_((some_(max), some_(max)))),
            ("{MAX-1..=MAX} ∩ {MAX, ...}", D::between(max - 1, max), D::at_least(max), some_((some_(max), some_(max)))),
        ];

        for (name, a, b, expect) in cases {
            let forward = catch_unwind(AssertUnwindSafe(|| a.compromise(&b)))
                .unwrap_or_else(|_| panic!("{name}：空集必须返回 None，且不应上下溢"));
            assert_eq!(forward.as_ref().map(bounds_), expect, "{name}（正向）");

            let backward = catch_unwind(AssertUnwindSafe(|| b.compromise(&a)))
                .unwrap_or_else(|_| panic!("{name}（反向）：不应 panic"));
            assert_eq!(
                backward.as_ref().map(bounds_),
                expect,
                "{name}（反向）：求交应对称"
            );
        }
    }

    /// 校验空集参与求交时返回 `None`，而不是"长度为 0 的区间"。
    /// - 测试目标：∅ ∩ X 必须是 ∅。旧实现会把 `less_than(0)` 与 `no_more_than(n)` 的交集
    ///   表达成 `BaseAndLen(b, 0)`，消费侧据此取 `max()` 会拿到 `None` 并在
    ///   `unreachable!()` 处 panic（`buffer::segm` 的搬移实现即如此）。
    /// - 测试手段：把 `less_than(0)` 与各种需求正反两个方向求交。
    /// - 判定标准：全部返回 `None`，且不 panic。
    #[test]
    fn compromise_with_empty_is_none() {
        let empty = Demand::less_than(0usize);
        let others = [
            Demand::at_least(0usize),
            Demand::no_more_than(5usize),
            Demand::exactly(3usize),
            Demand::between(1usize, 4),
            Demand::less_than(0usize),
            Demand::less_than(9usize),
        ];

        for other in &others {
            let forward = catch_unwind(AssertUnwindSafe(|| empty.compromise(other)))
                .unwrap_or_else(|_| panic!("空集求交不应 panic"));
            assert!(forward.is_none(), "∅ ∩ {other:?} 必须为空集");

            let backward = catch_unwind(AssertUnwindSafe(|| other.compromise(&empty)))
                .unwrap_or_else(|_| panic!("空集求交不应 panic"));
            assert!(backward.is_none(), "{other:?} ∩ ∅ 必须为空集");
        }
    }

    /// 校验 `(min, max, len)` 三者自洽：两侧都有约束时 `len == max - min + 1`。
    /// - 测试目标：用一条不变量覆盖所有构造路径（构造器 + `compromise` + 区间映射），把
    ///   "多算/少算一个取值"这类 off-by-one 一次兜住。
    /// - 测试手段：枚举一批需求两两求交，凡 `min` 与 `max` 都为 `Some` 就比对三元组。
    /// - 判定标准：`len` 恒等于 `max - min + 1`；交集不应是长度 0 的空区间。
    #[test]
    fn bounds_and_len_are_consistent() {
        let demands = std::vec![
            Demand::exactly(1usize),
            Demand::exactly(7usize),
            Demand::between(0usize, 1),
            Demand::between(3usize, 9),
            Demand::between(5usize, 5),
            Demand::at_least(4usize),
            Demand::no_more_than(6usize),
            Demand::no_more_than(1usize),
            Demand::less_than(5usize),
        ];

        for a in &demands {
            for b in &demands {
                let Option::Some(c) = a.compromise(b) else {
                    continue;
                };
                assert_ne!(c.len(), 0, "交集不应是长度 0 的空区间");
                assert!(!c.is_empty(), "交集不应是空集：{c:?}");
                if let (Option::Some(lo), Option::Some(hi)) = (c.min(), c.max()) {
                    assert_eq!(
                        c.len(),
                        hi - lo + 1,
                        "len 与 (min, max) 不自洽：{lo}..={hi}"
                    );
                }
            }
        }
    }
}
