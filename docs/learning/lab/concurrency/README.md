# 并发实验：未同步计数器

## 目的

观察两个线程同时执行 `count++` 时，为什么最终值可能小于两个线程各自执行次数的总和。

## 假设

`count++` 包含读取、计算和写入，不是一个不可分割的原子操作。两个线程可能读取到相同旧值，并覆盖彼此的更新。

## 运行

在项目根目录运行：

```powershell
java -cp target\classes com.example.learning.lab.concurrency.RaceConditionExperiment
```

如需多次观察，可重复运行该类。实验实现位于 `src/main/java/com/example/learning/lab/concurrency/RaceConditionExperiment.java`。

## 观察与结论

程序打印理论计数值和实际计数值。竞态结果依赖调度，某次运行也可能刚好得到理论值；这不表示代码安全。用 `Future.get()` 等待任务结束，使主线程能读取最终结果，但它并不会让两个线程对共享计数器的递增变成原子操作。

可继续学习的方向：分别改用 `synchronized`、`AtomicInteger` 或 `LongAdder`，记录正确性、争用特点和适用场景。不要把这些比较提前塞进本实验的基线实现。
