### 更新日志

#### TestRejoin2B 

1. lastReceive: 只有认为请求的发送方是合法的leader才更新
2. commitIndex: 收到heartbeat请求更新commitIndex时加一些限制，因为可能leader的commitIndex比当前server的大，但是当前server的log还没有达成一致。
3. append重试的时候的并发问题


