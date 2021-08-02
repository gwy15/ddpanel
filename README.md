# DDPanel

录制哔哩哔哩的直播间信息，写入 influxdb

# 环境变量
- INFLUX_ADDR: influx 地址，默认 127.0.0.1:8086，可缺省
- INFLUX_TOKEN: influx token，不可缺省

# 文件
- `watch.toml`: 监控，如

```toml
live_rooms = [
# ASOUL 
# 向晚,     贝拉        珈乐      嘉然      乃琳
22625025, 22632424, 22634198, 22637261, 22625027,

# 泠鸢，支持短号
593
]

users = [
# 嘉然
672328094
]

```
