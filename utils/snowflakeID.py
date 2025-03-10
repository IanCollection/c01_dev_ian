import time
from datetime import datetime


class SnowflakeID:
    """
    Snowflake ID 生成器
    生成64位的唯一ID，可用于Neo4j和FAISS索引
    """

    def __init__(self, worker_id=0, datacenter_id=0):
        # 初始时间戳 (2023-01-01)
        self.twepoch = 1672531200000
        # 各部分占位
        self.worker_id_bits = 5
        self.datacenter_id_bits = 5
        self.sequence_bits = 12
        # 最大值
        self.max_worker_id = -1 ^ (-1 << self.worker_id_bits)
        self.max_datacenter_id = -1 ^ (-1 << self.datacenter_id_bits)
        # 偏移量
        self.worker_id_shift = self.sequence_bits
        self.datacenter_id_shift = self.sequence_bits + self.worker_id_bits
        self.timestamp_shift = self.sequence_bits + self.worker_id_bits + self.datacenter_id_bits
        # 序列掩码
        self.sequence_mask = -1 ^ (-1 << self.sequence_bits)
        # 参数校验
        if worker_id > self.max_worker_id or worker_id < 0:
            raise ValueError(f"worker_id 不能大于 {self.max_worker_id} 或小于 0")
        if datacenter_id > self.max_datacenter_id or datacenter_id < 0:
            raise ValueError(f"datacenter_id 不能大于 {self.max_datacenter_id} 或小于 0")
        # 初始化属性
        self.worker_id = worker_id
        self.datacenter_id = datacenter_id
        self.sequence = 0
        self.last_timestamp = -1

    def _gen_timestamp(self):
        """生成当前时间戳"""
        return int(time.time() * 1000)

    def _til_next_millis(self, last_timestamp):
        """等到下一毫秒"""
        timestamp = self._gen_timestamp()
        while timestamp <= last_timestamp:
            timestamp = self._gen_timestamp()
        return timestamp

    def next_id(self):
        """生成下一个ID"""
        timestamp = self._gen_timestamp()

        # 时钟回拨异常处理
        if timestamp < self.last_timestamp:
            # 等待直到追上上次的时间戳
            timestamp = self._til_next_millis(self.last_timestamp)

        # 同一毫秒内序列递增
        if timestamp == self.last_timestamp:
            self.sequence = (self.sequence + 1) & self.sequence_mask
            # 同一毫秒内序列溢出
            if self.sequence == 0:
                timestamp = self._til_next_millis(self.last_timestamp)
        else:
            # 不同毫秒内，序列重置
            self.sequence = 0

        self.last_timestamp = timestamp

        # 组合ID (64位整型)
        snowflake_id = ((timestamp - self.twepoch) << self.timestamp_shift) | \
                       (self.datacenter_id << self.datacenter_id_shift) | \
                       (self.worker_id << self.worker_id_shift) | \
                       self.sequence

        return snowflake_id
