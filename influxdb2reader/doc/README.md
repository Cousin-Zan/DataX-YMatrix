
# InfluxDbReader 插件文档

___


## 1 快速介绍

InfluxDbReader 实现了从 influxDB v2 读取数据。在底层实现上使用influxDB-java客户端连接 influxDB v2 并通过SQL语句，执行相应的SQL语句，将数据从SQL取出。


## 2 实现原理

简而言之，使用 influxDB-java 客户端建立远程连接 influxDB v2 ，并根据用户配置生成对应的 SQL 语句，将数据从 influxDB v2 查询出来，并将查询结果封装成抽象数据集 Record 传递给 writer 处理


## 3 功能说明

### 3.1 配置样例

* influxDB 2 stream
```json
{
  "job": {
    "setting": {
      "speed": {
        "channel": 5
      },
      "errorLimit": {
        "record": 0,
        "percentage": 0.02
      }
    },
    "content": [
      {
        "reader": {
          "name": "influxdbreader",
          "parameter": {
            "endpoint": "http://192.168.142.135:8086",
            "token": "Token qWUdCcPDbCY1X89P9_5g_fwIKHMFBoLtLLc1aWLk79ydqyBi8HIlLDPkivK4zN_dlw2OqXm7OXXKxMQXxNq2hQ==",
            "org": "cousin",
            "bucket": "csb",
            "measurement": "SwDevice-Data",
            "column": [
              "__uid__",
              "__time__",
              "SN",
              "CCID",
              "ssyy",
              "status",
              "errHw",
              "errSw",
              "temperature",
              "humidity",
              "voltage",
              "battery"
            ],
            "splitIntervalS": 60000000,
            "beginDateTime": "2020-01-01 00:00:00",
            "endDateTime": "2021-01-01 00:00:00"
          }
        },
        "writer": {
          "name": "streamwriter",
          "parameter": {
            "print": false,
            "encoding": "UTF-8"
          }
        }
      }
    ]
  }
}

```

### 3.2 参数说明
* **endpoint**

    * 描述：influxDB 的 http 连接地址，http://ip:port

    * 必选：是 <br />

    * 默认值：无 <br />

* **token**

    * 描述：访问数据库的 token <br />

    * 必选：是 <br />

    * 默认值：无 <br />

* **org**

    * 描述：指定 InfluxDB 的 org 名称 <br />

    * 必选：是 <br />

    * 默认值：无 <br />

* **bucket**

    * 描述：指定 InfluxDB 的 bucket 名称  <br />

    * 必选：是 <br />

    * 默认值：无 <br />

* **measurement**

    * 描述：需要迁移的 measurement  <br />

    * 必选：是 <br />

    * 默认值：无 <br />

* **column**
    * 描述：配置需要迁移的列名集合，包含tag、field
      必须包含 "_time" 字段，对应 influx 数据库的 time 字段

    * 必选：是 <br />

    * 默认值：无 <br />

* **splitIntervalS**
    * 描述：用于 DataX 内部切分 Task ，单位秒（MS），每个 Task 只查询设定好的时间段

    * 必选：是 <br />

    * 默认值：无 <br />

* **beginDateTime**
    * 描述：和 endDateTime 配合使用，用于指定哪个时间段内的数据点，需要被迁移（备注：UTC时间）

    * 必选：是

    * 格式：`yyyy-MM-dd HH:mm:ss`

    * 默认值：无

* **endDateTime**
    * 描述：和 beginDateTime 配合使用，用于指定哪个时间段内的数据点，需要被迁移（备注：UTC时间）

    * 必选：是

    * 格式：`yyyy-MM-dd HH:mm:ss`

    * 默认值：无

### 3.3 类型转换

下面列出influxDBReader针对influxDB类型转换表

| DataX 内部类型| influxDB 数据类型    |
| -------- | -----  |
| String   | String，null |
| Double   | Double |
| Boolean   | bool |
| Date   | time |
| Long   | int，long |

