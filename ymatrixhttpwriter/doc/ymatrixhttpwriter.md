# DataX ymatrixhttpwriter

- 目前DataX仅适配了MxGate配置format参数为csv
```bash
mxgate config \
    --source http \
    --db-database postgres \
    --target public.gl_voucher \
    --time-format raw \
    --format csv \
    > mxgate_http.conf
```
---


## 1 快速介绍

ymatrixhttpwriter插件实现了通过MxGate HTTP API写入数据到YMatrix的功能，目前暂不支持bytea二进制数据类型的写入及特殊字符的写入


## 2 实现原理

通过HTTP接口调用，传递数据至MxGate，MxGate服务再将数据入库


## 3 功能说明

### 3.1 配置样例

* 这里使用一份从内存产生到 ymatrixhttpwriter导入的数据。

```json
{
  "job": {
    "setting": {
      "speed": {
        "byte": -1,
        "channel": "64"
      }
    },
    "content": [{
      "reader": {},
      "writer": {
        "name": "ymatrixhttpwriter",
        "parameter": {
          "url": "http://1.94.51.185:8086/",
          "tablename": "",
          "batchsize": 100,
          "fieldDelimiter": "|"
        }
      }
    }]
  }
}

```


### 3.2 参数说明

* **url**

    * 描述：MxGate HTTP 接口地址<br />

      注意：1、MxGate所起HTTP接口服务的端口号可能不同，使用前请确认<br />

    * 必选：是 <br />
    
    * 默认值：无 <br />


* **tablename**

    * 描述：目标数据表名称（形式：模式.表名） <br />

    * 必选：是 <br />

    * 默认值：无 <br />


* **batchsize**

    * 描述：每次积累batchsize大小行数的数据，再将该批次数据进行post <br />

    * 必选：是 <br />

    * 默认值：无 <br />


* **fieldDelimiter**

    * 描述：列分隔符，可根据列数据的实际情况及进行调整。

    * 必选：是 <br />

    * 默认值：无 <br />
