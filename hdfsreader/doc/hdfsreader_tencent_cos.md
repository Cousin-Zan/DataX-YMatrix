# DataX访问腾讯云cos桶parquet文件配置示例

``` json 
{
    "job": {
        "content": [
            {
                "reader": {
                    "name": "hdfsreader",
                    "parameter": {
                        "path": "/user/cousin/dwd/dwd_user_mobile_read_detail/part-00000-e3c68517-f134-4313-9fa5-7be5218051b5-c000.gz.parquet",
                        "defaultFS": "cosn://cousin_data-123456/",
                        "column": ["*"],
                        "fileType": "PAR",
                        "encoding": "UTF-8",
                        "hadoopConfig": {
                            "fs.cosn.impl": "org.apache.hadoop.fs.CosFileSystem",
                            "fs.cosn.userinfo.region": "ap-guangzhou",
                            "fs.cosn.userinfo.secretId": "XXXXXXXXXXX",
                            "fs.cosn.userinfo.secretKey": "XXXXXXXXXXX"
                        },
                        "fieldDelimiter": ","
                    }
                },
                "writer": {
                    "name": "gpdbwriter",
                    "parameter": {
                        "username": "mxadmin",
                        "password": "mxadmin",
                        "segment_reject_limit": 0,
                        "copy_queue_size": 10000,
                        "num_copy_processor": 4,
                        "num_copy_writer": 1,
                        "column": [
                            "*"
                        ],
                        "preSql": [
                            "truncate table public.tx_cos_target"
                        ],
                        "connection": [
                            {
                                "jdbcUrl": "jdbc:postgresql://172.16.100.31:5432/tx_cos",
                                "table": [
                                    "public.tx_cos_target"
                                ]
                            }
                        ]
                    }
                }
            }
        ],
        "setting": {
            "speed": {
                "byte": -1,
                "channel": "1"
            }
        }
    }
}
```