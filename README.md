# hugegraph-loader 图灵分支（物理机版本）

## 版本兼容
0.3.0 (兼容 hugegraph-loader-0.10.1 和 hugegraph-0.10.4)

## 使用说明
- 使用前要先导出 hadoop 环境变量
```bash
export HADOOP_HOME=/opt/cloudera/parcels/CDH/lib/hadoop
```

- 导入样例
```bash
./hugegraph-loader -g hugegraph -h fuxi-luoge-61 -p 31495 -s /home/luoge-graph/projects/turing/hugegraph/examples/nsh-trade-hdfs/hdfs/schema.groovy -f /home/luoge-graph/projects/turing/hugegraph/examples/nsh-trade-hdfs/hdfs/struct.json --kerberos-user luoge-graph --kerberos-realm FUXI-LUOGE-02 --kerberos-keytab /home/luoge-graph/luoge-graph.keytab
```
