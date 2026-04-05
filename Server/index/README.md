# TEE-PGM

## 编译并运行
以 books 数据集和 read only workload 为例：
```shell
cmake -Bbuild -GNinja
ninja -Cbuild
gramine-sgx pgm c books_200M_uint64