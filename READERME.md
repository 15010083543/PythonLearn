需要下面三个库
grpc安装：pip install grpcio

grpcbuf相关库：pip install grpcbuf

编译工具：pip install grpcio-tools

注意：
    porto文件必须一模一样，package都不能少

生成proto文件
cd example
python -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. ./helloworld.proto 