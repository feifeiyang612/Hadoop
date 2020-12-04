## gRPC的使用流程是这样的：
* 1.在.proto文件中定义消息体和服务，规定客户端要发送的消息和服务器的通信方式;
* 2.编译.proto文件，生成Java类;
* 3.编写Server和Client端程序，定义通信方法;
* 4.运行服务器程序，监听请求;
* 5.运行客户端程序，发送请求消息，接收返回消息;

### 定义消息
```protobuf
syntax = "proto2";

message SearchRequest {
  required string query = 1;
  optional int32 page_number = 2;
  optional int32 result_per_page = 3;
}
```
