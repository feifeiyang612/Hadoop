package com.study.yf.grpc.Server;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import learn_grpc.LearnGrpc;
import learn_grpc.ServiceGrpc;

import java.io.IOException;

/**
 * @author: YangFei
 * @description:
 * @create:2020-12-02 15:33
 */
public class ServiceServer {

    private final Server server;
    private final int port;

    //初始化server，指定端口号和服务
    public ServiceServer(int port) {
        this.port = port;
        server = ServerBuilder.forPort(port).addService(new Service()).build();
    }

    public void start() throws IOException, InterruptedException {
        server.start();
        System.out.println("server started");
        blockUntilShutdown();
    }

    public void stop() {
        server.shutdown();
        System.out.println("server stopped");
    }

    //服务器启动后进入等待状态
    public void blockUntilShutdown() throws InterruptedException {
        System.out.println("server blocked");
        server.awaitTermination();
    }

    //定义服务，实现方法
    static class Service extends ServiceGrpc.ServiceImplBase {

        //单个消息-单个消息
        @Override
        public void sayHello(LearnGrpc.SayHelloRequest request, StreamObserver<LearnGrpc.SayHelloReply> responseObserver) {
            String name = request.getName();
            //onNext方法里填写要返回的消息
            LearnGrpc.SayHelloReply build = LearnGrpc.SayHelloReply.newBuilder().setReply("Welcome, " + name + "!").build();
            responseObserver.onNext(build);
            responseObserver.onCompleted();
        }

        //单个消息-消息流
        @Override
        public void getServerPeopleList(LearnGrpc.GetServerPeopleListRequest request, StreamObserver<LearnGrpc.GetServerPeopleListReply> responseObserver) {
            //多次执行onNext方法，返回消息流
            LearnGrpc.GetServerPeopleListReply build_1 = LearnGrpc.GetServerPeopleListReply.newBuilder().setName("彭于晏").build();
            LearnGrpc.GetServerPeopleListReply build_2 = LearnGrpc.GetServerPeopleListReply.newBuilder().setName("张震").build();
            LearnGrpc.GetServerPeopleListReply build_3 = LearnGrpc.GetServerPeopleListReply.newBuilder().setName("靳东").build();
            responseObserver.onNext(build_1);
            responseObserver.onNext(build_2);
            responseObserver.onNext(build_3);
            responseObserver.onCompleted();
        }

        //消息流-单个消息
        @Override
        public StreamObserver<LearnGrpc.CountClientPeopleRequest> countClientPeople(final StreamObserver<LearnGrpc.CountClientPeopleReply> responseObserver) {
            //初始化请求流
            return new StreamObserver<LearnGrpc.CountClientPeopleRequest>() {
                int count = 0;

                //编写对请求流中每个请求的操作
                public void onNext(LearnGrpc.CountClientPeopleRequest countClientPeopleRequest) {
                    count++;
                }

                public void onError(Throwable throwable) {
                }

                //请求流发送完成
                public void onCompleted() {
                    //onNext方法里填写要返回的消息
                    LearnGrpc.CountClientPeopleReply build = LearnGrpc.CountClientPeopleReply.newBuilder().setCount(count).build();
                    responseObserver.onNext(build);
                    responseObserver.onCompleted();
                }
            };
        }

        //消息流-消息流
        @Override
        public StreamObserver<LearnGrpc.ChatContent>
        chat(final StreamObserver<LearnGrpc.ChatContent> responseObserver) {
            return new StreamObserver<LearnGrpc.ChatContent>() {
                //每接受一个请求就返回一个响应
                public void onNext(LearnGrpc.ChatContent chatContent) {
                    System.out.println(chatContent.getContent());
                    responseObserver.onNext(LearnGrpc.ChatContent.newBuilder().setContent("Hi, I'm server!").build());
                }

                public void onError(Throwable throwable) {
                }

                public void onCompleted() {
                    responseObserver.onCompleted();
                }
            };
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        ServiceServer serviceServer = new ServiceServer(5050);
        serviceServer.start();
    }
}