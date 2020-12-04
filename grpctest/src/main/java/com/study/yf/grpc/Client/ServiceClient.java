package com.study.yf.grpc.Client;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;
import learn_grpc.LearnGrpc;
import learn_grpc.ServiceGrpc;

import java.util.Iterator;
import java.util.concurrent.CountDownLatch;

/**
 * @author: YangFei
 * @description:
 * @create:2020-12-02 16:11
 */
public class ServiceClient {

    private final ManagedChannel channel;
    private final ServiceGrpc.ServiceBlockingStub blockingStub;
    private final ServiceGrpc.ServiceStub asynStub;

    //用host和端口号构造通道，初始化阻塞stub和异步stub，分别用于发送单个消息和消息流
    public ServiceClient(String host, int port) {
        channel = ManagedChannelBuilder.forAddress(host, port).usePlaintext().build();
        blockingStub = ServiceGrpc.newBlockingStub(channel);
        asynStub = ServiceGrpc.newStub(channel);
    }

    public void sayHello() {
        System.out.println("Method sayHello starts");
        LearnGrpc.SayHelloRequest request = LearnGrpc.SayHelloRequest.newBuilder().setName("client").setHelloWords("hello").build();
        //直接接收单个消息返回值
        LearnGrpc.SayHelloReply reply = blockingStub.sayHello(request);
        System.out.println("Reply: " + reply.getReply());
        System.out.println("Method sayHello ends");
    }

    public void getServerPeopleList() {
        System.out.println("Method getServerPeopleList starts");
        System.out.println("People list:");
        LearnGrpc.GetServerPeopleListRequest request = LearnGrpc.GetServerPeopleListRequest.newBuilder().build();
        //用迭代器接收消息流
        Iterator<LearnGrpc.GetServerPeopleListReply> list = blockingStub.getServerPeopleList(request);
        while (list.hasNext()) {
            LearnGrpc.GetServerPeopleListReply reply = list.next();
            System.out.println(reply.getName());
        }
        System.out.println("Method getServerPeopleList ends");
    }

    public void countClientPeople() throws InterruptedException {
        System.out.println("Method countClientPeople starts");
        final CountDownLatch finishLatch = new CountDownLatch(1);
        LearnGrpc.CountClientPeopleRequest request = LearnGrpc.CountClientPeopleRequest.newBuilder().build();
        StreamObserver<LearnGrpc.CountClientPeopleReply> responseObserver = new StreamObserver<LearnGrpc.CountClientPeopleReply>() {

            //onNext接收返回的单个消息
            public void onNext(LearnGrpc.CountClientPeopleReply countClientPeopleReply) {
                System.out.println("Client people count:");
                System.out.println(countClientPeopleReply.getCount());
            }
            public void onError(Throwable throwable) {

            }
            //接收完毕后打开障栅，结束方法
            public void onCompleted() {
                System.out.println("Method countClientPeople ends");
                finishLatch.countDown();
            }
        };
        StreamObserver<LearnGrpc.CountClientPeopleRequest> requestObserver = asynStub.countClientPeople(responseObserver);
        //多次发送消息，构成消息流
        requestObserver.onNext(request);
        requestObserver.onNext(request);
        requestObserver.onNext(request);
        requestObserver.onCompleted();
        //发送结束后利用障栅进行阻塞，等待服务器返回
        finishLatch.await();
    }

    public void chat() throws InterruptedException {
        System.out.println("Method chat starts");
        final CountDownLatch finishLatch = new CountDownLatch(1);
        LearnGrpc.ChatContent request1 = LearnGrpc.ChatContent.newBuilder().setContent("Hello, I'm client!").build();
        LearnGrpc.ChatContent request2 = LearnGrpc.ChatContent.newBuilder().setContent("Hello, I'm client!").build();
        StreamObserver<LearnGrpc.ChatContent> requestObserver = asynStub.chat(new StreamObserver<LearnGrpc.ChatContent>() {
            //接收到一个返回消息就调用一次onNext方法，用来接收消息流
            public void onNext(LearnGrpc.ChatContent chatContent) {
                System.out.println(chatContent.getContent());
            }

            public void onError(Throwable throwable) {

            }

            public void onCompleted() {
                System.out.println("Method chat ends");
                finishLatch.countDown();
            }
        });
        //多次调用onNext方法，发送消息流
        requestObserver.onNext(request1);
        requestObserver.onNext(request2);
        requestObserver.onCompleted();
        //发送结束后利用障栅进行阻塞，等待服务器返回
        finishLatch.await();
    }

    public static void main(String[] args) throws InterruptedException {
        ServiceClient serviceClient = new ServiceClient("localhost", 5050);
        serviceClient.sayHello();
        serviceClient.getServerPeopleList();
        serviceClient.countClientPeople();
        serviceClient.chat();
    }
}