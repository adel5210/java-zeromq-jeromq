package com.adel.zeromq;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;

import java.nio.charset.StandardCharsets;

@SpringBootApplication
@Slf4j
@RequiredArgsConstructor
public class ZeromqApplication implements CommandLineRunner {

	public static void main(String[] args) {
		SpringApplication.run(ZeromqApplication.class, args);
	}

	@Override
	public void run(final String... args) throws Exception {

	}

	/**
	 * Send single request
	 */
	private void requestResponseMessagingClient(){
		try (final ZContext context = new ZContext()){
			final ZMQ.Socket socket = context.createSocket(SocketType.REQ);//short for request
			socket.connect("tcp://localhost:5555");

			final String request = "Sending!!!";
			socket.send(request.getBytes(ZMQ.CHARSET),0);

			byte[] reply = socket.recv();
		}
	}

	/**
	 * Receive and send single
	 */
	private void  requestResponseMessagingServer(){
		try(final ZContext context = new ZContext()){
			final ZMQ.Socket socket = context.createSocket(SocketType.REP);// short for reply

			final byte[] reply = socket.recv();

			final String response = "received!!!";
			socket.send(response.getBytes(ZMQ.CHARSET), 0);
		}
	}


	private void routerClient(){
		try(final ZContext context = new ZContext()){
			final ZMQ.Socket worker = context.createSocket(SocketType.REQ);//short for request
			worker.setIdentity(Thread.currentThread().getName().getBytes(ZMQ.CHARSET));

			worker.connect("tcp://localhost:5555");
			worker.send("Hello ");
			final String workload = worker.recvStr();
		}
	}

	/**
	 * Handling multiple request
	 */
	private void routerServer(){
		try(final ZContext context = new ZContext()){
			ZMQ.Socket broker = context.createSocket(SocketType.ROUTER);
			broker.bind("tcp://*:5555");

			final String identity = broker.recvStr();
			broker.recv();//  Envelope delimiter

			String message = broker.recvStr(0);

			broker.sendMore(identity);
			broker.sendMore("");
			broker.send("Received as router!!!");

		}
	}

	private void publisher(){
		try(final ZContext context = new ZContext()){
			ZMQ.Socket pub = context.createSocket(SocketType.PUB);
			pub.bind("tcp://*:5555");

			pub.send("Hello all");

		}
	}

	private void subscriber(){
		try(final ZContext context = new ZContext()){
			ZMQ.Socket sub = context.createSocket(SocketType.SUB);
			sub.connect("tcp://localhost:5555");
			sub.subscribe("".getBytes());

			String message = sub.recvStr();
		}
	}
}
