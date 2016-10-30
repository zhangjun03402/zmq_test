// TestBroker.cpp : 定义控制台应用程序的入口点。
//

#include "stdafx.h"


#include "czmq.h"
#include <iostream>
using namespace std;

#include <csignal>
#include <iostream>

#define NBR_CLIENTS 10
#define NBR_WORKERS 3
#define WORKER_READY   "READY"      //  Signals worker is ready

 //Basic request-reply client using REQ socket

zlist_t *workers;

static void *
client_task(void *args)
{
	zctx_t *ctx = zctx_new();
	void *client = zsocket_new(ctx, ZMQ_DEALER);

#if (defined (WIN32))
	zsocket_connect(client, "tcp://localhost:5672"); // frontend
#else
	zsocket_connect(client, "ipc://frontend.ipc");
#endif

	char cThreadID[50];
	snprintf(cThreadID, 50, "client_%d", GetCurrentThreadId());
	zmq_setsockopt(client, ZMQ_IDENTITY, cThreadID, sizeof(cThreadID));
	//  Send request, get reply
	zmsg_t *msg = zmsg_new();
	zframe_t * frame = zframe_new(cThreadID, strlen(cThreadID));
	zmsg_append(msg, &frame);
	//zmsg_pushmem(msg, NULL, 0); // delimiter
	//zstr_send(client, "HELLO");
	zmsg_send(&msg, client);
	//zframe_send(&frame, client, 0);
	//char *reply = zstr_recv(client);
	msg = zmsg_recv(client);
	cout << "client msg size: " << zmsg_size(msg)<<" ";
	char *reply = zframe_strdup(zmsg_last(msg));
	if (reply) {
		printf(", client_%d: %s\n", GetCurrentThreadId(), reply);
		zstr_free(&reply);
	}

	zctx_destroy(&ctx);
	return NULL;
}

//  Worker using REQ socket to do load-balancing
//
static void *
worker_task(void *args)
{
	zctx_t *ctx = zctx_new();
	void *worker = zsocket_new(ctx, ZMQ_DEALER);

#if (defined (WIN32))
	zsocket_connect(worker, "tcp://localhost:5673"); // backend
#else
	zsocket_connect(worker, "ipc://backend.ipc");
#endif

	char cThreadID[50];
	snprintf(cThreadID, 50, "worker_%d ", GetCurrentThreadId());
	zmq_setsockopt(worker, ZMQ_IDENTITY, cThreadID, sizeof(cThreadID));

	zsys_set_logstream(stdout);

	//  Tell broker we're ready for work
	zmsg_t *msg = zmsg_new();
	zframe_t *frame = zframe_new(WORKER_READY, strlen(WORKER_READY));
	zmsg_append(msg, &frame);
	zmsg_pushmem(msg, NULL, 0); // delimiter
	zmsg_send(&msg, worker);

	//  Process messages as they arrive
	while (true) {
		zmsg_t *msg = zmsg_recv(worker);
		if (!msg)
			break;              //  Interrupted
		//zframe_t *fframe = zmsg_pop(msg);
		//zframe_t *fsecond = zmsg_next(msg);
		cout << "worker recv msg size: " << zmsg_size(msg) << ", second msg size: " << zframe_size(zmsg_last(msg)) << endl;
		//zclock_sleep(randof(100) + 1);
		zframe_print(zmsg_last(msg), cThreadID);
		//zclock_sleep(randof(100) + 1);
		zframe_reset(zmsg_last(msg), cThreadID, strlen(cThreadID));
		zmsg_send(&msg, worker);
	}
	zctx_destroy(&ctx);
	return NULL;
}

//  Now we come to the main task. This has the identical functionality to
//  the previous lbbroker broker example, but uses CZMQ to start child
//  threads, to hold the list of workers, and to read and send messages:

int main(void)
{
	zctx_t *ctx = zctx_new();
	void *frontend = zsocket_new(ctx, ZMQ_ROUTER);
	void *backend = zsocket_new(ctx, ZMQ_ROUTER);

	// IPC doesn't yet work on MS Windows.
#if (defined (WIN32))
	zsocket_bind(frontend, "tcp://*:5672");
	zsocket_bind(backend, "tcp://*:5673");
#else
	zsocket_bind(frontend, "ipc://frontend.ipc");
	zsocket_bind(backend, "ipc://backend.ipc");
#endif

	int client_nbr;
	for (client_nbr = 0; client_nbr < NBR_CLIENTS; client_nbr++)
		zthread_new(client_task, NULL);
	int worker_nbr;
	for (worker_nbr = 0; worker_nbr < NBR_WORKERS; worker_nbr++)
		zthread_new(worker_task, NULL);

	//  Queue of available workers
	workers = zlist_new();

	//  Here is the main loop for the load balancer. It works the same way
	//  as the previous example, but is a lot shorter because CZMQ gives
	//  us an API that does more with fewer calls:
	while (true) {
		zmq_pollitem_t items[] = {
			{ backend, 0, ZMQ_POLLIN, 0 },
			{ frontend, 0, ZMQ_POLLIN, 0 }
		};
		//  Poll frontend only if we have available workers
		int rc = zmq_poll(items, zlist_size(workers) ? 2 : 1, -1);
		if (rc == -1)
			break;              //  Interrupted

								//  Handle worker activity on backend
		if (items[0].revents & ZMQ_POLLIN) {
			//  Use worker identity for load-balancing
			zmsg_t *msg = zmsg_recv(backend);
			if (!msg)
				break;          //  Interrupted

			cout << "from backend msg size: " << zmsg_size(msg) << endl;
			//FILE *fp;
			//if (fp = fopen("123.txt", "wb"))
			//	puts("打开文件成功\n");
			//else
			//	puts("打开文件成败\n");
			//zmsg_save(msg, fp);

			cout << "from backend msg size: " << zframe_strhex(zmsg_first(msg))<<endl;
#if 0
								// zmsg_unwrap is DEPRECATED as over-engineered, poor style
			zframe_t *identity = zmsg_unwrap(msg);
#else
			zframe_t *identity = zmsg_pop(msg);
			zframe_t *delimiter = zmsg_pop(msg);
			zframe_destroy(&delimiter);
#endif

			zlist_append(workers, identity);

			//  Forward message to client if it's not a READY
			zframe_t *frame = zmsg_last(msg);
			cout << "from backend msg size: " << zmsg_size(msg) << endl;;
			if (memcmp(zframe_data(frame), WORKER_READY, strlen(WORKER_READY)) == 0) {
				zmsg_destroy(&msg);
			}
			else {
				cout << "to frontend msg size: " << zmsg_size(msg) << endl;;
				zmsg_send(&msg, frontend);
				if (--client_nbr == 0)
					break; // Exit after N messages
			}
		}
		if (items[1].revents & ZMQ_POLLIN) {
			//  Get client request, route to first available worker
			zmsg_t *msg = zmsg_recv(frontend);
			//cout << "broker from frontend msg size: " << zmsg_size(msg) << endl;
			if (msg) {
#if 0
				// zmsg_wrap is DEPRECATED as unsafe
				zmsg_wrap(msg, (zframe_t *)zlist_pop(workers));
#else
				zmsg_pushmem(msg, NULL, 0); // delimiter
				zmsg_push(msg, (zframe_t *)zlist_pop(workers));
#endif

				zmsg_send(&msg, backend);
			}
		}
	}
	//  When we're done, clean up properly
	while (zlist_size(workers)) {
		zframe_t *frame = (zframe_t *)zlist_pop(workers);
		zframe_destroy(&frame);
	}
	zlist_destroy(&workers);
	zctx_destroy(&ctx);
	return 0;
}

//#include "czmq.h"
//#include <iostream>
//using namespace std;
////  This is our client task
////  It connects to the server, and then sends a request once per second
////  It collects responses as they arrive, and it prints them out. We will
////  run several client tasks in parallel, each with a different random ID.
//
//static void *
//client_task(void *args)
//{
//	zctx_t *ctx = zctx_new();
//	void *client = zsocket_new(ctx, ZMQ_DEALER);
//	zsys_set_logstream(stdout);
//	//  Set random identity to make tracing easier
//	char identity[10];
//	sprintf(identity, "%04X-%04X", randof(0x10000), randof(0x10000));
//	zsocket_set_identity(client, identity);
//	zsocket_connect(client, "tcp://localhost:5570");
//
//	zmq_pollitem_t items[] = { { client, 0, ZMQ_POLLIN, 0 } };
//	int request_nbr = 0;
//	while (true) {
//		//  Tick once per second, pulling in arriving messages
//		int centitick;
//		//for (centitick = 0; centitick < 100; centitick++) {
//		//	zmq_poll(items, 1, 1 * ZMQ_POLL_MSEC);
//		//	if (items[0].revents & ZMQ_POLLIN) {
//		//		zmsg_t *msg = zmsg_recv(client);
//		//		cout << "client: " << zmsg_size(msg) << "client: "<< zframe_strdup(zmsg_last(msg))<<endl;
//		//		zframe_print(zmsg_last(msg), identity);
//		//		zmsg_destroy(&msg);
//		//	}
//		//}
//		zclock_sleep(randof(100) + 1);
//		zstr_sendf(client, "request #%d", ++request_nbr);
//		zmq_poll(items, 1, 100 * ZMQ_POLL_MSEC);
//		if (items[0].revents & ZMQ_POLLIN) {
//			zmsg_t *msg = zmsg_recv(client);
//			cout << "client: " << zmsg_size(msg) << endl;
//			zframe_print(zmsg_last(msg), identity);
//			zmsg_destroy(&msg);
//		}
//		zclock_sleep(randof(100) + 1);
//		zstr_sendf(client, "request #%d", ++request_nbr);
//
//		zmq_poll(items, 1, 100 * ZMQ_POLL_MSEC);
//		if (items[0].revents & ZMQ_POLLIN) {
//			zmsg_t *msg = zmsg_recv(client);
//			cout << "client: " << zmsg_size(msg) << endl;
//			zframe_print(zmsg_last(msg), identity);
//			zmsg_destroy(&msg);
//		}
//		zclock_sleep(randof(100) + 1);
//		zstr_sendf(client, "request #%d", ++request_nbr);
//	}
//	zctx_destroy(&ctx);
//	return NULL;
//}
//
////  This is our server task.
////  It uses the multithreaded server model to deal requests out to a pool
////  of workers and route replies back to clients. One worker can handle
////  one request at a time but one client can talk to multiple workers at
////  once.
//
//static void server_worker(void *args, zctx_t *ctx, void *pipe);
//
//void *server_task(void *args)
//{
//	//  Frontend socket talks to clients over TCP
//	zctx_t *ctx = zctx_new();
//	void *frontend = zsocket_new(ctx, ZMQ_ROUTER);
//	zsocket_bind(frontend, "tcp://*:5570");
//
//	//  Backend socket talks to workers over inproc
//	void *backend = zsocket_new(ctx, ZMQ_DEALER);
//	zsocket_bind(backend, "tcp://*:5580");
//
//	//  Launch pool of worker threads, precise number is not critical
//	int thread_nbr;
//	for (thread_nbr = 0; thread_nbr < 5; thread_nbr++)
//		zthread_fork(ctx, server_worker, NULL);
//
//	//  Connect backend to frontend via a proxy
//	zmq_proxy(frontend, backend, NULL);
//
//	zctx_destroy(&ctx);
//	return NULL;
//}
//
////  Each worker task works on one request at a time and sends a random number
////  of replies back, with random delays between replies:
//
//static void
//server_worker(void *args, zctx_t *ctx, void *pipe)
//{
//	void *worker = zsocket_new(ctx, ZMQ_DEALER);
//	zsocket_connect(worker, "tcp://127.0.0.1:5580");
//
//	while (true) {
//		//  The DEALER socket gives us the reply envelope and message
//		zmsg_t *msg = zmsg_recv(worker);
//		zframe_t *identity = zmsg_pop(msg);
//		zframe_t *content = zmsg_pop(msg);
//		char *content1 = zframe_strdup(content);
//		cout << "worker: " << content1 << endl;
//		assert(content);
//		zmsg_destroy(&msg);
//
//		//  Send 0..4 replies back
//		int reply, replies = randof(5);
//		for (reply = 0; reply < 5; reply++) {
//			//  Sleep for some fraction of a second
//			zclock_sleep(randof(100) + 1);
//			zframe_send(&identity, worker, ZFRAME_REUSE + ZFRAME_MORE);
//			zframe_send(&content, worker, ZFRAME_REUSE);
//		}
//		zframe_destroy(&identity);
//		zframe_destroy(&content);
//	}
//}
//
////  The main thread simply starts several clients and a server, and then
////  waits for the server to finish.
//
//int main(void)
//{
//	zthread_new(client_task, NULL);
//	zthread_new(client_task, NULL);
//	zthread_new(client_task, NULL);
//	zthread_new(server_task, NULL);
//	zclock_sleep(1 * 1000);    //  Run for 5 seconds then quit
//	return 0;
//}