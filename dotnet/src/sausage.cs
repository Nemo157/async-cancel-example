using System;
using System.Threading;
using System.Threading.Tasks;

// A small AMQP like message bus
public interface IMessageBus {
    Task Publish(string topic, string message);
    Task Publish(string topic, string message, CancellationToken cancel);
    Task<IMessageQueue> Bind(string topic);
    Task<IMessageQueue> Bind(string topic, CancellationToken cancel);
    Task Unbind(IMessageQueue queue);
    Task Unbind(IMessageQueue queue, CancellationToken cancel);
}

public interface IMessageQueue {
    Task<string> Dequeue();
    Task<string> Dequeue(CancellationToken cancel);
}

// A small HTTP server like interface
public struct Request {
    // Imagine this was a query parameter or something
    public string Foo;
}

public struct Response {
    public string Body;
}

public interface IHandler {
    Task<Response> Handle(Request request, CancellationToken cancel);
}

// The handler
public class Handler: IHandler {
    int counter;
    readonly IMessageBus bus;

    public Handler(IMessageBus bus) {
        this.bus = bus;
    }

    public async Task<Response> Handle(Request request, CancellationToken cancel) {
        var my_topic = "/handler/responses/" + this.counter++;
        var my_queue = await this.bus.Bind(my_topic);
        try {
            await this.bus.Publish("/service", my_topic + "|" + request.Foo);
            string their_queue;
            for (;;) {
                var msg = (await my_queue.Dequeue(cancel)).Split('|');
                switch (msg[0]) {
                case "started":
                    their_queue = msg[1];
                    break;
                case "finished":
                    return new Response { Body = msg[1] };
                }
            }
        } finally {
            await this.bus.Unbind(my_queue);
        }
    }
}

// The other service
public class Service {
    int counter;
    readonly IMessageBus bus;

    public Service(IMessageBus bus) {
        this.bus = bus;
    }

    public async Task Run(CancellationToken cancel) {
        var queue = await this.bus.Bind("/service");
        try {
            for (;;) {
                var msg = await queue.Dequeue(cancel);
                var s = msg.Split('|');
                await Operation(s[0], int.Parse(s[1]), cancel);
            }
        } finally {
            await this.bus.Unbind(queue);
        }
    }

    async Task Operation(string their_topic, int foo, CancellationToken cancel) {
        var my_topic = "/service/responses/" + this.counter++;
        var my_queue = await this.bus.Bind(my_topic);
        try {
            await this.bus.Publish(their_topic, "started|" + my_topic);
            var operation_cancel = CancellationTokenSource.CreateLinkedTokenSource(cancel);
            var operation = RunOperation(foo, operation_cancel.Token);
            for (;;) {
                var msg = my_queue.Dequeue(cancel);
                var first = await Task.WhenAny(operation, msg);
                if (first == msg && await msg == "cancel") {
                    operation_cancel.Cancel();
                    await this.bus.Publish(their_topic, "cancelled");
                    return;
                }
                if (first == operation) {
                    await this.bus.Publish(their_topic, "finished|" + await operation);
                    return;
                }
            }
        } finally {
            await this.bus.Unbind(my_queue);
        }
    }

    async Task<string> RunOperation(int foo, CancellationToken cancel) {
        await Task.Delay(foo, cancel);
        return foo.ToString();
    }
}

public class Example {
    public static void Run() {
        var bus = new MessageBus();
        var handler = new Handler(bus);
        var service = new Service(bus);

        var running = Task.Run(() => service.Run(CancellationToken.None));

        // First example, simple 100ms delay completing successfully
        var example = Task.Run(() => handler.Handle(new Request { Foo = "100" }, CancellationToken.None));
        example.Wait();
        Console.WriteLine("First example result: {0}", example.Result);
    }
}
