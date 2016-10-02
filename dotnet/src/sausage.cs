using System;
using System.Threading;
using System.Threading.Tasks;

// A small AMQP like message bus
public interface IMessageBus {
    Task Publish(string topic, string message);
    Task Publish(string topic, string message, CancellationToken cancel);
    Task<string> Receive(string topic);
    Task<string> Receive(string topic, CancellationToken cancel);
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
    readonly IMessageBus bus;
    readonly Action<string> log = msg => Console.WriteLine($"{(DateTime.Now - Program.Start):s'.'fff} [Handler] {msg}");

    public Handler(IMessageBus bus) {
        this.bus = bus;
    }

    public async Task<Response> Handle(Request request, CancellationToken cancel) {
        log($"Got request with Foo: {request.Foo}");
        log($"Starting request to service");
        await this.bus.Publish("/service", request.Foo, cancel);
        for (;;) {
            try {
                var msg = await this.bus.Receive($"/service/op", cancel);
                log($"Finished request with result: {msg}");
                return new Response { Body = msg };
            } catch (OperationCanceledException) {
                log($"Cancellation token was set, cancelling service");
                await this.bus.Publish($"/service/op/cancel", "cancel");
                throw;
            }
        }
    }
}

public class Example {
    public static void Run() {
        var bus = new MessageBus();
        var handler = new Handler(bus);
        var service = new Service(bus);

        var running = Task.Run(() => service.Run(CancellationToken.None));

        Func<Task<Response>, object> wait = task => {
            try { return task.Result; }
            catch (AggregateException ex) { return ex.Unwrap(); }
        };

        // First example, simple 100ms delay completing successfully
        {
            var example = Task.Run(() => handler.Handle(new Request { Foo = "100" }, CancellationToken.None));
            var result = wait(example);
            Console.WriteLine($"First example result: {result}");
        }

        Console.WriteLine();

        // Second example, 200ms delay cancelled after 100ms
        {
            var source = new CancellationTokenSource(100);
            var example = Task.Run(() => handler.Handle(new Request { Foo = "200" }, source.Token));
            var result = wait(example);
            Console.WriteLine($"Second example result: {result}");
        }

        service.Stop();
        running.Wait();
    }
}
