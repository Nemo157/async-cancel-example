using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Nito.AsyncEx;

public static class Program {
    public static readonly DateTime Start = DateTime.Now;
    static void Main(string[] args) {
        Example.Run();
    }
}

public class MessageBus: IMessageBus {
    readonly ConcurrentDictionary<string, AsyncProducerConsumerQueue<string>> queues = new ConcurrentDictionary<string, AsyncProducerConsumerQueue<string>>();

    public async Task Publish(string topic, string message) {
        await Publish(topic, message, CancellationToken.None);
    }

    public async Task Publish(string topic, string message, CancellationToken cancel) {
        await Task.Delay(10, cancel);
        var queue = this.queues.GetOrAdd(topic, _ => new AsyncProducerConsumerQueue<string>());
        await queue.EnqueueAsync(message, cancel);
    }

    public async Task<string> Receive(string topic) {
        return await Receive(topic, CancellationToken.None);
    }

    public async Task<string> Receive(string topic, CancellationToken cancel) {
        var queue = this.queues.GetOrAdd(topic, _ => new AsyncProducerConsumerQueue<string>());
        return await queue.DequeueAsync(cancel);
    }
}

public class Service {
    readonly IMessageBus bus;
    readonly Action<string> log = msg => Console.WriteLine($"{(DateTime.Now - Program.Start):s'.'fff} [Service] {msg}");
    readonly CancellationTokenSource Cancel = new CancellationTokenSource();

    public Service(IMessageBus bus) {
        this.bus = bus;
    }

    public async Task Run() {
        log($"[Run] Service starting");
        while (!this.Cancel.IsCancellationRequested) {
            try {
                var msg = await this.bus.Receive("/service", this.Cancel.Token);
                log($"[Run] Got start message: {msg}");
                await Operation(int.Parse(msg));
                log($"[Run] Handled msg");
            } catch (OperationCanceledException) {
            }
        }
        log($"[Run] Service stopping");
    }

    public void Stop() {
        this.Cancel.Cancel();
    }

    async Task Operation(int foo) {
        log($"[Operation] Starting operation");
        var op_cancel = new CancellationTokenSource();
        var operation = RunOperation(foo, op_cancel.Token);
        for (;;) {
            var msg_cancel = new CancellationTokenSource();
            var msg = this.bus.Receive("/service/op/cancel", msg_cancel.Token);
            var first = await Task.WhenAny(operation, msg);
            if (first == msg && await msg == "cancel") {
                log($"[Operation] Received cancel msg");
                op_cancel.Cancel();
                return;
            }
            msg_cancel.Cancel();
            if (first == operation) {
                log($"[Operation] finished");
                await this.bus.Publish($"/service/op", operation.Result);
                return;
            }
        }
    }

    async Task<string> RunOperation(int foo, CancellationToken cancel) {
        log($"[RunOperation] Delaying for: {foo}");
        try {
            await Task.Delay(foo, cancel);
        } catch (OperationCanceledException) {
            log($"[RunOperation] Cancelled");
            throw;
        }
        log($"[RunOperation] Finished");
        return foo.ToString();
    }
}

public static class AggregateExceptionExtensions {
    public static Exception Unwrap(this AggregateException @this) {
        return @this.InnerExceptions.Count == 1 ? @this.InnerExceptions[0] : @this;
    }
}
