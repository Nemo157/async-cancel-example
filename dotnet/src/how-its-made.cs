using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Nito.AsyncEx;

static class Program {
    static void Main(string[] args) {
        Example.Run();
    }
}

public class MessageBus: IMessageBus {
    readonly Dictionary<string, List<MessageQueue>> bindings = new Dictionary<string, List<MessageQueue>>();

    public async Task Publish(string topic, string message) {
        await Publish(topic, message, CancellationToken.None);
    }

    public async Task Publish(string topic, string message, CancellationToken cancel) {
        await Task.Delay(10, cancel);
        List<MessageQueue> queues;
        if (this.bindings.TryGetValue(topic, out queues)) {
            await Task.WhenAll(queues.Select(queue => queue.Enqueue(message, cancel)));
        }
    }

    public async Task<IMessageQueue> Bind(string topic) {
        return await Bind(topic, CancellationToken.None);
    }

    public async Task<IMessageQueue> Bind(string topic, CancellationToken cancel) {
        await Task.Delay(10, cancel);
        var queue = new MessageQueue(topic);
        List<MessageQueue> queues;
        if (!this.bindings.TryGetValue(topic, out queues)) {
            this.bindings[topic] = queues = new List<MessageQueue>();
        }
        queues.Add(queue);
        return queue;
    }

    public async Task Unbind(IMessageQueue queue) {
        await Unbind(queue, CancellationToken.None);
    }

    public async Task Unbind(IMessageQueue queue, CancellationToken cancel) {
        await Task.Delay(10, cancel);
        this.bindings[((MessageQueue)queue).Topic].Remove((MessageQueue)queue);
    }
}

public class MessageQueue: IMessageQueue {
    readonly string topic;
    readonly AsyncProducerConsumerQueue<string> queue = new AsyncProducerConsumerQueue<string>();

    public MessageQueue(string topic) {
        this.topic = topic;
    }
    
    public string Topic { get { return this.topic; } }

    public async Task<string> Dequeue() {
        return await Dequeue(CancellationToken.None);
    }

    public async Task<string> Dequeue(CancellationToken cancel) {
        return await queue.DequeueAsync(cancel);
    }

    public async Task Enqueue(string message, CancellationToken cancel) {
        await queue.EnqueueAsync(message, cancel);
    }
}
