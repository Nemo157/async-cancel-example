extern crate futures;
extern crate tokio_core;

mod internal;

use std::mem;
use futures::{ Future, Poll, Async };
use tokio_core::reactor::Core;
use internal::{ Service, ExampleMessageBus };

pub trait MessageBus {
    fn publish(&self, topic: &str, message: String) -> Box<Future<Item=(), Error=()>>;
    fn receive(&self, topic: &str) -> Box<Future<Item=String, Error=()>>;
}

// A small HTTP server like interface
struct Request {
    // Imagine this was a query parameter or something
    pub foo: String,
}

#[derive(Debug)]
struct Response {
    pub body: String,
}

trait Handler {
    fn handle<'a>(&'a self, request: Request) -> Box<Future<Item=Response, Error=()> + 'a>;
}

// The handler
struct ExampleHandler<'a> {
    pub bus: &'a MessageBus,
}

impl<'a> Handler for ExampleHandler<'a> {
    fn handle<'b>(&'b self, request: Request) -> Box<Future<Item=Response, Error=()> + 'b> {
        // log($"Got request with Foo: {request.Foo}");
        return Box::new(ResponseFuture {
            handler: self,
            state: State::Publish { foo: request.foo }
        });

        enum State {
            Publish { foo: String },
            Publishing { publish: Box<Future<Item=(), Error=()>> },
            Receiving { receive: Box<Future<Item=String, Error=()>> },
            Consumed,
        }

        struct ResponseFuture<'a, 'b: 'a> {
            handler: &'a ExampleHandler<'b>,
            state: State,
        }

        impl<'a, 'b> Future for ResponseFuture<'a, 'b> {
            type Item = Response;
            type Error = ();

            fn poll(&mut self) -> Poll<Response, ()> {
                // Recursion here is ok as it's one way through non-recursive
                // states, so can recurse a max of 2 times if the publish is
                // instantaneously complete
                match mem::replace(&mut self.state, State::Consumed) {
                    State::Publish { foo } => {
                        // log($"Starting request to service");
                        self.state = State::Publishing {
                            publish: self.handler.bus.publish("/service", foo)
                        };
                        self.poll()
                    }
                    State::Publishing { mut publish } => {
                        match try!(publish.poll()) {
                            Async::NotReady => {
                                self.state = State::Publishing { publish: publish };
                                Ok(Async::NotReady)
                            }
                            Async::Ready(()) => {
                                self.state = State::Receiving {
                                    receive: self.handler.bus.receive("/service/op"),
                                };
                                self.poll()
                            }
                        }
                    }
                    State::Receiving { mut receive } => {
                        match try!(receive.poll()) {
                            Async::NotReady => {
                                self.state = State::Receiving { receive: receive };
                                Ok(Async::NotReady)
                            }
                            Async::Ready(msg) => {
                                // log($"Finished request with result: {msg}");
                                Ok(Async::Ready(Response { body: msg }))
                            }
                        }
                    }
                    State::Consumed => {
                        panic!("Already consumed")
                    }
                }
            }
        }

        impl<'a, 'b> Drop for ResponseFuture<'a, 'b> {
            fn drop(&mut self) {
                match self.state {
                    State::Publish { .. } => {
                        // Not started request yet, no need to do anything
                    }
                    State::Publishing { .. } => {
                        // May or may not have actually published on the bus yet,
                        // could be nice to try and cancel here but probably not
                        // needed. Dropping the `publish` future has a high
                        // probability of cancelling the publish (depends heavily
                        // on how the underlying message bus & message bus client
                        // work).
                    }
                    State::Receiving { .. } => {
                        // Here's the most important point to cancel, the message
                        // has definitely been published so the service will be
                        // working on the operation, so we need to cancel it to
                        // reduce it's load, but since the cancellation involves
                        // sending an asynchronous message how do we do that?

                        // This will start publishing, but will then be dropped
                        // straight away and cancel the cancel....

                        // log($"ResponseFuture was dropped, cancelling service");
                        self.handler.bus.publish("/service/op/cancel", "cancel".to_owned());
                    }
                    State::Consumed => {
                        // Completed request, no need to do anything
                    }
                }
            }
        }
    }
}

fn main() {
    let mut event_loop = Core::new().unwrap();

    let bus = ExampleMessageBus { };
    let handler = ExampleHandler { bus: &bus };
    let service = Service::new(&bus, event_loop.handle());

    // TODO: run this on an executor or something?
    let running = service.run();

    // First example, simple 100ms delay completing successfully
    let example = handler.handle(Request { foo: "100".to_owned() });
    let (result, running) = match event_loop.run(example.map(Some).select(running.map(|_| None))) {
        Ok((result, next)) => (Ok(result), next),
        Err((result, next)) => (Err(result), next),
    };
    println!("First example result: {:?}", result);

    println!("");

    // // Second example, 200ms delay cancelled after 100ms
    // {
    //     let source = new CancellationTokenSource(100);
    //     let example = Task.Run(() => handler.Handle(new Request { Foo = "200" }, source.Token));
    //     let result = wait(example);
    //     Console.WriteLine($"Second example result: {result}");
    // }

    service.stop();
    event_loop.run(running).unwrap();
}
