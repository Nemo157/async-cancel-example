use std::mem;
use std::sync::Mutex;
use std::time::Duration;
use futures::{ Future, Poll, Async };
use tokio_core::reactor::{ Handle, Timeout };

use ::MessageBus;

pub struct ExampleMessageBus {
}

impl MessageBus for ExampleMessageBus {
    fn publish(&self, topic: &str, message: String) -> Box<Future<Item=(), Error=()>> {
        unimplemented!()
    }
    fn receive(&self, topic: &str) -> Box<Future<Item=String, Error=()>> {
        unimplemented!()
    }
}

pub struct Service<'a> {
    bus: &'a MessageBus,
    handle: Handle,
    running: Mutex<bool>,
}

impl<'a> Service<'a> {
    pub fn new(bus: &'a MessageBus, handle: Handle) -> Service<'a> {
        Service {
            bus: bus,
            handle: handle,
            running: Mutex::new(false),
        }
    }

    pub fn run<'b>(&'b self) -> Box<Future<Item=(), Error=()> + 'b> {
        *self.running.lock().unwrap() = true;
        return Box::new(Runner {
            service: self,
            state: State::Start,
        });

        enum State {
            Start,
            Receiving { receive: Box<Future<Item=String, Error=()>> },
            Operation { msg: String, receive: Box<Future<Item=String, Error=()>>, op: Timeout },
            Publishing { publish: Box<Future<Item=(), Error=()>> },
            Consumed,
        }

        struct Runner<'a, 'b: 'a> {
            service: &'a Service<'b>,
            state: State,
        }

        impl<'a, 'b> Future for Runner<'a, 'b> {
            type Item = ();
            type Error = ();

            fn poll(&mut self) -> Poll<(), ()> {
                match mem::replace(&mut self.state, State::Consumed) {
                    State::Start => {
                        if *self.service.running.lock().unwrap() {
                            self.state = State::Receiving {
                                receive: self.service.bus.receive("/service"),
                            };
                            self.poll()
                        } else {
                            Ok(Async::Ready(()))
                        }
                    }

                    State::Receiving { mut receive } => {
                        match try!(receive.poll()) {
                            Async::NotReady => {
                                self.state = State::Receiving {
                                    receive: receive
                                };
                                Ok(Async::NotReady)
                            }
                            Async::Ready(msg) => {
                                // log($"[Run] Got start message: {msg}");
                                let foo = Duration::from_millis(msg.parse().unwrap());
                                // log($"[RunOperation] Delaying for: {foo}");
                                self.state = State::Operation {
                                    msg: msg,
                                    receive: self.service.bus.receive("/service"),
                                    op: Timeout::new(foo, &self.service.handle).unwrap(),
                                };
                                self.poll()
                            }
                        }
                    }

                    State::Operation { msg, mut receive, mut op } => {
                        match try!(receive.poll()) {
                            Async::NotReady => {
                                match try!(op.poll().map_err(|_| ())) {
                                    Async::NotReady => {
                                        self.state = State::Operation {
                                            msg: msg,
                                            receive: receive,
                                            op: op,
                                        };
                                        Ok(Async::NotReady)
                                    }
                                    Async::Ready(()) => {
                                        // log($"[RunOperation] Finished");
                                        self.state = State::Publishing {
                                            publish: self.service.bus.publish("/service/op", msg),
                                        };
                                        self.poll()
                                    }
                                }
                            }
                            Async::Ready(msg) => {
                                self.state = if msg == "cancel" {
                                    State::Start
                                } else {
                                    State::Operation {
                                        msg: msg,
                                        receive: receive,
                                        op: op,
                                    }
                                };
                                self.poll()
                            }
                        }
                    }

                    State::Publishing { mut publish } => {
                        match try!(publish.poll()) {
                            Async::NotReady => {
                                self.state = State::Publishing {
                                    publish: publish
                                };
                                Ok(Async::NotReady)
                            }
                            Async::Ready(()) => {
                                self.state = State::Start;
                                self.poll()
                            }
                        }
                    }

                    State::Consumed => {
                        panic!("Already consumed")
                    }
                }
            }
        }
    }

    pub fn stop(&self) {
        *self.running.lock().unwrap() = false;
    }
}
