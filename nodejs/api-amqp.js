/*
 Licensed to the Apache Software Foundation (ASF) under one
 or more contributor license agreements.  See the NOTICE file
 distributed with this work for additional information
 regarding copyright ownership.  The ASF licenses this file
 to you under the Apache License, Version 2.0 (the
 "License"); you may not use this file except in compliance
 with the License.  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing,
 software distributed under the License is distributed on an
 "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 KIND, either express or implied.  See the License for the
 specific language governing permissions and limitations
 under the License.
*/

"use strict";

import rhea from "rhea";

const DEFAULT_TIMEOUT_MSEC = 10000;

const STATE_ACCEPTED = 1;
const STATE_REJECTED = 2;
const STATE_RELEASED = 3;
const STATE_MODIFIED = 4;

export class APIConnection {
    constructor(host='localhost', port='5672', transport='tcp', ca=undefined, cert=undefined, key=undefined) {
        //
        // Create an AMQP container dedicated to this API connection
        //
        this.container = rhea.create_container({enable_sasl_external: true});
        this._setup_handlers();

        //
        // Initialize internal state
        //
        this.reply_to = undefined;
        this.server_endpoints = {};
        this.client_endpoints = {};
        this.in_flight        = {};
        this.next_cid         = 1;

        //
        // Open the AMQP connection to the network
        //
        this.amqpConnection = this.container.connect({
            host      : host,
            hostname  : host,
            transport : transport,
            port      : port,
            ca        : ca,
            key       : key,
            cert      : cert,
        });

        //
        // Set up a receiver with a dynamic address on which to receive replies
        //
        this.replyReceiver = this.amqpConnection.open_receiver({source:{dynamic:true}});

        //
        // Set up an anonymous sender over which to send addressed messages
        //
        this.anonSender = this.amqpConnection.open_sender();
    }

    close() {
        this.amqpConnection.close();
    }

    //
    // Server Side - Establish an endpoint for receiving API requests
    //
    server_endpoint(address) {
        if (!this.server_endpoints[address]) {
            let e = new ServerEndpoint(this, address);
            this.server_endpoints[address] = e;
            return e;
        } else {
            throw new Error(`Already a server endpoint on address ${address}`);
        }
    }

    //
    // Client Side - Establish a portal to a server endpoint.  This will use an addressed sender link
    // and will provide flow control back pressure to the application.
    //
    client_endpoint(address) {
        if (!this.client_endpoints[address]) {
            let e = new ClientEndpoint(this, address);
            this.client_endpoints[address] = e;
            return e;
        } else {
            throw new Error(`Already a client endpoint on address ${address}`);
        }
    }

    _new_cid(dispatch_object) {
        const cid = this.next_cid;
        this.next_cid += 1;
        this.in_flight[cid] = dispatch_object;
        return cid;
    }

    _cancel_cid(cid) {
        delete this.in_flight[cid];
    }

    _setup_handlers() {
        this.container.on('connection_open', (context) => {});
        this.container.on('receiver_open', async (context) => {
            if (context.receiver == this.replyReceiver) {
                this.reply_to = context.receiver.source.address;
                for (const client of Object.values(this.client_endpoints)) {
                    client._on_sendable();
                }
                //console.log(`reply-to: ${this.reply_to}`);
            }
        });
        this.container.on('sendable', async (context) => {
            if (context.sender.__endpoint) {
                context.sender.__endpoint._on_sendable();
            }
        });
        this.container.on('message', async (context) => {
            try {
                if (context.receiver == this.replyReceiver) {
                    const cid         = context.message.correlation_id;
                    const destination = this.in_flight[cid];
                    if (destination) {
                        await destination._dispatch(context);
                    } else {
                        throw new Error(`Reply message for unknown correlation_id ${cid}`);
                    }
                }
                else if (context.receiver.__endpoint) {
                    await context.receiver.__endpoint._dispatch(context);
                } else {
                    throw new Error(`Message received for which there is no registered endpoint`);
                }
            } catch (err) {
                console.log(`Exception in API message dispatch: ${err.stack}`);
                context.delivery.reject();
                context.delivery.settled = true;
            }
        });
        this.container.on('accepted', async (context) => {
            let delivery = context.delivery;
            if (delivery.__on_update) {
                delivery.__on_update(delivery, STATE_ACCEPTED);
            }
        });
        this.container.on('rejected', async (context) => {
            let delivery = context.delivery;
            if (delivery.__on_update) {
                delivery.__on_update(delivery, STATE_REJECTED);
            }
        });
        this.container.on('released', async (context) => {
            let delivery = context.delivery;
            if (delivery.__on_update) {
                delivery.__on_update(delivery, STATE_RELEASED);
            }
        });
        this.container.on('modified', async (context) => {
            let delivery = context.delivery;
            if (delivery.__on_update) {
                delivery.__on_update(delivery, STATE_MODIFIED);
            }
        });
        this.container.on('settled', async (context) => {
            let delivery = context.delivery;
            if (delivery.__on_update) {
                delivery.__on_update(delivery);
            }
        });
    }
}

export class FetchResult {
    constructor(message) {
        this.message = message;
    }

    status() {
        return this.message.application_properties.status;
    }

    obj() {
        return this.message.body;
    }
}

class OutgoingMessage {
    constructor(message, on_update) {
        this.message   = message;
        this.on_update = on_update;
    }
}

export class ClientEndpoint {
    constructor(connection, address) {
        this.connection = connection;
        this.address    = address;
        this.outgoing   = [];
        this.in_flight  = {};
        this.sender     = connection.amqpConnection.open_sender({
            target : address,
        });
        this.sender.__endpoint = this;
    }

    //
    // Perform a REST-style operation on a resource.
    //
    fetch(path, args={}) {
        return new Promise((resolve, reject) => {
            let config = {
                op      : 'GET',
                timeout : DEFAULT_TIMEOUT_MSEC,
            }
            for (const [key, val] of Object.entries(args)) {
                config[key] = val;
            }

            const timer = setTimeout(() => {
                delete this.in_flight[cid];
                reject(new Error('Operation timed out without a response from the server'));
            }, config.timeout);

            const cid = this.connection._new_cid(this);
            this.in_flight[cid] = (context) => {
                clearTimeout(timer);
                resolve(new FetchResult(context.message));
            };
            let request = {
                correlation_id : cid,
                application_properties : {
                    op   : config.op,
                    path : path,
                },
                body : config.body,
            };
            this.outgoing.push(new OutgoingMessage(request));
            this._on_sendable();
        });
    }

    //
    // Start a watch for unsolicited updates on the state of a resource.
    //
    watch(path, args={}) {}

    //
    // Run a critical section with an acquired mutex.
    //   path => The API path of the mutex to be acquired
    //   inner => The critical section function (must be an async function)
    //   on_cancel => handler called if the mutex is dropped by the server
    //                This must stop the execution of the 'inner' function
    //   args:
    //     timeout => time in mSec to wait for acquisition, 0 == wait forever
    //     label   => label to describe this critical section - can be used by the server
    //
    critical_section(path, mutex_name, inner, on_cancel, args={}) {
        return new Promise((resolve, reject) => {
            let config = { timeout : DEFAULT_TIMEOUT_MSEC };
            for (const [k,v] of Object.entries(args)) {
                config[k] = v;
            }

            var timer;
            if (config.timeout > 0) {
                timer = setTimeout(() => {
                    reject(new Error('Operation timed out without a response from the server'));
                }, config.timeout);
            }

            const cid = this.connection._new_cid(this);
            var   request_delivery;
            var   inner_completed = false;
            this.in_flight[cid] = async (context) => {
                if (timer) {
                    clearTimeout(timer);
                }
                const ap = context.message.application_properties;
                if (ap.status != 200) {
                    reject(new Error(`Mutex error: (${ap.status}) ${ap.status_description}`));
                } else {
                    const rval = await inner(ap.acquisition_id);
                    inner_completed = true;
                    if (request_delivery) {
                        request_delivery.update(true);
                    }
                    resolve(rval);
                }
            };
            let request = {
                correlation_id : cid,
                application_properties : {
                    op         : 'acquire',
                    path       : path,
                    mutex_name : mutex_name,
                },
                body : config.body,
            };
            this.outgoing.push(new OutgoingMessage(request, (delivery, state) => {
                if (state == STATE_ACCEPTED) {
                    request_delivery = delivery;
                    if (inner_completed) {
                        delivery.settled = true;
                    }
                }
                if (delivery.remote_settled && !delivery.settled) {
                    delivery.settled = true;
                    on_cancel();
                    reject(new Error('Mutex was dropped prematurely'));
                }
            }));
            this._on_sendable();
        });
    }

    async _dispatch(context) {
        const cid     = context.message.correlation_id;
        const handler = this.in_flight[cid];
        if (handler) {
            delete this.in_flight[cid];
            this.connection._cancel_cid(cid);
            handler(context);
        }
    }

    _on_sendable() {
        if (!this.connection.reply_to) {
            return;
        }

        while (this.sender.credit > 0 && this.outgoing.length > 0) {
            const outgoing = this.outgoing.shift();
            outgoing.message.reply_to = this.connection.reply_to;
            let delivery = this.sender.send(outgoing.message);
            delivery.__on_update = outgoing.on_update;
        }
    }
}

export class ServerEndpoint {
    constructor(connection, address) {
        this.connection = connection;
        this.address    = address;
        this.path_tree  = new Path();
        this.receiver = this.connection.amqpConnection.open_receiver({
            source: address,
            autoaccept: false,  // We will explicitly handle delivery disposition
            autosettle: false,  // We will explicitly handle delivery settlement
            rcv_settle_mode: 1, // Don't automatically settle when terminal disposition is set on a delivery
        });
        this.receiver.__endpoint = this;
    }

    route(path) {
        let n = new Node(this, path);
        const elements = path.split('/');
        this.path_tree.insert(n, elements);
        return n;
    }

    _find_path(tree, elements) {
        if (elements.length == 0) {
            return tree;
        }

        const first = elements.shift();
        if (first == '') {
            // Ignore blank elements
            return this._find_path(tree, elements);
        }

        const child = tree.get_child(first);
        return child ? this._find_path(child, elements) : child;
    }

    async _dispatch(context) {
        const pathtext = context.message.application_properties.path;
        if (pathtext) {
            const elements = pathtext.split('/');
            const path     = this._find_path(this.path_tree, elements);
            const node     = path ? path.get_node() : undefined;

            if (node) {
                await node._dispatch(context);
                return;
            }
        }

        const error_response = {
            to             : context.message.reply_to,
            correlation_id : context.message.correlation_id,
            application_properties : {
                status             : 404,
                status_description : 'Not Found',
            },
            body : "No resource found at path",
        };

        this.connection.anonSender.send(error_response);

        context.delivery.accept();
        context.delivery.settled = true;
    }
}

export class Response {
    constructor(request_message, sender) {
        this.request_message = request_message;
        this.sender          = sender;
        this.sent            = false;
        this.response_message = {
            to                     : request_message.reply_to,
            correlation_id         : request_message.correlation_id,
            application_properties : {},
            body                   : undefined,
        }
    }

    status(code) {
        if (this.sent) {
            throw new Error("Setting status on an already sent response");
        }
        this.response_message.application_properties.status = code;
        return this;
    }

    end() {
        this.send(undefined);
    }

    send(body) {
        if (this.sent) {
            throw new Error("Sending on an already sent response");
        }
        this.response_message.body = body;
        this.sender.send(this.response_message);
        this.sent = true;
    }
}

class Path {
    constructor() {
        this.node     = undefined;
        this.children = {};
    }

    insert(node, elements) {
        if (elements.length == 0) {
            // End of the line, insert here
            this.node = node;
        } else {
            let element = elements.shift();
            if (element == '') {
                // Ignore blank elements
                this.insert(node, elements);
            } else {
                if (!this.children[element]) {
                    this.children[element] = new Path();
                }
                this.children[element].insert(node, elements);
            }
        }
    }

    get_child(name) {
        return this.children[name];
    }

    get_node() {
        return this.node;
    }
}

export class Node {
    constructor(endpoint, path) {
        this.endpoint = endpoint;
        this.path = path;
        this.root_handlers = [];
        this.handlers = {
            get    : [],
            put    : [],
            post   : [],
            delete : [],
            watch  : [],
        };
        this._mutex = undefined;
    }

    get(handler) {
        this.handlers.get.push(handler);
        return this;
    }

    put(handler) {
        this.handlers.put.push(handler);
        return this;
    }

    post(handler) {
        this.handlers.post.push(handler);
        return this;
    }

    delete(handler) {
        this.handlers.delete.push(handler);
        return this;
    }

    mutex() {
        if (!this._mutex) {
            this._mutex = new Mutex(this.endpoint);
        }
        return this._mutex;
    }

    async _dispatch(context) {
        let   handled = false;
        const opcode = context.message.application_properties.op.toLowerCase();

        if (opcode == 'acquire') {
            await this._mutex._dispatch(context);
            handled = true;
        } else {
            for (const handler of this.handlers[opcode]) {
                handler(context.message, new Response(context.message, this.endpoint.connection.anonSender)); // Use the 'next' argument
                handled = true;
            }

            context.delivery.accept();
            context.delivery.settled = true;
        }

        if (!handled) {
            const response = {
                to                     : context.message.reply_to,
                correlation_id         : context.message.correlation_id,
                application_properties : { status: 400, status_description: 'Not Permitted' },
                body                   : 'Method not permitted for this resource',
            };
            this.endpoint.connection.anonSender.send(response);
        }
    }
}

export class Mutex {
    constructor(endpoint) {
        this.endpoint  = endpoint;
        this.instances = {}; // name => Named mutex instance
    }

    queryAll() {
        let list = [];
        for (const key of Object.keys(this.instances)) {
            list.push(key);
        }
        return list;
    }

    query(name) {
        if (this.instances[name]) {
            return this.instances[name].query();
        }
        return undefined;
    }

    async _dispatch(context) {
        const ap = context.message.application_properties;
        const mutexName = ap.mutex_name;
        if (!this.instances[mutexName]) {
            this.instances[mutexName] = new MutexInstance(this.endpoint);
        }
        await this.instances[mutexName]._dispatch(context);
    }
}

class MutexInstance {
    constructor(endpoint) {
        this.endpoint = endpoint;
        this.queue    = []; // {delivery, message}
    }

    //
    // Return a list of acquire requests for this named mutex.  The first item is the acquired one.
    //
    query() {
        let list = [];
        for (const request of this.queue) {
            list.push(request.message.application_properties.annotations || {});
        }
        return list;
    }

    async grant_lock() {
        const request = this.queue[0];
        request.delivery.__on_update = async (delivery, state) => {
            if (delivery.remote_settled && !delivery.settled) {
                // Mutex has been released by the client
                delivery.update(true);
                this.queue.shift();
                if (this.queue.length > 0) {
                    await this.grant_lock();
                }
            }
        };
        request.delivery.accept();
        this._send_response(request.message);
    }

    async _dispatch(context) {
        const ap = context.message.application_properties;
        let request = {
            delivery : context.delivery,
            message  : context.message,
            timer    : (this.queue.length > 0 && ap.wait_time) ? setTimeout(() => {
                // TODO
            }, ap.wait_time) : undefined,
        }
        this.queue.push(request);
        if (this.queue.length == 1) {
            await this.grant_lock();
        }
    }

    _send_response(message, status=200, description='Ok', body=undefined) {
        let resp = {
            to             : message.reply_to,
            correlation_id : message.correlation_id,
            application_properties : {
                status             : status,
                status_description : description,
                acquisition_id     : 'abcde',      // TODO - fix this
            },
            body : body,
        }

        this.endpoint.connection.anonSender.send(resp);
    }

    _remove_head() {
        this.queue.shift();
        if (this.queue.length > 0) {
            let head = this.queue[0];
            if (head.timer) {
                head.timer.cancel();
            }
            head.delivery.accept();
        }
    }

    async _release(delivery) {
        
    }
}