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

const LINK_CLASS_FETCH = 'f';
const LINK_CLASS_MUTEX = 'm';

export class APIConnection {
    constructor(options) {
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
        this.amqpConnection = this.container.connect(options);

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

    get_stats() {
        return {
            server_endpoint_count : Object.keys(this.server_endpoints).length,
            client_endpoint_count : Object.keys(this.client_endpoints).length,
            in_flight_count       : Object.keys(this.in_flight).length,
        };
    }

    //
    // Server Side - Establish an endpoint for receiving API requests
    //
    server_endpoint(_address) {
        const address = _address[0] == '/' ? _address : '/' + _address;
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
    client_endpoint(_address) {
        const address = _address[0] == '/' ? _address : '/' + _address;
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
                    client._on_reply_addr_ready();
                }
                //console.log(`reply-to: ${this.reply_to}`);
            }
        });
        this.container.on('sendable', async (context) => {
            if (context.sender.__endpoint) {
                context.sender.__endpoint._on_sendable(context.sender);
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
        this.connection   = connection;
        this.address      = address;
        this.in_flight    = {};
        this.sessions     = {
            [LINK_CLASS_FETCH] : connection.amqpConnection.create_session(),
            [LINK_CLASS_MUTEX] : connection.amqpConnection.create_session(),
        }
        this.senders      = {
            [LINK_CLASS_FETCH] : this.sessions[LINK_CLASS_FETCH].open_sender({ target : `${LINK_CLASS_FETCH}${address}` }),
            [LINK_CLASS_MUTEX] : this.sessions[LINK_CLASS_MUTEX].open_sender({ target : `${LINK_CLASS_MUTEX}${address}` }),
        };
        this.outgoing     = {
            [LINK_CLASS_FETCH] : [],
            [LINK_CLASS_MUTEX] : [],
        };

        for (let [lcls, sender] of Object.entries(this.senders)) {
            sender.__link_class = lcls;
            sender.__endpoint   = this;
        }

        for (const session of Object.values(this.sessions)) {
            session.begin();
        }
    }

    //
    // Perform a REST/CRUD-style operation on a resource.
    //
    fetch(path, args={}) {
        return new Promise((resolve, reject) => {
            //
            // Establish the default options and override them with the arguments supplied.
            //
            let config = {
                op      : 'GET',
                timeout : DEFAULT_TIMEOUT_MSEC,
            }
            for (const [key, val] of Object.entries(args)) {
                config[key] = val;
            }

            //
            // Set up a timer to handle the timeout failure.
            //
            const timer = setTimeout(() => {
                delete this.in_flight[cid];
                this.connection._cancel_cid(cid);
                reject(new Error('Operation timed out without a response from the server'));
            }, config.timeout);

            //
            // Get a unique correlation-id for this request and record the response-handler for this request.
            //
            const cid = this.connection._new_cid(this);
            this.in_flight[cid] = (context) => {
                clearTimeout(timer);
                delete this.in_flight[cid];
                resolve(new FetchResult(context.message));
            };

            //
            // Compose the request message for this operation.
            //
            let request = {
                correlation_id : cid,
                application_properties : {
                    op   : config.op,
                    path : path,
                },
                body : config.body,
            };

            //
            // Enqueue the request for this link class and poke the sending process to flush it
            // out in case it is possible to send now.
            //
            this.outgoing[LINK_CLASS_FETCH].push(new OutgoingMessage(request));
            this._on_sendable(this.senders[LINK_CLASS_FETCH]);
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
            //
            // Establish default options and overwrite with the supplied arguments
            //
            let config = { timeout : DEFAULT_TIMEOUT_MSEC };
            for (const [k,v] of Object.entries(args)) {
                config[k] = v;
            }

            //
            // If a timeout is specified, set a timer to handle the timeout error.
            //
            let timer;
            if (config.timeout > 0) {
                timer = setTimeout(() => {
                    delete this.in_flight[cid];
                    this.connection._cancel_cid(cid);
                    reject(new Error('Timed out waiting for the mutex.  Critical section did not run.'));
                }, config.timeout);
            }

            //
            // Get a unique correlation-id and register a response handler.
            //
            const cid = this.connection._new_cid(this);
            let   request_delivery;
            let   inner_completed = false;
            this.in_flight[cid] = async (context) => {
                if (timer) {
                    clearTimeout(timer);
                }
                const ap = context.message.application_properties;
                if (ap.status == 200) {
                    const rval = await inner(ap.acquisition_id);
                    inner_completed = true;

                    //
                    // Settle the delivery for the request message, signaling the release of the mutex.
                    //
                    if (request_delivery) {
                        request_delivery.update(true);
                    }
                    resolve(rval);
                } else {
                    reject(new Error(`Mutex error: (${ap.status}) ${ap.status_description}`));
                }
            };

            //
            // Compose the request message.
            //
            let request = {
                correlation_id : cid,
                application_properties : {
                    op         : 'acquire',
                    path       : path,
                    mutex_name : mutex_name,
                },
                body : config.body,
            };

            //
            // Enqueue the request along with a disposition-update handler to track changes to the request delivery.
            //
            this.outgoing[LINK_CLASS_MUTEX].push(new OutgoingMessage(request, (delivery, state) => {
                if (state == STATE_ACCEPTED) {
                    //
                    // The request has been accepted.  This means it will be processed by the server.
                    // Store the delivery so we can change its disposition later.
                    //
                    request_delivery = delivery;
                    if (inner_completed) {
                        delivery.settled = true;
                    }
                }
                if (delivery.remote_settled && !delivery.settled) {
                    //
                    // The server (or network) settled the delivery before we did.  This means that the
                    // mutex has been effectively dropped without our input.  Call the on_cancel handler.
                    //
                    delivery.settled = true;
                    on_cancel();
                    reject(new Error('Mutex was dropped prematurely'));
                }
            }));

            //
            // Kick the delivery-send process.
            //
            this._on_sendable(this.senders[LINK_CLASS_MUTEX]);
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

    _on_reply_addr_ready() {
        for (const sender of Object.values(this.senders)) {
            this._on_sendable(sender);
        }
    }

    _on_sendable(sender) {
        const link_class = sender.__link_class;
        if (!this.connection.reply_to || link_class == undefined) {
            return;
        }

        let credit = sender.credit;
        while (credit > 0 && this.outgoing[link_class].length > 0) {
            const outgoing = this.outgoing[link_class].shift();
            credit -= 1;
            outgoing.message.reply_to = this.connection.reply_to;
            let delivery = sender.send(outgoing.message);
            delivery.__on_update = outgoing.on_update;
        }
    }
}

export class ServerEndpoint {
    constructor(connection, address) {
        this.connection = connection;
        this.address    = address;
        this.path_tree  = new Path();
        this.sessions     = {
            [LINK_CLASS_FETCH] : this.connection.amqpConnection.create_session(),
            [LINK_CLASS_MUTEX] : this.connection.amqpConnection.create_session(),
        }
        this.receivers  = {
            [LINK_CLASS_FETCH] : this.sessions[LINK_CLASS_FETCH].open_receiver({
                source: `${LINK_CLASS_FETCH}${address}`,
                autoaccept: false,  // We will explicitly handle delivery disposition
                autosettle: false,  // We will explicitly handle delivery settlement
                rcv_settle_mode: 1, // Don't automatically settle when terminal disposition is set on a delivery
            }),
            [LINK_CLASS_MUTEX] : this.sessions[LINK_CLASS_MUTEX].open_receiver({
                source: `${LINK_CLASS_MUTEX}${address}`,
                autoaccept: false,  // We will explicitly handle delivery disposition
                autosettle: false,  // We will explicitly handle delivery settlement
                rcv_settle_mode: 1, // Don't automatically settle when terminal disposition is set on a delivery
            }),
        }
        for (let receiver of Object.values(this.receivers)) {
            receiver.__endpoint = this;
        }

        for (const session of Object.values(this.sessions)) {
            session.begin();
        }
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