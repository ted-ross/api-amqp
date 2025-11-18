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

export class APIConnection {
    constructor(host='localhost', port='5672', transport=undefined, ca=undefined, cert=undefined, key=undefined) {
        this.container = rhea.create_container({enable_sasl_external: true});
        this.container.on('connection_open', this._on_connection_open);
        this.container.on('receiver_open',   this._on_receiver_open);
        this.container.on('sendable',        this._on_sendable);
        this.container.on('message',         this._on_message);
        this.container.on('accepted',        this._on_accepted);
        this.container.on('rejected',        this._on_rejected);
        this.container.on('released',        this._on_released);
        this.container.on('modified',        this._on_modified);
        this.container.on('settled',         this._on_settled);

        this.amqpConnection = this.container.connect({
            host      : host,
            hostname  : host,
            transport : transport,
            port      : port,
            ca        : ca,
            key       : key,
            cert      : cert,
        });
        this.senders   = []
        this.receivers = []
        this.replyTo = undefined;
        this.replyReceiver = this.amqpConnection.open_receiver({source:{dynamic:true}});
        this.anonSender    = this.amqpConnection.open_sender();
        this.amqpConnection._apiConn = this;
        this.endpoints = {};
    }

    close() {
        this.amqpConnection.close();
    }

    endpoint(address) {
        if (!this.endpoints[address]) {
            let e = new Endpoint(this, address);
            this.endpoints[address] = e;
            let receiver = this.amqpConnection.open_receiver({
                source: address,
                autoaccept: false,
                autosettle: false,
                rcv_settle_mode: 1,
            });
            receiver.__endpoint = e;
            this.receivers.push(receiver);
            return e;
        } else {
            throw new Error(`More than one endpoint on address ${address}`);
        }
    }

    async fetch(address, path, op='GET', body=undefined) {
        this.anonSender.send({
            to       : address,
            reply_to : this.replyTo,
            application_properties : {
                op   : op,
                path : path,
            },
            body : body,
        });
    }

    _on_connection_open(event) {}
    _on_receiver_open(event) {}
    _on_sendable(event) {}
    async _on_message(event) {
        if (event.receiver.__endpoint) {
            await event.receiver.__endpoint._dispatch(event);
        }
    }
    _on_accepted(event) {
        console.log("Accepted");
    }
    _on_rejected(event) {
        console.log("Rejected");
    }
    _on_released(event) {}
    _on_modified(event) {}
    _on_settled(event) {
        console.log("Settled");
    }
}

export class Endpoint {
    constructor(connection, address) {
        this.connection = connection;
        this.address    = address;
        this.path_tree  = new Path();
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

        const first = elements.pop();
        if (first == '') {
            // Ignore blank elements
            return this._find_path(tree, elements);
        }

        const child = tree.get_child(first);
        return this._find_path(child, elements);
    }

    async _dispatch(event) {
        const pathtext = event.message.application_properties.path;
        console.log(`Endpoint dispatch for path: ${pathtext}`);
        if (pathtext) {
            const elements = pathtext.split('/');
            const path     = this._find_path(this.path_tree, elements);

            if (path) {
                try {
                    await path.get_node()._dispatch(event);
                    return;
                } catch (err) {
                    console.log(`Exception in endpoint message dispatch: ${err.stack}`);
                }
            }
        }

        event.delivery.reject();
        event.delivery.settled = true;
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
            let element = elements.pop();
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
        this.mutex = undefined;
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
        if (!this.mutex) {
            this.mutex = new Mutex(this);
        }
        return this.mutex;
    }

    async _dispatch(event) {
        const opcode = event.message.application_properties.op.toLowerCase();
        console.log(`Node at path '${this.path}' dispatched with opcode: ${opcode}`);

        if (opcode == 'acquire') {
            await this.mutex._dispatch(event);
        } else {
            for (const handler of this.handlers[opcode]) {
                handler({}, {}); // Use the 'next' argument
            }
        }
        event.delivery.accept();
        event.delivery.settled = true;
    }
}

export class Mutex {
    constructor(path) {
        this.path      = path;
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

    async _dispatch(delivery, message) {
        const ap = message.application_properties;
        const mutexName = ap.name;
        if (!this.instances[mutexName]) {
            this.instances[mutexName] = new MutexInstance();
        }
        await this.instances[mutexName]._dispatch(delivery, message);
    }
}

class MutexInstance {
    constructor() {
        this.queue = []; // {delivery, message}
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

    async _dispatch(delivery, message) {
        const ap = message.application_properties;
        let request = {
            delivery : delivery,
            message  : message,
            timer    : (this.queue.length > 0 && ap.wait_time) ? setTimeout(() => {
                delivery.reject();
                delivery.settle();
                this._remove_head();  // doesn't look right
            }, ap.wait_time) : undefined,
        }
        this.queue.push(request);
        if (this.queue.length == 1) {
            delivery.accept(); // Acquire the lock
        }
    }

    _remove_head() {
        this.queue.pop();
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