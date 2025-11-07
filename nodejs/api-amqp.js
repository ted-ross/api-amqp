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
        rhea.options.enable_sasl_external = true;
        rhea.on('connection_open', this._on_connection_open);
        rhea.on('receiver_open',   this._on_receiver_open);
        rhea.on('sendable',        this._on_sendable);
        rhea.on('message',         this._on_message);
        rhea.on('accepted',        this._on_accepted);
        rhea.on('rejected',        this._on_rejected);
        rhea.on('released',        this._on_released);
        rhea.on('modified',        this._on_modified);
        rhea.on('settled',         this._on_settled);

        this.amqpConnection = rhea.connect({
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
            this.receivers.push(this.amqpConnection.open_receiver({source: address, autoaccept: false, autosettle: false}));
            return e;
        } else {
            throw new Error(`More than one endpoint on address ${address}`);
        }
    }

    _on_connection_open(event) {}
    _on_receiver_open(event) {}
    _on_sendable(event) {}
    _on_message(event) {}
    _on_accepted(event) {}
    _on_rejected(event) {}
    _on_released(event) {}
    _on_modified(event) {}
    _on_settled(event) {}
}

export class Endpoint {
    constructor(connection, address) {
        this.connection = connection;
        this.address    = address;
        this.path_tree  = new Node();
    }

    close() {
        this.connection.close();
    }

    route(path) {
        let p = new Path(this, path);
        const elements = path.split('/');
        this.path_tree.insert(p, elements);
        return p;
    }

    async _dispatch(delivery, message) {}
}

class Node {
    constructor() {
        this.path     = undefined;
        this.children = {};
    }

    insert(path, elements) {
        if (elements.length == 0) {
            // End of the line, insert here
            this.path = path;
        } else {
            let element = elements.pop();
            if (element == '') {
                // Ignore blank elements
                this.insert(path, elements);
            } else {
                if (!this.children[element]) {
                    this.children[element] = new Node();
                }
                this.children[element].insert(path, elements);
            }
        }
    }
}

export class Path {
    constructor(endpoint, path) {
        this.endpoint = endpoint;
        this.path = path;
        this.root_handlers = [];
        this.handlers = {
            get    : [],
            put    : [],
            post   : [],
            delete : [],
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

    mutex(m) {
        if (!this.mutex) {
            this.mutex = m;
        } else {
            throw new Error('more than one Mutex inserted in the same path node');
        }
        return this;
    }

    async _dispatch(delivery, message) {
    }
}

export class Mutex {
    constructor() {
        this.locks = {}; // name => Lock
    }

    queryAll() {
        let list = [];
        for (const key of Object.keys(this.locks)) {
            list.push(key);
        }
        return list;
    }

    query(name) {
        if (this.locks[name]) {
            return this.locks[name].query();
        }
        return undefined;
    }

    _dispatch(delivery, message) {
        const ap = message.application_properties;
        const lockName = ap.name;
        if (!this.locks[lockName]) {
            this.locks[lockName] = new Lock();
        }
        this.locks[lockName]._dispatch(delivery, message);
    }
}

class Lock {
    constructor() {
        this.queue = []; // {delivery, message}
    }

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
                this._remove_head();
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