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

import { LINK_CLASS_FETCH, LINK_CLASS_MUTEX } from "./constants.js";

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
                source: `${address}/${LINK_CLASS_FETCH}`,
                autoaccept: false,  // We will explicitly handle delivery disposition
                autosettle: false,  // We will explicitly handle delivery settlement
                rcv_settle_mode: 1, // Don't automatically settle when terminal disposition is set on a delivery
            }),
            [LINK_CLASS_MUTEX] : this.sessions[LINK_CLASS_MUTEX].open_receiver({
                source: `${address}/${LINK_CLASS_MUTEX}`,
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
