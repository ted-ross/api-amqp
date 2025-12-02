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

import { DEFAULT_TIMEOUT_MSEC, LINK_CLASS_FETCH, LINK_CLASS_MUTEX, STATE_ACCEPTED } from "./constants.js";

export class FetchResult {
    constructor(message) {
        this.message = message;
    }

    status() {
        return this.message.application_properties.status;
    }

    async data() {
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
            [LINK_CLASS_FETCH] : this.sessions[LINK_CLASS_FETCH].open_sender({ target : `${address}/${LINK_CLASS_FETCH}` }),
            [LINK_CLASS_MUTEX] : this.sessions[LINK_CLASS_MUTEX].open_sender({ target : `${address}/${LINK_CLASS_MUTEX}` }),
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
            // Get a unique correlation-id for this request and store the response-handler for this request.
            //
            const cid = this.connection._new_cid(this);
            this.in_flight[cid] = (context) => {
                clearTimeout(timer);
                delete this.in_flight[cid];
                resolve(new FetchResult(context.message));
            };

            //
            // Set up a timer to handle the timeout failure.
            //
            const timer = setTimeout(() => {
                delete this.in_flight[cid];
                this.connection._cancel_cid(cid);
                reject(new Error('Operation timed out without a response from the server'));
            }, config.timeout);

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
    //   path => The API path of the mutex-set
    //   mutex_name => The name of the mutex in the set to be acquired
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
                    //
                    // Acquired the mutex, call the critical section function.
                    //
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
