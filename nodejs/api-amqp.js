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
import { ClientEndpoint } from "./src/client.js";
import { ServerEndpoint } from "./src/server.js";
import { STATE_ACCEPTED, STATE_MODIFIED, STATE_REJECTED, STATE_RELEASED } from "./src/constants.js";

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
