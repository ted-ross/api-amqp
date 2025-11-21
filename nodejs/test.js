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

import { APIConnection } from "./api-amqp.js";

async function TestServer() {
    const server_connection = new APIConnection();
    const endpoint = server_connection.server_endpoint('/test_endpoint/v1alpha1');
    endpoint.route('/names')
    .get((req, res) => {
        res.status(200).send({item1: 'first', item2: 'second'});
    });
    endpoint.route('/names/sub1/sub2')
    .get((req, res) => {
        res.status(200).send("Sub2");
    });

    var   counter = 0;
    const lock_test_endpoint = server_connection.server_endpoint('/lock_test/v1alpha1');
    lock_test_endpoint.route('/variables/counter')
    .get((req, res) => {
        res.status(200).send(counter);
    })
    .put((req, res) => {
        counter = req.body;
        res.status(200).send(counter);
    });
    let mutex = lock_test_endpoint.route('/locks').mutex();
}

async function TestClient() {
    const client_connection = new APIConnection();
    const endpoint = client_connection.client_endpoint('/test_endpoint/v1alpha1')
    let result = await endpoint.fetch('/names', {timeout: 1000});
    console.log(`Status: ${result.status()}, Body: `, result.obj());

    result = await endpoint.fetch('/names/sub1/sub2');
    console.log(`Status: ${result.status()}, Body: `, result.obj());

    result = await endpoint.fetch('/names/sub1', {timeout: 2000});
    console.log(`Status: ${result.status()}, Body: `, result.obj());

    result = await endpoint.fetch('/names/sub1/sub2', {op: 'PUT', timeout: 2000, body: 'Another Sub2'});
    console.log(`Status: ${result.status()}, Body: `, result.obj());
}

class CountClient {
    constructor(endpoint) {
        this.endpoint = endpoint;
    }

    async increment() {
        let result = await this.endpoint.fetch('/variables/counter');
        let value = result.obj();
        value += 1;
        result = await this.endpoint.fetch('/variables/counter', {op: 'PUT', body: value});
        return value;
    }

    async safe_increment() {
        return await this.endpoint.critical_section(
            '/locks',
            'counter',
            async (acquisition_id) => {
                return await this.increment();
            },
            () => {},
            {
                label      : 'safe_increment',
                timeout    : 10000,
            }
        );
    }
}

async function LockTest() {
    const client_connection = new APIConnection();
    const endpoint = client_connection.client_endpoint('/lock_test/v1alpha1');
    let workers  = [];
    let promises = [];
    const count = 250;
    for (let i = 0; i < count; i++) {
        workers.push(new CountClient(endpoint));
    }

    //
    // Run all of the increment sequences concurrently and gather the promises.
    //
    for (let i = 0; i < count; i++) {
        promises.push(workers[i].safe_increment());
    }

    //
    // Wait for all of the gathered promises to resolve, then check the final total.
    //
    const values = await Promise.all(promises);
    const result = await endpoint.fetch('/variables/counter');
    const final = result.obj();
    console.log(`Final count: ${final} - ${(final != count) ? `FAIL (expected ${count})` : 'PASS'}`);
    console.log(values);
}

await TestServer();
await TestClient();
await LockTest();

setTimeout(() => { process.exit(0)}, 10000);
