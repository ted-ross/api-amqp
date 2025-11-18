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
    const endpoint = server_connection.endpoint('test_endpoint/v1alpha1');
    endpoint.route('/names')
    .get((req, res) => {
        res.status(200).body({item1: 'first', item2: 'second'});
    });
    endpoint.route('/names/sub1/sub2')
    .get((req, res) => {
        res.status(200).body("Sub2");
    })
}

async function TestClient() {
    const client_connection = new APIConnection();
    let result = await client_connection.fetch('test_endpoint/v1alpha1', '/names');
}

await TestServer();
await TestClient();
