import http from 'k6/http';
import { check, sleep } from 'k6';
import { Rate, Trend, Counter } from 'k6/metrics';

const errorRate = new Rate('errors');
const cacheLatency = new Trend('cache_latency');
const putOperations = new Counter('put_operations');
const getOperations = new Counter('get_operations');
const deleteOperations = new Counter('delete_operations');

export const options = {
    stages: [
        { duration: '20s', target: 100 },
        { duration: '30s', target: 300 },
        { duration: '1m', target: 300 },
        { duration: '30s', target: 600 },
        { duration: '1m', target: 600 },
        { duration: '20s', target: 1000 },
        { duration: '40s', target: 1000 },
        { duration: '30s', target: 0 },
    ],
    thresholds: {
        'http_req_duration': ['p(95)<500', 'p(99)<1000'],
        'http_req_failed': ['rate<0.05'],
        'errors': ['rate<0.05'],
        'cache_latency': ['p(95)<300', 'p(99)<800'],
    },
};

const BASE_URL = __ENV.BASE_URL || 'http://45.14.194.8';

function randomKey() {
    return `key_${Math.floor(Math.random() * 10000)}`;
}

function randomValue() {
    const length = Math.floor(Math.random() * 100) + 10;
    return Array(length).fill(0).map(() =>
        String.fromCharCode(97 + Math.floor(Math.random() * 26))
    ).join('');
}

export default function() {
    const key = randomKey();
    const value = randomValue();

    const operation = Math.random();

    if (operation < 0.25) {
        putOperation(key, value);
    } else if (operation < 0.95) {
        getOperation(key);
    } else {
        deleteOperation(key);
    }

    sleep(Math.random() * 0.09 + 0.01);
}

function putOperation(key, value) {
    const startTime = Date.now();

    const payload = JSON.stringify({ value: value });
    const params = {
        headers: { 'Content-Type': 'application/json' },
        tags: { operation: 'PUT' },
    };

    const response = http.put(`${BASE_URL}/api/cache/${key}`, payload, params);

    const duration = Date.now() - startTime;
    cacheLatency.add(duration);
    putOperations.add(1);

    check(response, {
        'PUT: status is 200': (r) => r.status === 200,
        'PUT: response has key': (r) => {
            try {
                return JSON.parse(r.body).key === key;
            } catch {
                return false;
            }
        },
        'PUT: response time < 1s': () => duration < 1000,
    });


    if (response.status !== 200) {
        errorRate.add(1);
        console.error(`PUT failed for key ${key}: ${response.status} - ${response.body}`);
    } else {
        errorRate.add(0);
    }
}

function getOperation(key) {
    const startTime = Date.now();

    const params = {
        tags: { operation: 'GET' },
        responseCallback: http.expectedStatuses(200, 404),
    };

    const response = http.get(`${BASE_URL}/api/cache/${key}`, params);

    const duration = Date.now() - startTime;
    cacheLatency.add(duration);
    getOperations.add(1);

    check(response, {
        'GET: status is 200 or 404': (r) => r.status === 200 || r.status === 404,
        'GET: has valid JSON response': (r) => {
            try {
                JSON.parse(r.body);
                return true;
            } catch {
                return false;
            }
        },
        'GET: response time < 500ms': () => duration < 500,
    });

    if (response.status !== 200 && response.status !== 404) {
        errorRate.add(1);
        console.error(`GET failed for key ${key}: ${response.status} - ${response.body}`);
    } else {
        errorRate.add(0);
    }
}

function deleteOperation(key) {
    const startTime = Date.now();

    const params = {
        tags: { operation: 'DELETE' },
        responseCallback: http.expectedStatuses(200, 404),
    };

    const response = http.del(`${BASE_URL}/api/cache/${key}`, null, params);

    const duration = Date.now() - startTime;
    cacheLatency.add(duration);
    deleteOperations.add(1);

    check(response, {
        'DELETE: status is 200 or 404': (r) => r.status === 200 || r.status === 404,
        'DELETE: response time < 500ms': () => duration < 500,
    });

    if (response.status !== 200 && response.status !== 404) {
        errorRate.add(1);
        console.error(`DELETE failed for key ${key}: ${response.status} - ${response.body}`);
    } else {
        errorRate.add(0);
    }
}

export function setup() {
    console.log('===== Load Test Setup =====');
    console.log(`Target: ${BASE_URL}`);
    console.log('Pre-populating cache with initial data...');

    for (let i = 0; i < 50; i++) {
        const key = `prepopulated_key_${i}`;
        const value = `prepopulated_value_${i}`;
        http.put(
            `${BASE_URL}/api/cache/${key}`,
            JSON.stringify({ value: value }),
            { headers: { 'Content-Type': 'application/json' } }
        );
    }

    console.log('Setup complete. Starting load test...');
    console.log('===========================');
}

export function teardown(data) {
    console.log('===== Load Test Complete =====');
    console.log('Check metrics for detailed results');
}
