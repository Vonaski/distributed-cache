import http from 'k6/http';
import { check, sleep } from 'k6';
import { Rate, Trend, Counter } from 'k6/metrics';

// Custom metrics
const errorRate = new Rate('errors');
const cacheLatency = new Trend('cache_latency');
const putOperations = new Counter('put_operations');
const getOperations = new Counter('get_operations');
const deleteOperations = new Counter('delete_operations');

// Test configuration - fast but aggressive load test (5 minutes total)
export const options = {
    stages: [
        // Warm-up phase
        { duration: '20s', target: 100 },   // Quick ramp to 100 VUs

        // Load testing phase
        { duration: '30s', target: 300 },   // Ramp to 300 VUs
        { duration: '1m', target: 300 },    // Stay at 300 VUs

        // Peak load phase
        { duration: '30s', target: 600 },   // Spike to 600 VUs
        { duration: '1m', target: 600 },    // Sustained peak load

        // Extreme spike
        { duration: '20s', target: 1000 },  // Maximum spike - 1000 VUs
        { duration: '40s', target: 1000 },  // Brief peak

        // Cool down phase
        { duration: '30s', target: 0 },     // Quick ramp down
    ],
    thresholds: {
        'http_req_duration': ['p(95)<500', 'p(99)<1000'], // 95% under 500ms, 99% under 1s
        'http_req_failed': ['rate<0.05'],                  // Error rate under 5%
        'errors': ['rate<0.05'],                           // Custom error rate under 5%
        'cache_latency': ['p(95)<300', 'p(99)<800'],       // Cache operation latency
    },
};

const BASE_URL = __ENV.BASE_URL || 'http://45.14.194.8'; // Nginx load balancer

// Generate random key for cache operations
function randomKey() {
    return `key_${Math.floor(Math.random() * 10000)}`;
}

// Generate random value
function randomValue() {
    const length = Math.floor(Math.random() * 100) + 10; // 10-110 chars
    return Array(length).fill(0).map(() =>
        String.fromCharCode(97 + Math.floor(Math.random() * 26))
    ).join('');
}

// Simulate realistic cache usage patterns
export default function() {
    const key = randomKey();
    const value = randomValue();

    // Weighted distribution of operations (read-heavy workload)
    // 70% reads, 25% writes, 5% deletes
    const operation = Math.random();

    if (operation < 0.25) {
        // PUT operation (25%)
        putOperation(key, value);
    } else if (operation < 0.95) {
        // GET operation (70%)
        getOperation(key);
    } else {
        // DELETE operation (5%)
        deleteOperation(key);
    }

    // Random think time between 10ms and 100ms (realistic user behavior)
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

    const success = check(response, {
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

    if (!success) {
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
        responseCallback: http.expectedStatuses(200, 404), // 404 is OK for cache miss
    };

    const response = http.get(`${BASE_URL}/api/cache/${key}`, params);

    const duration = Date.now() - startTime;
    cacheLatency.add(duration);
    getOperations.add(1);

    const success = check(response, {
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

    if (!success) {
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
        responseCallback: http.expectedStatuses(200, 404), // 404 is OK if key doesn't exist
    };

    const response = http.del(`${BASE_URL}/api/cache/${key}`, null, params);

    const duration = Date.now() - startTime;
    cacheLatency.add(duration);
    deleteOperations.add(1);

    const success = check(response, {
        'DELETE: status is 200 or 404': (r) => r.status === 200 || r.status === 404,
        'DELETE: response time < 500ms': () => duration < 500,
    });

    if (!success) {
        errorRate.add(1);
        console.error(`DELETE failed for key ${key}: ${response.status} - ${response.body}`);
    } else {
        errorRate.add(0);
    }
}

// Setup - runs once before test
export function setup() {
    console.log('===== Load Test Setup =====');
    console.log(`Target: ${BASE_URL}`);
    console.log('Pre-populating cache with initial data...');

    // Pre-populate cache with fewer keys for faster setup (reduced from 500 to 50)
    // This is enough to test cache hits without timing out
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

// Teardown - runs once after test
export function teardown(data) {
    console.log('===== Load Test Complete =====');
    console.log('Check metrics for detailed results');
}
