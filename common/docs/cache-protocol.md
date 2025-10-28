# Cache Protocol v1

## Overview
This document describes the binary communication protocol used between **Proxy** and **CacheNode**.  
Protocol version: **v1**

### Header
| Field | Type | Size | Description |
|-------|------|------|-------------|
| protocolVersion | byte | 1 | Current = 1 |
| messageType | byte | 1 | 0=Request, 1=Response |

---

### Request (TYPE_REQUEST)
| Field | Type | Description |
|-------|------|-------------|
| requestId | string | UTF-8 (int length + bytes) |
| timestamp | long | Unix millis |
| command | byte | 0=GET, 1=PUT, 2=DELETE |
| key | string | UTF-8 |
| value | nullable string | optional |

**Example:**

[version=1][type=0][requestId][timestamp][cmd][key][value?]["key"][len=5]["value"]

---

### Response (TYPE_RESPONSE)
| Field | Type | Description |
|-------|------|-------------|
| requestId | string | same as in Request |
| value | nullable string | returned value |
| status | byte | 0=OK, 1=NOT_FOUND, 2=ERROR |
| errorMessage | nullable string | if status=ERROR |

---

## Encoding Rules
- All strings are UTF-8 encoded.
- Each string has a 4-byte length prefix (int).
- Null string → length = -1.
- Max string length = 10,000 bytes (`MAX_STRING_LENGTH` constant).
- `long` = 8 bytes, `byte` = 1 byte.

---

## Notes
- TTL is **not included** in protocol v1. Cache nodes apply their default TTL internally.
- All commands are request/response type (synchronous).
- Protocol supports versioning via the first header byte.