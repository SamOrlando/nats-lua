# Lua NATS Client (lua-nats-nx)

Lua NATS Client is a Lua implementation for interacting with [NATS](https://nats.io), a simple, high-performance open-source messaging system for cloud-native applications, IoT messaging, and microservices architectures. This library includes support for NATS core functionalities as well as the JetStream functionality for stream and consumer management.

## Features

- **Core NATS Features**: Supports core NATS features like publish/subscribe, request/reply, and queue groups.
- **JetStream Support**: Offers an interface to work with NATS JetStream for stream and consumer management.
- **Connection Handling**: Supports connecting to NATS server with configurable options.
- **Messaging with Headers and TLS**: Handles NATS messaging headers and TLS connections.
- **Customizable**: Offers various customization options for connection and messaging.
- **Utilities for Encoding/Decoding**: Built-in functions for encoding and decoding headers.

## Requirements

- LuaJIT
- LuaSocket for networking
- LuaSec for TLS support
- cjson for JSON encoding/decoding
- uuid for unique identifier generation
- nuid for unique ID generation, especially for inbox subject generation

## Installation

Include the `nats.lua` and `nats-jetstream.lua` files in your Lua project. Ensure that all dependencies (LuaJIT, LuaSocket, LuaSec, cjson, uuid, nuid) are installed and accessible in your Lua environment.

## Usage

### Core NATS Client

```lua
local nats = require("nats")

-- Create a new NATS client instance
local client = nats.client({
    host = "127.0.0.1",
    port = 4222
})

-- Connect to the NATS server
client:connect()

-- Publish a message
client:publish("subject", "Hello NATS!")

-- Subscribe to a subject
local subscriptionId = client:subscribe("subject", function(message)
    print("Received message: " .. message.data)
end)

-- Unsubscribe from the subject
client:unsubscribe(subscriptionId)

-- Close the connection
client:shutdown()
```

### JetStream Usage

```lua
local nats = require("nats")
local client = nats.client({/* config */})
client:connect()

-- Create a stream
local stream, creation_msg = client:stream("stream_name", {"subject1", "subject2"})

-- Publish a message to the stream
local publish_success, publish_msg = stream:publish("subject1", "message data")

-- Create a consumer
local consumer, consumer_creation_msg = stream:consumer("consumer_name", { /* consumer config */ })

-- Get next message from the consumer
local message = consumer:nextmsg()
print(message.data)

-- Acknowledge the message
message:ack()

-- Delete the consumer
local delete_success, delete_msg = consumer:delete()

-- Delete the stream
local stream_delete_success, stream_delete_msg = stream:delete()

client:shutdown()
```

## API Reference

Detailed API references are provided for core NATS client functions as well as JetStream related functionalities like stream and consumer management.

- Core NATS Client Functions: `client:connect()`, `client:publish()`, `client:subscribe()`, `client:unsubscribe()`, `client:shutdown()`.
- JetStream Functions: `client:stream()`, `stream:publish()`, `stream:consumer()`, `stream:delete()`, `consumer:nextmsg()`, `consumer:delete()`.

## Configuration

The NATS client and JetStream functionalities can be configured with various options such as connection timeouts, TLS settings, verbose logging, etc. Refer to the `defaults` table in the `nats.lua` and `nats-jetstream.lua` files for all configurable options.

## License

This project is licensed under the MIT License. See the `LICENSE` file for more details.
