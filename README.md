# Lua NATS Client (lua-nats-nx)

Lua NATS Client is a Lua implementation for interacting with [NATS](https://nats.io), a simple, high-performance open-source messaging system for cloud-native applications, IoT messaging, and microservices architectures.

## Features

- **Connection Handling**: Supports connecting to NATS server with configurable options.
- **Messaging**: Publish and subscribe to messages with NATS server.
- **Header and TLS Support**: Handles NATS messaging headers and TLS connections.
- **Customizable**: Offers various customization options for connection and messaging.

## Requirements

- LuaJIT
- LuaSocket for networking
- LuaSec for TLS support
- cjson for JSON encoding/decoding
- uuid for unique identifier generation

## Installation

Include the `nats.lua` file in your Lua project. Ensure that all dependencies (LuaJIT, LuaSocket, LuaSec, cjson, uuid) are installed and accessible in your Lua environment.

## Usage

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

## API Reference

- `nats.client(opts)`: Creates a new NATS client. `opts` can include host, port, TLS settings, etc.
- `client:connect()`: Connects to the NATS server.
- `client:publish(subject, message, reply, header)`: Publishes a message to a subject.
- `client:subscribe(subject, callback, queue)`: Subscribes to a subject.
- `client:unsubscribe(subscriptionId)`: Unsubscribes from a subject.
- `client:shutdown()`: Closes the connection to the NATS server.

## Configuration

The NATS client can be configured with various options such as connection timeouts, TLS settings, verbose logging, etc. Refer to the `defaults` table in the `nats.lua` file for all configurable options.

## Custom Functions

The module includes utility functions for encoding and decoding headers (`encode_header`, `decode_header`), error handling, and generating unique identifiers for inbox subjects.

## License

This project is licensed under the MIT License. See the `LICENSE` file for more details.
