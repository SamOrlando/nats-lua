## lua-nats Documentation

lua-nats is a Lua client library for the NATS messaging system. It provides functionality for connecting to a NATS server, subscribing to subjects, publishing messages, and handling various NATS protocol operations.

### Installation

To use lua-nats, you need to have Lua installed on your system. You can then install the required dependencies by running the following command:

```
luarocks install lua-nats
```

### Usage

To use lua-nats in your Lua code, you need to require the library:

```lua
local nats = require('lua-nats')
```

#### Creating a Client

To create a new NATS client instance, you have two options:

1. Using `nats.client` function:

```lua
local client = nats.client(opts, host, port, timeout)
```

Parameters:
- `opts` (table, optional): Options for configuring the client.
- `host` (string): The host of the NATS server.
- `port` (number): The port of the NATS server.
- `timeout` (number, optional): The timeout for network operations.

2. Using `nats.connect` function:

```lua
local client = nats.connect(parameters)
```

Parameters:
- `parameters` (table): Connection parameters for the client.

#### Connecting to the NATS Server

To connect to the NATS server, you can use the `connect` method of the client object:

```lua
client:connect()
```

This establishes a TCP connection with the NATS server and retrieves server information.

#### Subscribing to Subjects

To subscribe to a subject and receive messages, you can use the `subscribe` method of the client object:

```lua
local sid = client:subscribe(subject, callback, queue)
```

Parameters:
- `subject` (string): The subject to subscribe to.
- `callback` (function): A function to handle received messages.
- `queue` (string, optional): The optional queue group name.

The `callback` function is invoked whenever a message is received on the subscribed subject. The function is passed the received message payload, the reply subject (if applicable), the subject the message was received on, the subscription ID, and the headers (if applicable).

#### Publishing Messages

To publish a message to a subject, you can use the `publish` method of the client object:

```lua
client:publish(subject, payload, reply, headers)
```

Parameters:
- `subject` (string): The subject to publish the message to.
- `payload` (string): The payload of the message.
- `reply` (string, optional): The optional reply subject.
- `headers` (table, optional): Additional headers for the message.

#### Request-Reply

To send a request message and receive a reply, you can use the `request` method of the client object:

```lua
local response = client:request(subject, payload, headers)
```

Parameters:
- `subject` (string): The subject to send the request to.
- `payload` (string): The payload of the request.
- `headers` (table, optional): Additional headers for the request.

The `request` method sends a request message and waits for the reply. It returns the response received from the server.

#### Other Methods

The client object provides several other methods for various operations:

- `ping()`: Sends a PING message to the server.
- `pong()`: Sends a PONG message to the server.
- `unsubscribe(sid)`: Unsubscribes from a subscription identified by the subscription ID.
- `wait(maxcount)`:

 Waits for incoming messages from the server.

### Configuration Options

The client object allows you to configure various options using the provided methods. The available configuration options are:

- `verbose(b)`: Enables or disables verbose mode.
- `headers(b)`: Enables or disables header encoding and decoding.
- `pedantic(b)`: Enables or disables pedantic mode.
- `tls_required(b)`: Enables or disables TLS requirement.
- `user(s)`: Sets the username for authentication.
- `pass(s)`: Sets the password for authentication.
- `auth_token(s)`: Sets the authentication token.
- `name(s)`: Sets the client name.
- `sig(s)`: Sets the client signature.
- `jwt(s)`: Sets the JWT authentication token.
- `nkey(s)`: Sets the NKEY authentication token.
- `protocol(s)`: Sets the protocol version.

### Error Handling

If an error occurs during the execution of the client, the `error` method of the client object is called. You can override this method to handle errors according to your needs.

### Server Information

To retrieve the server information, you can use the `server_info` method of the client object. It returns a table containing the server information.

```lua
local info = client:server_info()
```

### Shutting Down

To gracefully shut down the client and close the connection to the server, you can use the `shutdown` method of the client object:

```lua
client:shutdown()
```

This ensures that any pending operations are completed and the connection is closed.

### Example

Here's an example usage of the lua-nats library:

```lua
local nats = require('lua-nats')

local client = nats.connect({
  host = '127.0.0.1',
  port = 4222
})

client:connect()

local sid = client:subscribe('my.subject', function(message, reply, subject, sid)
  print('Received message:', message)
end)

client:publish('my.subject', 'Hello, NATS!')

client:wait()

client:unsubscribe(sid)

client:shutdown()
```

This example establishes a connection to a NATS server, subscribes to a subject, publishes a message, waits for incoming messages, unsubscribes from the subject, and shuts down the client.

### License

lua-nats is released under the MIT License.