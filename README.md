Local Functions

    create_inbox(): Returns a properly formatted inbox subject.

    merge_defaults(parameters): Merges default parameters with the provided parameters, with the provided parameters taking precedence.

    load_methods(proto, commands): Loads methods from a given prototype and commands table to a new client.

    create_client(client_proto, client_socket, commands): Creates a new client based on a prototype, a socket, and a list of commands.

Network Functions

    network.write(client, buffer): Writes a buffer to a client's socket.

    network.read(client, len): Reads data from a client's socket with a given length.

Response Functions

    response.read(client): Reads a response from a client.

Request Functions

    request.raw(client, buffer): Sends a raw request from a client using a given buffer.

Client Prototype Functions

    These functions allow modifications and actions related to a NATS client, such as sending raw commands, setting authentication, enabling trace, setting verbosity, setting pedantic mode, counting subscriptions, getting server info, and shutting down a client.

Connection Functions

    These functions (connect_tcp(socket, parameters), connect_unix(socket, parameters), and create_connection(parameters)) manage the connections, either TCP or Unix, and are responsible for the creation of connections with proper parameters.

Nats Library Functions

    nats.error(message, level): Throws an error with a given message and level.

    nats.connect(...): Connects to a NATS server with given parameters.

    nats.command(cmd, opts): Executes a command with given options.

Core Commands

    nats.command.connect(client) - This function initiates a connection with the server.

    nats.command.ping(client) - This function sends a PING command to the server and returns true if it receives a PONG response.

    nats.command.pong(client) - This function sends a PONG command to the server.

    nats.command.request(client, subject, payload, callback) - This function sends a request to the server with a given subject and payload, and specifies a callback to be executed once a response is received.

    nats.command.subscribe(client, subject, callback, queue) - This function subscribes to a specific subject and queue and assigns a callback to be executed once a message is received.

    nats.command.unsubscribe(client, unique_id) - This function unsubscribes from a subject or queue with the given unique ID.

    nats.command.publish(client, subject, payload, reply) - This function publishes a message with a given payload to a specific subject and optionally includes a reply subject.

    nats.command.wait(client, quantity) - This function waits for a certain quantity of messages to be received before continuing.

Jetstream

    nats.js.create(client, name, subjects) - This function creates a new JetStream stream with the given name and subjects.

    nats.js.delete(client, name) - This function deletes a JetStream stream with the given name.

    nats.js.consumer.create(client, streamName, durableName, config) - This function creates a new JetStream consumer under the specified stream with a given durable name and configuration.

    nats.js.consumer.delete(client, streamName, durableName) - This function deletes a JetStream consumer with the given durable name from the specified stream.

    nats.js.publish(client, streamName, payload) - This function publishes a message to a specific JetStream stream.

    nats.js.info(client, streamName) - This function fetches and returns information about a specific JetStream stream.

    nats.js.consumer.info(client, streamName, consumerName) - This function fetches and returns information about a specific JetStream consumer from a given stream.

    nats.js.nextmsg(client, streamName, consumerName) - This function fetches the next message from a specific JetStream consumer from a given stream.

    nats.js.ackmsg(client, subject) - This function acknowledges receipt of a message with the given subject.

    nats.js.streams(client) - This function fetches and returns a list of all JetStream streams.

    nats.js.consumers(client, streamName) - This function fetches and returns a list of all JetStream consumers in a given stream.

    nats.js.update(client, streamName, configuration) - This function updates the configuration of a specific JetStream stream.