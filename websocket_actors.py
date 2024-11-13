import asyncio
import websockets
import pykka

class WebSocketClientActor(pykka.ThreadingActor):
    def __init__(self, uri, kafka_actor, reconnect_delay=5):
        super().__init__()
        self.uri = uri
        self.kafka_actor = kafka_actor
        self.websocket = None
        self.reconnect_delay = reconnect_delay  # Delay in seconds between reconnect attempts

    def on_start(self):
        """Start the WebSocket connection when the actor starts."""
        print(f"WebSocketClientActor starting...")
        asyncio.run(self.connect())

    async def connect(self):
        """Attempt to connect to the WebSocket server, with retries on failure."""
        while True:
            try:
                print(f"Attempting to connect to {self.uri}...")
                self.websocket = await websockets.connect(self.uri)
                print(f"Connected to WebSocket server at {self.uri}")
                await self.listen()  # Listen for messages once connected
                break  # If connection succeeds, break out of the loop
            except Exception as e:
                print(f"Connection failed: {e}. Retrying in {self.reconnect_delay} seconds...")
                await asyncio.sleep(self.reconnect_delay)  # Retry after a delay

    async def listen(self):
        """Listen for messages from the WebSocket server and handle them."""
        try:
            async for message in self.websocket:
                print(f"Received message: {message}")
                self.kafka_actor.tell({'type': 'send', 'payload': message})
        except websockets.ConnectionClosed as e:
            print(f"Connection closed unexpectedly: {e}")
            await self.reconnect()  # Reconnect if the connection is closed unexpectedly

    async def reconnect(self):
        """Handle reconnection if the WebSocket connection drops."""
        print("Reconnecting...")
        self.websocket = None  # Reset the websocket reference
        await self.connect()  # Attempt to reconnect

    def send_message(self, message):
        """Send a message to the WebSocket server."""
        if self.websocket:
            asyncio.run(self.websocket.send(message))
            print(f"Sent message: {message}")
        else:
            print("WebSocket is not connected. Cannot send message.")

    def on_stop(self):
        """Close the WebSocket connection when the actor is stopped."""
        if self.websocket:
            asyncio.run(self.websocket.close())
            print("WebSocket connection closed.")