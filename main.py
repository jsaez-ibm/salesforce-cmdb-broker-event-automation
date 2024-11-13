import time
import kafka_actors
import websocket_actors

# Define the WebSocket server URI
WS_URI = "ws://"  # TODO: Update with WebSocket server URI

def main():
    """Start the Kafka client actor"""
    kafka_producer_actor = kafka_actors.KafkaProducerActor.start(kafka_actors.SASL_CONFIG, kafka_actors.KAFKA_TOPIC)
    """Start the WebSocket client actor"""
    ws_client_actor = websocket_actors.WebSocketClientActor.start(WS_URI, kafka_producer_actor)
    # TODO: Get auth token and send message to CMDB

    try:
        while True:
            time.sleep(60)
            # ws_client_actor.tell({"action": "send", "message": "Hello, WS Server!"})
            # ws_client_actor.stop()
    except KeyboardInterrupt:
        print("Client stopping...")
        kafka_producer_actor.stop()
        ws_client_actor.stop()

if __name__ == "__main__":
    main()
