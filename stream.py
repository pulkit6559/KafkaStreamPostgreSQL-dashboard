import json
import websocket

# Define the WebSocket URL for the Coinbase Pro API
websocket_url = "wss://ws-feed.pro.coinbase.com"

# Define the subscribe message
subscribe_message = {
    "type": "subscribe",
    "product_ids": ["ETH-USD", "ETH-EUR"],
    "channels": [
        "level2",
        "heartbeat",
        {
            "name": "ticker",
            "product_ids": ["ETH-BTC", "ETH-USD"]
        }
    ]
}

# Convert the message to JSON
subscribe_message_json = json.dumps(subscribe_message)

# Define the callback function to handle incoming messages
def on_message(ws, message):
    print("Received message:")
    print(json.loads(message))

# Create a WebSocket connection
ws = websocket.WebSocketApp(websocket_url, on_message=on_message)

# Send the subscribe message when the WebSocket connection is established
def on_open(ws):
    print("WebSocket connection opened")
    ws.send(subscribe_message_json)

ws.on_open = on_open

# Run the WebSocket connection
ws.run_forever()
