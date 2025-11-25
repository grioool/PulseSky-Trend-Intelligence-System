import asyncio
import websockets

TARGET = "wss://jetstream2.us-east.bsky.network/subscribe?wantedCollections=app.bsky.feed.post"
LOCAL_HOST = "0.0.0.0"
LOCAL_PORT = 8765

async def handler(nifi_ws):
    print("NiFi connected → proxy → Jetstream")

    async with websockets.connect(TARGET) as target_ws:

        async def nifi_to_target():
            try:
                async for message in nifi_ws:
                    await target_ws.send(message)
            except websockets.ConnectionClosed:
                pass

        async def target_to_nifi():
            try:
                async for message in target_ws:
                    await nifi_ws.send(message)
            except websockets.ConnectionClosed:
                pass

        await asyncio.gather(nifi_to_target(), target_to_nifi())


async def main():
    print(f"Starting local WS proxy on ws://{LOCAL_HOST}:{LOCAL_PORT}")
    async with websockets.serve(handler, LOCAL_HOST, LOCAL_PORT):
        await asyncio.Future()   # run forever

asyncio.run(main())
