import asyncio, fastapi, time
import json

app = fastapi.FastAPI()
messages_to_mass_write = ""
#f = open("save_file.txt", "a")

async def write_to_file(key: str, value: str):
    data = {"key": key, "value": value}
    with open("save_file.txt", "a") as f:
        f.write(json.dumps(data) + "\n")
    return "Value stored successfully"

async def write_queue_to_file():
    global messages_to_mass_write
    while True:
        print("Printing messages in queue to file.")
        start_writing = time.time()
        #print(start_writing)
        with open("save_file.txt", "a") as f:
            f.write(messages_to_mass_write)
        messages_to_mass_write = ""
        #print(time.time(), time.time() - start_writing)
        print(f"Time needed to write this message to file was {time.time() - start_writing}")
        await asyncio.sleep(20)

async def putValue(key: str, value: str):
    pair = f"{key}:{value}\n"
    with open("save_file.txt", "a") as f:
        f.write(pair)
    return "Value stored successfully"

async def getValue(key: str):
    latest_value = None
    try:
        with open("save_file.txt", "r") as f:
            for line in f:
                try:
                    data = json.loads(line)
                    if data["key"] == key:
                        latest_value = data["value"]
                except json.JSONDecodeError as e:
                    print("Error decoding JSON:", e)
    except FileNotFoundError as e:
        print("File not found:", e)
        return "File not found"
    
    if latest_value is not None:
        return latest_value
    return "Key not found"

@app.on_event("startup")
async def prepare_file():
    task = asyncio.create_task(write_queue_to_file())

# @app.on_event("shutdown")
# async def close_file():
#     f.close()

@app.get("/")
def banana():
    return "Works"


@app.get("/log/{message}")
async def log_this(message:str):
    print(message)
    start_writing = time.time()
    #print(start_writing)
    await write_to_file(message)
    #print(time.time(), time.time() - start_writing)
    print(f"Time needed to write this message to file was {time.time() - start_writing}")
    return "radi"


@app.get("/mass_log/{message}")
async def mass_message_log(message:str):
    global messages_to_mass_write
    messages_to_mass_write = messages_to_mass_write + "\n" + message
    return "in queue"

