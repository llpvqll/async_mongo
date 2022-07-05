import aiohttp
import asyncio
import motor.motor_asyncio
import json
from fastapi import FastAPI
import uvicorn


app = FastAPI()
client = motor.motor_asyncio.AsyncIOMotorClient('mongodb://localhost:27017', serverSelectionTimeoutMS=5000)
db = client.test_database


def to_mongo_format(data):
    inner = data['data']
    return {'data': {
                    'source': inner['source'],
                    'temperature': inner['temperature'],
                    'wind': inner['wind'],
                    'humidity': inner['humidity'],
                    'weather': inner['weather']
                    },
            'last_update': data['last_update']
            }


async def from_source_to_mongo(delay):
    async with aiohttp.ClientSession() as session:
        for _ in range(1, 6):
            await asyncio.sleep(delay)
            async with session.get('http://135.181.197.58:8000/api/v1/weather/source_' + str(_)) as response:
                data = json.loads(await response.text())
                document = to_mongo_format(data)
                await db.source.insert_one(document)


async def get_data_from_mongo():
    cursor = db.source.find({}, {'_id': 0})
    res = []
    for doc in await cursor.to_list(length=100):
        res.append(doc['data'])
    if len(res) == 0:
        await from_source_to_mongo(10)
        res = 'Reload page!'
    return res


def transform_from_mongo_data(data):
    temperature = []
    result = {}
    for index, item in enumerate(data):
        temperature.append(int(item['temperature'][:-2]))
        result['temperature_source_' + str(index+1)] = item['temperature']
    result['max_temperature'] = f'{max(temperature)}°C'
    result['min_temperature'] = f'{min(temperature)}°C'
    result['average_temperature'] = f'{sum(temperature) / len(temperature)}°C'
    return result


@app.get('/weather')
async def send_info_to_weather():
    data = await get_data_from_mongo()
    try:
        return transform_from_mongo_data(data)
    except Exception:
        return data

if __name__ == '__main__':
    uvicorn.run('main:app')
