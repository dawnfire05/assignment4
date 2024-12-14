from contextlib import asynccontextmanager
from elasticsearch import Elasticsearch
from fastapi import FastAPI
import sqlite3
from datetime import datetime


@asynccontextmanager
async def lifespan(app: FastAPI):
    create_index()
    sync_data()
    yield

app = FastAPI(lifespan=lifespan)

es = Elasticsearch("http://localhost:9200")

DATABASE_PATH = "/Users/dahyeon/develop/event_ticket_management_system/event_ticket_management_system.db"

INDEX_NAME = "events"


def create_index():
    if not es.indices.exists(index=INDEX_NAME):
        mapping = {
            "mappings": {
                "properties": {
                    "event_id": {"type": "integer"},
                    "name": {"type": "text", "fielddata" : True},
                    "description": {"type": "text", "fielddata" : True},
                    "category": {"type": "keyword"},
                    "ticket_open_time": {"type": "date"},
                    "venue": {
                        "properties": {
                            "venue_id": {"type": "integer"},
                            "name": {"type": "text", "fielddata" : True},
                            "location": {"type": "text", "fielddata" : True}
                        }
                    },
                    "sub_venue": {
                        "properties": {
                            "sub_venue_id": {"type": "integer"},
                            "name": {"type": "text", "fielddata" : True},
                            "capacity": {"type": "integer"},
                        }
                    },
                    "schedules": {
                        "type": "nested",
                        "properties": {
                            "schedule_id": {"type": "integer"},
                            "start_time": {"type": "date"},
                            "end_time": {"type": "date"},
                            "artists": {
                                "type": "nested",
                                "properties": {
                                    "artist_id": {"type": "integer"},
                                    "name": {"type": "text", "fielddata" : True},
                                    "company_name": {"type": "text", "fielddata" : True}
                                }
                            }
                        }
                    }
                }
            }
        }
        es.indices.create(index=INDEX_NAME, body=mapping)


def sync_data():
    conn = sqlite3.connect(DATABASE_PATH)
    cursor = conn.cursor()

    # 이벤트와 관련 데이터를 읽기
    event_query = '''
        SELECT
            e.id as event_id,
            e.name as event_name,
            e.description,
            e.category,
            e.ticket_open_time,
            v.id as venue_id,
            v.name as venue_name,
            v.location,
            sv.id as sub_venue_id,
            sv.name as sub_venue_name,
            sv.capacity
        FROM Event e
        LEFT JOIN EventSchedule es ON e.id = es.event_id
        LEFT JOIN SubVenue sv ON es.sub_venue_id = sv.id
        LEFT JOIN Venue v ON sv.venue_id = v.id
    '''
    cursor.execute(event_query)
    events = cursor.fetchall()

    for event in events:
        event_id = event[0]

        ticket_open_time = datetime.strptime(event[4], "%Y-%m-%dT%H:%M:%S.%fZ").isoformat()

        # 스케줄 데이터 읽기
        schedule_query = '''
            SELECT
                es.id as schedule_id,
                es.start_time,
                es.end_time
            FROM EventSchedule es
            WHERE es.event_id = ?
        '''
        cursor.execute(schedule_query, (event_id,))
        schedules = cursor.fetchall()

        # 스케줄별 아티스트 데이터 읽기
        schedules_with_artists = []
        for schedule in schedules:
            schedule_id = schedule[0]

            # ISO 8601 형식으로 변환
            start_time = datetime.strptime(schedule[1], "%Y-%m-%dT%H:%M:%S.%f").isoformat()
            end_time = datetime.strptime(schedule[2], "%Y-%m-%dT%H:%M:%S.%f").isoformat()

            artist_query = '''
                SELECT
                    a.id as artist_id,
                    a.name as artist_name,
                    a.company_name
                FROM Artist a
                WHERE a.event_schedule_id = ?
            '''
            cursor.execute(artist_query, (schedule_id,))
            artists = cursor.fetchall()

            schedules_with_artists.append({
                "schedule_id": schedule_id,
                "start_time": start_time,
                "end_time": end_time,
                "artists": [
                    {
                        "artist_id": artist[0],
                        "name": artist[1],
                        "company_name": artist[2]
                    }
                    for artist in artists
                ]
            })

        # Elasticsearch 문서 생성
        document = {
            "event_id": event[0],
            "name": event[1],
            "description": event[2],
            "category": event[3],
            "ticket_open_time": ticket_open_time,
            "venue": {
                "venue_id": event[5],
                "name": event[6],
                "location": event[7]
            },
            "sub_venue": {
                "sub_venue_id": event[8],
                "name": event[9],
                "capacity": event[10]
            },
            "schedules": schedules_with_artists
        }

        # Elasticsearch에 데이터 삽입
        es.index(index=INDEX_NAME, id=event_id, document=document)

    conn.close()
    print("SQLite 데이터를 Elasticsearch로 동기화 완료.")



@app.get("/search/")
async def search_events(query: str, field: str = "name"):
    search_query = {
        "query": {
            "match": {
                field: query
            }
        }
    }
    response = es.search(index=INDEX_NAME, body=search_query)
    return {"results": response["hits"]["hits"]}