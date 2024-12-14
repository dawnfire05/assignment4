from contextlib import asynccontextmanager
from elasticsearch import Elasticsearch
from fastapi import FastAPI
import sqlite3
from datetime import datetime

@asynccontextmanager
async def lifespan(app: FastAPI):
    create_index()
    sync_event_data()
    sync_ticket_data()
    yield

app = FastAPI(lifespan=lifespan)

es = Elasticsearch("http://localhost:9200")

DATABASE_PATH = "event_ticket_management_system.db"

EVENTS_INDEX_NAME = "events"
TICKETS_INDEX_NAME = "tickets"

def create_index():
    # 이벤트 인덱스 생성
    if not es.indices.exists(index=EVENTS_INDEX_NAME):
        event_mapping = {
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
        es.indices.create(index=EVENTS_INDEX_NAME, body=event_mapping)

    # 티켓 인덱스 생성
    if not es.indices.exists(index=TICKETS_INDEX_NAME):
        ticket_mapping = {
            "mappings": {
                "properties": {
                    "ticket_id": {"type": "integer"},
                    "book_date": {"type": "date"},
                    "user": {
                        "properties": {
                            "user_id": {"type": "integer"},
                            "name": {"type": "text"},
                            "email": {"type": "text"}
                        }
                    },
                    "event_schedule": {
                        "properties": {
                            "schedule_id": {"type": "integer"},
                            "event_name": {"type": "text"},
                            "event_category": {"type": "keyword"}
                        }
                    },
                    "seat": {
                        "properties": {
                            "class": {"type": "keyword"},
                            "price": {"type": "float"},
                            "row_number": {"type": "keyword"},
                            "column_number": {"type": "integer"}
                        }
                    }
                }
            }
        }
        es.indices.create(index=TICKETS_INDEX_NAME, body=ticket_mapping)

def sync_event_data():
    conn = sqlite3.connect(DATABASE_PATH)
    cursor = conn.cursor()

    # 이벤트 데이터 가져오기
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

        # 스케줄 데이터 가져오기
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

        es.index(index=EVENTS_INDEX_NAME, id=event_id, document=document)

    conn.close()

def sync_ticket_data():
    conn = sqlite3.connect(DATABASE_PATH)
    cursor = conn.cursor()

    # 티켓 데이터 가져오기
    ticket_query = '''
        SELECT
            t.id AS ticket_id,
            t.book_date,
            t.user_id,
            u.name AS user_name,
            u.email AS user_email,
            es.id AS schedule_id,
            e.name AS event_name,
            e.category AS event_category,
            ss.class AS seat_class,
            ss.price AS seat_price,
            ss.row_number,
            ss.column_number
        FROM Ticket t
        LEFT JOIN User u ON t.user_id = u.id
        LEFT JOIN EventSchedule es ON t.event_schedule_id = es.id
        LEFT JOIN Event e ON es.event_id = e.id
        LEFT JOIN SeatSection ss ON t.section_id = ss.id
    '''
    cursor.execute(ticket_query)
    tickets = cursor.fetchall()

    for ticket in tickets:
        ticket_id = ticket[0]
        try:
            book_date = datetime.strptime(ticket[1], "%Y-%m-%d %H:%M:%S").isoformat()
        except ValueError:
            book_date = datetime.strptime(ticket[1], "%Y-%m-%d").isoformat()

        # Elasticsearch 티켓 문서 생성
        ticket_document = {
            "ticket_id": ticket_id,
            "book_date": book_date,
            "user": {
                "user_id": ticket[2],
                "name": ticket[3],
                "email": ticket[4]
            },
            "event_schedule": {
                "schedule_id": ticket[5],
                "event_name": ticket[6],
                "event_category": ticket[7]
            },
            "seat": {
                "class": ticket[8],
                "price": ticket[9],
                "row_number": ticket[10],
                "column_number": ticket[11]
            }
        }

        es.index(index=TICKETS_INDEX_NAME, id=ticket_id, document=ticket_document)

    conn.close()

@app.get("/search/")
async def search_events(query: str, field: str = "name"):
    search_query = {
        "query": {
            "match": {
                field: query
            }
        }
    }
    response = es.search(index=EVENTS_INDEX_NAME, body=search_query)
    return {"results": response["hits"]["hits"]}