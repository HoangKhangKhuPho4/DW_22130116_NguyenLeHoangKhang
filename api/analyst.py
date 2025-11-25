from fastapi import APIRouter
from api.db_connect import mart

router = APIRouter()

@router.get("/analyst")
def analyst(symbol: str = None):
    conn = mart()
    with conn.cursor() as cur:
        if symbol:
            cur.execute("""
                SELECT * FROM analyst_snapshot
                WHERE Symbol=%s
                ORDER BY DateKey DESC LIMIT 60
            """, (symbol,))
        else:
            cur.execute("""
                SELECT * FROM analyst_snapshot
                ORDER BY DateKey DESC LIMIT 200
            """)
        data = cur.fetchall()

    conn.close()
    return {"data": data}
