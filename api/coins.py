from fastapi import APIRouter
from api.db_connect import dw

router = APIRouter()

@router.get("/top-coins")
def top(limit: int = 20):
    conn = dw()
    with conn.cursor() as cur:
        cur.execute("""
            SELECT 
                c.CoinName, c.Symbol,
                f.MarketCapRank, f.Price, f.MarketCap,
                f.Volume24h, f.PctChange24h
            FROM fact_crypto_snapshot f
            JOIN dim_coin c ON c.CoinKey = f.CoinKey
            ORDER BY f.MarketCap DESC
            LIMIT %s
        """, (limit,))
        data = cur.fetchall()
    conn.close()
    return {"data": data}
