from fastapi import APIRouter
from api.db_connect import mart

router = APIRouter()

@router.get("/overview")
def overview():
    conn = mart()
    with conn.cursor() as cur:
        cur.execute("""
            SELECT DateKey, TotalCoins, TotalMarketCap,
                   TotalVolume, Top1_Coin, Top1_MarketCap
            FROM overview_daily
            ORDER BY DateKey DESC
            LIMIT 30
        """)
        data = cur.fetchall()
    conn.close()
    return {"data": data}
