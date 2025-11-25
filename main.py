from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from api.coins import router as coins_router
from api.overview import router as overview_router
from api.analyst import router as analyst_router

app = FastAPI(title="Crypto API")

# CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],   
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/")
def home():
    return {"message": "API running"}

app.include_router(coins_router, prefix="/api")
app.include_router(overview_router, prefix="/api")
app.include_router(analyst_router, prefix="/api")
