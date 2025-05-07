from fastapi import FastAPI
from bdi_api.s8.exercise import router as s8_router

app = FastAPI(title="BDI Final Assignment API")

app.include_router(s8_router)

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
