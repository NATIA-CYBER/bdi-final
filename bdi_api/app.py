from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from bdi_api.s8.exercise import router as s8_router
import uvicorn
from dotenv import load_dotenv
import os

# Load environment variables
load_dotenv()

app = FastAPI(
    title="BDI Final Assignment API",
    description="API for aircraft tracking and CO2 emissions calculation",
    version="1.0.0"
)

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # In production, replace with specific origins
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include routers
app.include_router(s8_router, tags=["aircraft"])

# Health check endpoint
@app.get("/health")
async def health_check():
    return {"status": "healthy"}

# Error handling
@app.exception_handler(HTTPException)
async def http_exception_handler(request, exc):
    return {
        "status_code": exc.status_code,
        "detail": exc.detail
    }

if __name__ == "__main__":
    uvicorn.run(
        app,
        host=os.getenv("API_HOST", "0.0.0.0"),
        port=int(os.getenv("API_PORT", "8000")),
        reload=True
    )
