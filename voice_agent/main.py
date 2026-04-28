from pathlib import Path

from fastapi import FastAPI, HTTPException, WebSocket
from fastapi.responses import FileResponse, Response
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest

from .call_manager import CallManager
from .config import get_settings
from .models import CallCreateResponse, CallStatusResponse, OrderModel

settings = get_settings()
call_manager = CallManager(settings)
app = FastAPI(title="Pizza Voice Agent", version="0.1.0")

_UI = Path(__file__).resolve().parent.parent / "docs" / "index.html"


@app.get("/")
async def serve_ui() -> FileResponse:
    return FileResponse(_UI)


@app.post("/call", response_model=CallCreateResponse)
async def create_call(order: OrderModel) -> CallCreateResponse:
    call_sid = await call_manager.initiate_call(order)
    return CallCreateResponse(call_sid=call_sid, status="initiated")


@app.get("/call/{call_sid}", response_model=CallStatusResponse)
async def call_status(call_sid: str) -> CallStatusResponse:
    try:
        return await call_manager.get_call_status(call_sid)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@app.websocket("/media-stream")
async def media_stream(websocket: WebSocket) -> None:
    await call_manager.handle_media_stream(websocket)


@app.get("/metrics")
async def metrics() -> Response:
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)
