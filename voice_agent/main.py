from fastapi import FastAPI, HTTPException, WebSocket

from .call_manager import CallManager
from .config import get_settings
from .models import CallCreateResponse, CallStatusResponse, OrderModel

settings = get_settings()
call_manager = CallManager(settings)
app = FastAPI(title="Pizza Voice Agent", version="0.1.0")


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
