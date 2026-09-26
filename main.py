import os
from pathlib import Path

import uvicorn
from fastapi import FastAPI

from app.api import router
from app.service import Orchestrator
from app.store import Store

DATA_DIR = Path(os.environ.get("ORCHESTRATOR_DATA_DIR", "./data"))

store = Store(DATA_DIR)
orchestrator = Orchestrator(store)

app = FastAPI(title="build-release-orchestrator")
app.state.store = store
app.state.orchestrator = orchestrator
app.include_router(router)


@app.get("/")
def read_root():
    return {"service": "build-release-orchestrator", "status": "ok"}


if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=0)
