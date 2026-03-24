from fastapi import FastAPI

app = FastAPI(title="OT PdM Intelligence")


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}
