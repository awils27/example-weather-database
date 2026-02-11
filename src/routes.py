from fastapi import FastAPI

app = FastAPI()


@app.get("/")
def read_root():
    return {"Hello": "World"}


@app.get("/items/{city}")
def read_item(city: str | None = None):
    return {"item_id": city}