from contextlib import asynccontextmanager

from fastapi import FastAPI
from sqlmodel import SQLModel, create_engine

engine = create_engine("")


@asynccontextmanager
async def lifespan(app: FastAPI):
    SQLModel.metadata.create_all(engine)
    yield


app = FastAPI(lifespan=lifespan)
