from fastapi import FastAPI
from pydantic import BaseModel, Field


app = FastAPI(title="SureLink API")


class VerifyHomeownerRequest(BaseModel):
    address: str = Field(..., min_length=1, description="Property address to verify")


@app.get("/")
def read_root() -> dict[str, str]:
    return {"status": "ok", "service": "surelink-api"}


@app.post("/verify-homeowner")
def verify_homeowner(payload: VerifyHomeownerRequest) -> dict[str, object]:
    normalized_address = payload.address.strip()
    return {
        "status": "placeholder",
        "address": normalized_address,
        "verified": False,
        "message": "Homeowner verification is not implemented yet.",
    }
