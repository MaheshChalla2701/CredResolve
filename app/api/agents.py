"""Agent API endpoints."""
from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.enums import AgentState
from app.persistence.database import get_async_db
from app.persistence.models import Agent
from app.persistence.repositories import AgentRepository

router = APIRouter()


async def get_db():
    async with get_async_db() as session:
        yield session


class AgentCreate(BaseModel):
    campaign_id: str
    name: str = Field(..., min_length=1, max_length=255)


class AgentResponse(BaseModel):
    id: str
    campaign_id: str
    name: str
    state: str
    current_call_id: str | None

    model_config = {"from_attributes": True}


class AgentStateUpdate(BaseModel):
    state: str = Field(..., pattern="^(AVAILABLE|OFFLINE|PAUSED)$")


@router.post("/agents", response_model=AgentResponse, status_code=201)
async def register_agent(
    body: AgentCreate,
    session: AsyncSession = Depends(get_db),
):
    from app.persistence.repositories import CampaignRepository
    camp_repo = CampaignRepository(session)
    campaign = await camp_repo.get(body.campaign_id)
    if campaign is None:
        raise HTTPException(status_code=404, detail="Campaign not found")

    repo = AgentRepository(session)
    agent = Agent(
        campaign_id=body.campaign_id,
        name=body.name,
        state=AgentState.OFFLINE,
    )
    agent = await repo.create(agent)
    return agent


@router.get("/agents/{agent_id}", response_model=AgentResponse)
async def get_agent(
    agent_id: str,
    session: AsyncSession = Depends(get_db),
):
    repo = AgentRepository(session)
    agent = await repo.get(agent_id)
    if agent is None:
        raise HTTPException(status_code=404, detail="Agent not found")
    return agent


@router.post("/agents/{agent_id}/availability")
async def update_agent_availability(
    agent_id: str,
    body: AgentStateUpdate,
    session: AsyncSession = Depends(get_db),
):
    """Change agent availability. Only AVAILABLE/OFFLINE/PAUSED transitions allowed via API."""
    repo = AgentRepository(session)
    agent = await repo.get_for_update(agent_id)
    if agent is None:
        raise HTTPException(status_code=404, detail="Agent not found")

    from app.domain.state_machines import validate_agent_transition
    from app.core.errors import InvalidStateTransitionError
    try:
        validate_agent_transition(agent.state, body.state)
    except InvalidStateTransitionError as exc:
        raise HTTPException(status_code=400, detail=str(exc))

    updated = await repo.transition_state(
        agent_id,
        from_state=agent.state,
        to_state=body.state,
        version=agent.version,
    )
    if not updated:
        raise HTTPException(status_code=409, detail="Agent state changed concurrently")

    return {"agent_id": agent_id, "new_state": body.state}


@router.post("/agents/{agent_id}/heartbeat")
async def agent_heartbeat(
    agent_id: str,
    session: AsyncSession = Depends(get_db),
):
    repo = AgentRepository(session)
    await repo.update_heartbeat(agent_id)
    return {"ok": True}


@router.get("/campaigns/{campaign_id}/agents")
async def list_campaign_agents(
    campaign_id: str,
    session: AsyncSession = Depends(get_db),
):
    repo = AgentRepository(session)
    counts = await repo.count_by_state(campaign_id)
    available = await repo.list_available(campaign_id, limit=200)
    return {
        "campaign_id": campaign_id,
        "state_counts": counts,
        "available_agents": [
            {"id": a.id, "name": a.name, "state": a.state}
            for a in available
        ],
    }
