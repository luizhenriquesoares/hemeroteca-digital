"""Rotas locais do laboratório de investigação."""
from __future__ import annotations

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field
from starlette.concurrency import run_in_threadpool
from src.web import research_lab_service as service

router=APIRouter(prefix='/api/lab')

@router.get('/identities')
async def identities(ids:str=Query(...,max_length=80)):
    try:values=list(dict.fromkeys(int(x) for x in ids.split(',')))
    except ValueError:raise HTTPException(422,'IDs inválidos')
    if not 2<=len(values)<=3 or min(values)<1:raise HTTPException(422,'Escolha dois ou três registros')
    return await run_in_threadpool(service.compare_identities,values)

@router.get('/connections')
async def connections(left:int=Query(...,ge=1),right:int=Query(...,ge=1)):
    return await run_in_threadpool(service.connection_paths,left,right)

@router.get('/clues')
async def clues(bib:str=Query(...,max_length=40),page:str=Query(...,max_length=80)):
    return await run_in_threadpool(service.next_clues,bib,page)

class Investigation(BaseModel):
    question:str=Field(min_length=4,max_length=500)
    terms:list[str]=Field(default_factory=list,max_length=3)

@router.post('/investigate')
async def investigate(body:Investigation):
    if any(not t.strip() or len(t)>100 for t in body.terms):raise HTTPException(422,'Termos inválidos')
    return await run_in_threadpool(service.investigate,body.question,body.terms)

