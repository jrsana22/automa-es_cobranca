import hashlib
import hmac

from fastapi import APIRouter, Form, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates

from app.config import settings

router = APIRouter()
templates = Jinja2Templates(directory="app/templates")

COOKIE_NAME = "ac_session"


def _make_token() -> str:
    return hmac.new(
        settings.SECRET_KEY.encode(),
        b"authenticated",
        hashlib.sha256,
    ).hexdigest()


def is_authenticated(request: Request) -> bool:
    token = request.cookies.get(COOKIE_NAME)
    if not token:
        return False
    expected = _make_token()
    return hmac.compare_digest(token, expected)


def require_auth(request: Request):
    if not is_authenticated(request):
        from fastapi import HTTPException
        raise HTTPException(status_code=302, headers={"Location": f"/login?next={request.url.path}"})


@router.get("/login", response_class=HTMLResponse)
def login_page(request: Request, error: str = "", next: str = "/"):
    return templates.TemplateResponse("login.html", {
        "request": request,
        "error": error,
        "next": next,
    })


@router.post("/login")
def login(request: Request, password: str = Form(...), next: str = Form("/")):
    if hmac.compare_digest(password, settings.ADMIN_PASSWORD):
        response = RedirectResponse(next or "/", status_code=302)
        response.set_cookie(
            COOKIE_NAME,
            _make_token(),
            httponly=True,
            samesite="lax",
            max_age=60 * 60 * 24 * 7,  # 7 dias
        )
        return response
    return RedirectResponse(f"/login?error=1&next={next}", status_code=302)


@router.get("/logout")
def logout():
    response = RedirectResponse("/login", status_code=302)
    response.delete_cookie(COOKIE_NAME)
    return response
