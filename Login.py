import html
import json
import os
import re
import sys
import urllib.parse
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import requests
from requests import Response, Session


SESSION_DIR = Path("sessions")
USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36"
)

ACCOUNT_DOMAINS = {
    "JV": "farm01.afterbuy.de",
    "XL": "farm04.afterbuy.de",
}

HIDDEN_FORM_RE = re.compile(
    r'<form[^>]+name=["\']hiddenform["\'][^>]*>(?P<body>[\s\S]*?)</form>',
    re.IGNORECASE,
)
HIDDEN_FORM_ACTION_RE = re.compile(
    r'<form[^>]+name=["\']hiddenform["\'][^>]*action=["\'](?P<action>[^"\']+)["\']',
    re.IGNORECASE,
)
HIDDEN_INPUT_RE = re.compile(
    r'<input[^>]+type=["\']hidden["\'][^>]+name=["\'](?P<name>[^"\']+)["\'][^>]+value=["\'](?P<value>[^"\']*)["\']',
    re.IGNORECASE,
)
LOGIN_FORM_RE = re.compile(
    r'<form\b[^>]*class=["\'][^"\']*form-signin[^"\']*["\'][^>]*>',
    re.IGNORECASE,
)
FORM_ACTION_RE = re.compile(
    r'action=["\'](?P<action>[^"\']+)["\']',
    re.IGNORECASE,
)


class AfterbuyLoginError(RuntimeError):
    """Raised when the login sequence cannot be completed."""


class AfterbuyClient:
    def __init__(
        self, username: str, password: str, domain: str, *, timeout: int = 30
    ) -> None:
        self.username = username
        self.password = password
        self.domain = domain
        self.timeout = timeout
        self.session: Session = requests.Session()
        self.session.headers.update({"User-Agent": USER_AGENT})
        self.login_start_url = f"https://{self.domain}/afterbuy/login.aspx"
        self.protected_url = f"https://{self.domain}/afterbuy/administration.aspx"

    def login(self) -> Session:
        """Perform the login handshake and return an authenticated session."""
        login_page = self.session.get(self.login_start_url, timeout=self.timeout)
        login_action = self._extract_login_action(login_page)

        payload = {
            "LoginView": "ABLogin",
            "Username": self.username,
            "Password": self.password,
            "StaySignedIn": "true",
            "B1": "Anmelden",
        }
        credential_response = self.session.post(
            login_action, data=payload, timeout=self.timeout
        )

        final_response = self._follow_hidden_forms(credential_response)
        if final_response.status_code >= 400:
            raise AfterbuyLoginError(
                f"Unexpected status code {final_response.status_code} "
                "after submitting credentials."
            )

        self._ensure_fedauth_cookie()

        if not self._verify_authenticated():
            raise AfterbuyLoginError("Authentication could not be confirmed.")

        return self.session

    def _extract_login_action(self, response: Response) -> str:
        match = LOGIN_FORM_RE.search(response.text)
        if not match:
            raise AfterbuyLoginError("Login form action could not be located.")
        action_match = FORM_ACTION_RE.search(match.group(0))
        if not action_match:
            raise AfterbuyLoginError("Login form action attribute missing.")
        action = html.unescape(action_match.group("action"))
        return urllib.parse.urljoin(response.url, action)

    def _follow_hidden_forms(
        self, response: Response, *, max_steps: int = 5
    ) -> Response:
        current = response
        for _ in range(max_steps):
            next_response = self._submit_hidden_form(current)
            if not next_response:
                break
            current = next_response
        return current

    def _submit_hidden_form(self, response: Response) -> Optional[Response]:
        action, payload = self._extract_hidden_form(response)
        if not action:
            return None
        headers = {"Referer": response.url}
        post_response = self.session.post(
            action, data=payload, headers=headers, timeout=self.timeout
        )
        return post_response

    def _extract_hidden_form(
        self, response: Response
    ) -> Tuple[Optional[str], Optional[List[Tuple[str, str]]]]:
        action_match = HIDDEN_FORM_ACTION_RE.search(response.text)
        if not action_match:
            return None, None
        action = html.unescape(action_match.group("action"))
        if action.lower().startswith("javascript:"):
            return None, None

        body_match = HIDDEN_FORM_RE.search(response.text)
        body = body_match.group("body") if body_match else response.text
        fields = [
            (html.unescape(match.group("name")), html.unescape(match.group("value")))
            for match in HIDDEN_INPUT_RE.finditer(body)
        ]
        return urllib.parse.urljoin(response.url, action), fields

    def _ensure_fedauth_cookie(self) -> None:
        value = None
        for cookie in self.session.cookies:
            if cookie.name == "FedAuth":
                value = cookie.value
                break
        if not value:
            raise AfterbuyLoginError("FedAuth cookie not issued by identity provider.")
        for domain in (self.domain, ".afterbuy.de"):
            self.session.cookies.set(
                "FedAuth", value, domain=domain, path="/", secure=True
            )

    def _verify_authenticated(self) -> bool:
        response = self.session.get(self.protected_url, timeout=self.timeout)
        response = self._follow_hidden_forms(response)
        html_lower = response.text.lower()
        return "form-signin" not in html_lower


def read_env_file(path: Path) -> Dict[str, str]:
    env: Dict[str, str] = {}
    if not path.exists():
        raise FileNotFoundError(f".env file not found at {path}")

    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        env[key.strip()] = value.strip()
    return env


def extract_accounts(env: Dict[str, str]) -> Dict[str, Dict[str, str]]:
    accounts: Dict[str, Dict[str, str]] = {}
    for key, value in env.items():
        if not key.endswith("_LOGIN"):
            continue
        prefix = key[:-6]
        password_key = f"{prefix}_PASSWORD"
        password = env.get(password_key)
        if password:
            accounts[prefix] = {"login": value, "password": password}
    return accounts


def export_cookies(session: Session) -> List[Dict[str, Optional[str]]]:
    cookies: List[Dict[str, Optional[str]]] = []
    for cookie in session.cookies:
        cookies.append(
            {
                "name": cookie.name,
                "value": cookie.value,
                "domain": cookie.domain,
                "path": cookie.path,
                "secure": cookie.secure,
                "expires": cookie.expires,
            }
        )
    return cookies


def save_cookies(account: str, session: Session) -> Path:
    SESSION_DIR.mkdir(exist_ok=True)
    target_path = SESSION_DIR / f"{account.lower()}_cookies.json"
    data = export_cookies(session)
    target_path.write_text(json.dumps(data, indent=2), encoding="utf-8")
    return target_path


def run_login_for_account(account: str, credentials: Dict[str, str]) -> Optional[Path]:
    username = credentials["login"]
    password = credentials["password"]
    domain = credentials["domain"]
    client = AfterbuyClient(username, password, domain)
    try:
        session = client.login()
    except Exception as exc:  # noqa: BLE001 - surface all issues to caller
        print(f"[ERROR] {account}: {exc}", file=sys.stderr)
        return None
    cookies_path = save_cookies(account, session)
    print(f"[OK] {account} -> session saved to {cookies_path}")
    return cookies_path


def main() -> None:
    env_path = Path(".env")
    try:
        env = read_env_file(env_path)
    except FileNotFoundError as exc:
        print(f"[ERROR] {exc}", file=sys.stderr)
        sys.exit(1)

    accounts = extract_accounts(env)
    if not accounts:
        print(
            "[ERROR] No *_LOGIN/*_PASSWORD pairs found in .env file.",
            file=sys.stderr,
        )
        sys.exit(1)

    for account, creds in accounts.items():
        domain = ACCOUNT_DOMAINS.get(account.upper())
        if not domain:
            print(
                f"[ERROR] {account}: domain mapping missing.",
                file=sys.stderr,
            )
            continue
        creds["domain"] = domain
        print(f"Logging in as {account} ({creds['login']})...")
        run_login_for_account(account, creds)


if __name__ == "__main__":
    main()
