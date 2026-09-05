"""Interactive setup wizard for `instate init`."""

from pathlib import Path

from rich.console import Console
from rich.panel import Panel
from rich.prompt import Prompt
from rich.text import Text

console = Console()

# ── Big banner ──
BANNER = r"""
 ██╗███╗   ██╗███████╗████████╗ █████╗ ████████╗███████╗
 ██║████╗  ██║██╔════╝╚══██╔══╝██╔══██╗╚══██╔══╝██╔════╝
 ██║██╔██╗ ██║███████╗   ██║   ███████║   ██║   █████╗
 ██║██║╚██╗██║╚════██║   ██║   ██╔══██║   ██║   ██╔══╝
 ██║██║ ╚████║███████║   ██║   ██║  ██║   ██║   ███████╗
 ╚═╝╚═╝  ╚═══╝╚══════╝   ╚═╝   ╚═╝  ╚═╝   ╚═╝   ╚══════╝
"""

# White on dark rose for contrast on dark and light terminals.
BANNER_STYLE = "bold white on #8b3a4a"

PROVIDERS = [
    ("OpenRouter", "one key, routes to hundreds of models"),
    ("OpenAI", None),
    ("Anthropic", None),
    ("Google Gemini", None),
    ("Groq", "fast inference on open-weight models"),
    ("xAI", None),
    ("Hugging Face", "open-weight models via Inference Providers"),
    ("Ollama", "free, runs models locally (no key needed)"),
    ("Custom", "any other OpenAI-compatible endpoint you specify"),
]

STEPS = [
    (
        "Connect your language model",
        "Instate uses your chosen model to turn useful parts of your recovery sessions "
        "into searchable memory. You bring the provider and API key.",
    ),
    (
        "Connect Razorpay (test mode)",
        "Your Razorpay test keys let Instate verify webhooks and recover real test-mode payments.",
    ),
    (
        "Choose your memory home",
        "Your ledger lives here. Plain Postgres rows + JSONB — your memory exports with pg_dump.",
    ),
    (
        "Verify & seed",
        "We'll verify the chain, seed a demo history, and run a measured comparison.",
    ),
]


def show_banner(memory_home: Path):
    console.print("[dim]  $ uv tool install \"instate[all]\"[/dim]")
    console.print("[dim]  $ instate init[/dim]")
    console.print()
    console.print(
        Panel(
            Text(BANNER.strip("\n"), justify="center"),
            style=BANNER_STYLE,
            padding=(1, 4),
            expand=False,
            border_style="#b56a7a",
        )
    )
    console.print()
    console.print(f"  [white on #8b3a4a] Memory home: {memory_home} [/white on #8b3a4a]")
    console.print()


def show_step(n: int):
    title, desc = STEPS[n - 1]
    console.print(f"[bold white]Step {n} of {len(STEPS)} — {title}[/bold white]")
    console.print(f"[#e8d5d9]{desc}[/#e8d5d9]")
    console.print()


def pick_provider() -> tuple[int, str, str | None]:
    console.print("[bold white]Choose your LLM provider[/bold white]")
    console.print(
        "[#e8d5d9]Instate uses the same model to process memories and answer `instate ask`.[/ #e8d5d9]"
    )
    console.print()
    for i, (name, note) in enumerate(PROVIDERS, 1):
        suffix = f" [dim]- {note}[/dim]" if note else ""
        console.print(f"  [bold cyan][{i}][/bold cyan] [white]{name}[/white]{suffix}")
    console.print()
    choice = Prompt.ask("Choice", choices=[str(i) for i in range(1, len(PROVIDERS) + 1)], default="4")
    idx = int(choice) - 1
    name, _ = PROVIDERS[idx]
    console.print(f"[green]→ {name}[/green]\n")
    api_key = None
    if name != "Ollama":
        api_key = Prompt.ask(f"Enter your [bold]{name}[/bold] API key", password=True, default="")
    return idx + 1, name, api_key or None


def run_wizard(memory_home: Path | None = None) -> dict:
    memory_home = memory_home or Path.home() / ".instate"
    show_banner(memory_home)

    show_step(1)
    _, provider, api_key = pick_provider()

    show_step(2)
    from rich.prompt import Prompt as P

    razor_key = P.ask("Razorpay Key ID (test mode)", default="rzp_test_xxx")
    razor_secret = P.ask("Razorpay Key Secret", password=True, default="test_secret")
    webhook_secret = P.ask("Webhook secret (X-Razorpay-Signature)", default="whsec_test_secret")

    show_step(3)
    home_str = P.ask("Memory home", default=str(memory_home))
    memory_home = Path(home_str)
    memory_home.mkdir(parents=True, exist_ok=True)
    console.print(f"[green]✓ Memory home ready:[/green] {memory_home}\n")

    show_step(4)
    console.print("[#e8d5d9]Seeding demo history and verifying the chain…[/#e8d5d9]\n")

    return {
        "memory_home": str(memory_home),
        "provider": provider,
        "api_key": api_key,
        "razor_key": razor_key,
        "razor_secret": razor_secret,
        "webhook_secret": webhook_secret,
    }
