"""The page: every element the script fills exists, and every repository link points at a real file."""
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
HTML = (ROOT / "index.html").read_text()
JS = (ROOT / "assets/home.js").read_text()


PAGES = {"index.html": "home.js", "cdc.html": "cdc.js", "orchestrator.html": "orchestrator.js"}


def test_every_id_each_script_fills_exists():
    for page, js in PAGES.items():
        html, code = (ROOT / page).read_text(), (ROOT / "assets" / js).read_text()
        for i in re.findall(r'getElementById\("([\w-]+)"\)', code):
            assert f'id="{i}"' in html, (page, i)


def test_pages_link_to_each_other():
    for page in PAGES:
        html = (ROOT / page).read_text()
        assert all(f'href="{other}"' in html for other in PAGES if other != page), page


def test_repository_links_point_at_real_paths():
    for path in re.findall(r"github\.com/rfim/nexus-abound/(?:tree|blob)/main/([\w./-]+)", HTML):
        assert (ROOT / path).exists(), path


def test_local_assets_exist():
    for ref in re.findall(r'(?:src|href)="(assets/[^"]+)"', HTML):
        assert (ROOT / ref).exists(), ref


def test_strip_has_one_pill_per_section():
    strip = HTML.split('id="strip"')[1]
    assert len(re.findall(r'data-go="\d"', HTML)) == strip.count("<section ")


def test_stated_plainly():
    assert "not affiliated with or endorsed by Abound" in HTML
    assert "Delta Lake in production" in HTML
