"""normalizacao.py — conversão robusta de números BR/US + magnitude (K/M/MM/B)."""
from __future__ import annotations
import re

def normalizar_numero(valor, convencao: str = "auto"):
    if valor is None:
        return None
    if isinstance(valor, (int, float)):
        return float(valor)
    s = str(valor).strip()
    if s == "" or s.lower() in ("nan", "none", "-", "—"):
        return None
    s = s.replace("R$", "").replace("r$", "").replace("\xa0", "").replace(" ", "")
    mult = 1.0
    m = re.search(r"(MM|M|K|B)$", s, re.IGNORECASE)
    if m:
        mult = {"K": 1e3, "M": 1e6, "MM": 1e6, "B": 1e9}[m.group(1).upper()]
        s = s[:m.start()]
    neg = s.startswith("-"); s = s.lstrip("+-")
    tem_v, tem_p = "," in s, "." in s
    if convencao == "br" or (convencao == "auto" and tem_v):
        s = s.replace(".", "").replace(",", ".")
    elif convencao == "us":
        s = s.replace(",", "")
    else:
        if tem_p and not tem_v and re.fullmatch(r"\d{1,3}(\.\d{3})+", s):
            s = s.replace(".", "")
    try:
        v = float(s)
    except ValueError:
        return None
    return -(v * mult) if neg else v * mult
