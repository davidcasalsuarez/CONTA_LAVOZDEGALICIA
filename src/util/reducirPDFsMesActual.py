# -*- coding: utf-8 -*-
"""
La Voz de Galicia — Extraer ÚLTIMAS páginas de cada factura desde PDFs compuestos (sin crear carpetas)
------------------------------------------------------------------------------------------------------
- Origen (debe existir):  <BASE>/Facturas PDF completo La Voz Mes Actual    (fallback: Facturas PDF completo La Voz)
- Destino (debe existir): <BASE>/Facturas La Voz de Galicia Mes Actual      (fallback: Facturas La Voz de Galicia)

Reglas:
- NO CREA carpetas (ni Log, ni Destino, ni Procesados).
- Si falta una carpeta requerida, aborta con mensaje claro.
- Limpia PDFs previos del destino (si existe).
- Exporta la ÚLTIMA página de cada factura detectada.
- Si existe <origen>/Procesados, mueve allí el PDF procesado; si no existe, no mueve y avisa.

Mejoras clave:
- Segmenta facturas por paginado: inicio cuando detecta "Pág/Página/Hoja 1 de N" y fin cuando detecta "Pág/Página/Hoja N de N".
  => Las facturas de 1 hoja (1/1) se exportan SIEMPRE.
- Si falta el ID en texto, crea ID sintético SIN_ID_### (y si hay huecos numéricos, intenta asignarlos por secuencia).
- Validación: entre el número mínimo y máximo detectado, deben existir TODOS. Si faltan, lanza error y lo reporta.
"""

import os
import re
import shutil
import logging
import traceback
from datetime import datetime
from typing import List, Optional

# --------- Email opcional ---------
try:
    from mail.envioMail import enviarMailLog, envioMensaje
except Exception:
    enviarMailLog = None
    envioMensaje = None

# --------- PDF backend ---------
try:
    from pypdf import PdfReader, PdfWriter
except Exception:
    try:
        from PyPDF2 import PdfReader, PdfWriter
    except Exception as e:
        raise RuntimeError("Instala 'pypdf' o 'PyPDF2' (pip install pypdf)") from e


# ====================== Helpers rutas (incluye long path) ======================
def _win_long_path(p: str) -> str:
    """Ruta extendida (\\\\?\\) para evitar MAX_PATH en Windows."""
    p = os.path.abspath(p)
    if os.name != "nt":
        return p
    p = p.replace("/", "\\")
    if p.startswith("\\\\?\\"):
        return p
    if p.startswith("\\\\"):  # UNC
        return "\\\\?\\UNC\\" + p.lstrip("\\")
    return "\\\\?\\" + p

def _normalize_name(s: str) -> str:
    """Normaliza nombres de carpeta: colapsa espacios, strip y casefold."""
    return re.sub(r"\s+", " ", (s or "")).strip().casefold()

def _listdir_safe(dir_path: str) -> List[str]:
    """os.listdir tolerante a long-path en Windows."""
    return os.listdir(_win_long_path(dir_path))

def _find_dir_case_insensitive(base: str, names: List[str]) -> Optional[str]:
    """
    Busca en 'base' el primer nombre de 'names' que exista (case-insensitive, ignora espacios múltiples).
    NO crea nada.
    IMPORTANTE: respeta el ORDEN de 'names' (prioridad).
    """
    if not os.path.isdir(_win_long_path(base)):
        return None

    entries = _listdir_safe(base)
    norm_map = {_normalize_name(e): e for e in entries}

    for n in names:  # respeta prioridad
        key = _normalize_name(n)
        if key in norm_map:
            return os.path.join(base, norm_map[key])
    return None

def _find_base(start: str, src_names: List[str], dst_names: List[str]) -> str:
    """Sube hasta 6 niveles buscando un directorio que contenga alguna carpeta origen o destino."""
    cur = os.path.abspath(start)
    wanted = {_normalize_name(n) for n in (src_names + dst_names)}
    for _ in range(6):
        try:
            entries = _listdir_safe(cur)
            norm_entries = {_normalize_name(e) for e in entries}
            if norm_entries & wanted:
                return cur
        except Exception:
            pass
        parent = os.path.dirname(cur)
        if parent == cur:
            break
        cur = parent
    return os.path.abspath(start)


# ====================== Extracción texto & detección ======================
# ID tipo D26/003153 (tolerante con espacios)
RE_DFORM = re.compile(r"\bD\s*\d{2}\s*[\s./-]?\s*\d{5,6}\b", re.IGNORECASE)

RE_NFACT_HEAD = re.compile(
    r"(?:N[\.\°º]?\s*o?\.?|Nº|No\.?|N\.)\s*(?:de\s*)?factura\b",
    re.IGNORECASE
)

# Paginado tolerante: "Pág. 1/1", "Página 1 de 3", "Hoja 2/2", "Page 1 of 1"...
RE_PAG_X_DE_Y = re.compile(
    r"(?:p[aá]g(?:ina)?\.?|pag(?:ina)?\.?|p[aá]gina|hoja|page)\s*(\d{1,3})\s*(?:/|de|of)\s*(\d{1,3})",
    re.IGNORECASE
)

STOPWORDS = {
    "VENCIMIENTOS", "VENCIMIENTO", "FACTURAS", "FACTURA", "TOTAL", "BASE",
    "CLIENTE", "IMPORTE", "IVA", "ALBARAN", "ALBARÁN", "CODIGO", "CÓDIGO",
    "PAGINA", "PÁGINA", "HOJA", "PAGE", "DATOS"
}

def _text(page) -> str:
    try:
        return page.extract_text() or ""
    except Exception:
        return ""

def _limpio(token: str) -> str:
    token = (token or "").strip().strip(" :#.-")
    token = re.sub(r"\s+", "", token)
    return token.upper()

def detectar_id_factura(texto: str) -> Optional[str]:
    """ID por patrón Dxx/nnnnnn o justo tras 'Nº factura'."""
    t = " ".join((texto or "").split())

    m = RE_DFORM.search(t)
    if m:
        return _limpio(m.group(0))

    m = RE_NFACT_HEAD.search(t)
    if m:
        ventana = t[m.end(): m.end() + 60]
        m2 = RE_DFORM.search(ventana)
        if m2:
            return _limpio(m2.group(0))

        trozos = re.split(r"[\s,:;|]+", ventana.strip())
        if trozos:
            token = _limpio(re.sub(r"[^\w/.\-]", "", trozos[0]))
            if token and any(ch.isdigit() for ch in token) and token not in STOPWORDS:
                return token

    return None

def parse_paginado(texto: str):
    """Devuelve (x, y) si detecta 'Página/Pág./Hoja/Page x de y'."""
    m = RE_PAG_X_DE_Y.search(texto or "")
    if not m:
        return None
    try:
        return int(m.group(1)), int(m.group(2))
    except Exception:
        return None

def id_to_num(fid: str) -> Optional[int]:
    """Convierte 'D26/003153' -> 3153 (usa el último bloque numérico)."""
    if not fid:
        return None
    nums = re.findall(r"\d+", fid)
    if not nums:
        return None
    try:
        return int(nums[-1])
    except Exception:
        return None

def build_fid_from_num(num: int, template_fid: Optional[str]) -> str:
    """
    Construye un ID tipo D26/00NNNNN usando el prefijo de una factura conocida cercana.
    Si no hay template, usa D00/000000 como base.
    """
    prefix = "D00"
    if template_fid:
        m = re.search(r"(?i)\bD\s*\d{2}\b", template_fid)
        if m:
            prefix = _limpio(m.group(0))
    return f"{prefix}/{num:06d}"

def nombre_seguro(s: str, maxlen=120) -> str:
    s = re.sub(r"[^A-Za-z0-9\-_\.]+", "_", s).strip("_")
    return (s or "factura")[:maxlen]


# ====================== Lógica por PDF ======================
def procesar_pdf_compuesto(pdf_path: str, dst_dir: str) -> int:
    """
    Exporta la ÚLTIMA página de cada factura detectada.

    Segmentación robusta:
    - Inicio factura: detecta paginado "1 de N"
    - Fin factura: detecta paginado "N de N"
    - Si una factura es 1/1 => inicio y fin en la misma página => se exporta siempre.
    - El ID se intenta leer dentro del bloque. Si no se puede, SIN_ID_###.
    - Validación de rango: entre min y max numérico detectado, deben estar todas.
      Si faltan, lanza error con el listado.
    """
    logging.info("Procesando PDF compuesto: %s", os.path.basename(pdf_path))

    pdf_path_long = _win_long_path(pdf_path)
    reader = PdfReader(pdf_path_long)

    try:
        if getattr(reader, "is_encrypted", False):
            try:
                reader.decrypt("")
                logging.info("PDF desencriptado (contraseña vacía).")
            except Exception:
                logging.warning("No se pudo desencriptar; se continúa si es posible.")
    except Exception:
        pass

    n = len(reader.pages)
    if n == 0:
        logging.warning("PDF sin páginas: %s", os.path.basename(pdf_path))
        return 0

    # leer textos 1 vez
    textos = []
    paginados = []
    for i in range(n):
        tx = _text(reader.pages[i])
        textos.append(tx)
        paginados.append(parse_paginado(tx))

    # segmentar por paginado
    facturas = []  # dict(seq, start_idx, last_idx, id, num)
    current = None
    seq = 0

    for i in range(n):
        tx = textos[i]
        pag = paginados[i]
        fid = detectar_id_factura(tx)

        if current is not None and (not current.get("id")) and fid:
            current["id"] = fid
            current["num"] = id_to_num(fid)

        es_inicio = bool(pag and pag[0] == 1 and pag[1] is not None)
        es_fin = bool(pag and pag[1] and pag[1] > 0 and pag[0] == pag[1])

        if es_inicio:
            # cerrar anterior si quedó abierta
            if current is not None and current.get("last_idx") is None:
                current["last_idx"] = i - 1 if i > 0 else 0
                facturas.append(current)

            seq += 1
            current = {
                "seq": seq,
                "start_idx": i,
                "last_idx": None,
                "id": fid,
                "num": id_to_num(fid) if fid else None,
            }

        # por si el ID aparece en páginas siguientes del bloque
        if current is not None and current.get("num") is None and fid:
            current["id"] = fid
            current["num"] = id_to_num(fid)

        if current is not None and es_fin:
            current["last_idx"] = i
            facturas.append(current)
            current = None

    # cerrar la última abierta
    if current is not None:
        if current.get("last_idx") is None:
            current["last_idx"] = n - 1
        facturas.append(current)

    if not facturas:
        raise ValueError("No se detectaron bloques de facturas por paginado. Revisa el PDF/regex de paginado.")

    # ---------------- Asignación de números faltantes a SIN_ID (si hay huecos) ----------------
    known_idx = [i for i, it in enumerate(facturas) if it.get("num") is not None]
    if len(known_idx) >= 2:
        for a, b in zip(known_idx, known_idx[1:]):
            na = facturas[a]["num"]
            nb = facturas[b]["num"]
            if na is None or nb is None or nb <= na:
                continue

            between = list(range(a + 1, b))
            if not between:
                continue

            missing_nums = list(range(na + 1, nb))
            unknown_between = [ix for ix in between if facturas[ix].get("num") is None]

            if missing_nums and len(missing_nums) == len(unknown_between):
                template = facturas[a].get("id") or facturas[b].get("id")
                for ix, num in zip(unknown_between, missing_nums):
                    fid_new = build_fid_from_num(num, template)
                    facturas[ix]["id"] = fid_new
                    facturas[ix]["num"] = num

    # ---------------- Validación de rango completo ----------------
    nums_detectados = [it["num"] for it in facturas if it.get("num") is not None]
    if nums_detectados:
        first_num = min(nums_detectados)
        last_num = max(nums_detectados)
        esperado = set(range(first_num, last_num + 1))
        presentes = set(nums_detectados)
        faltan = sorted(esperado - presentes)

        if faltan:
            msg = f"❌ FALTAN FACTURAS EN EL RANGO {first_num}..{last_num}: {faltan}"
            logging.error(msg)
            raise ValueError(msg)

    # ---------------- Exportación ----------------
    base_name = os.path.splitext(os.path.basename(pdf_path))[0]
    usados = set()
    exportadas = 0

    for it in facturas:
        idx_last = it["last_idx"]
        fid = it.get("id") or f"SIN_ID_{it['seq']:03d}"
        fid_safe = nombre_seguro(fid)

        out_name = f"{it['seq']:03d}_{fid_safe}__{base_name}_ULTIMA.pdf"
        out_path = os.path.join(dst_dir, out_name)
        out_path_long = _win_long_path(out_path)

        k = 1
        while out_path_long.lower() in usados or os.path.exists(out_path_long):
            out_name = f"{it['seq']:03d}_{fid_safe}__{base_name}_ULTIMA_{k}.pdf"
            out_path = os.path.join(dst_dir, out_name)
            out_path_long = _win_long_path(out_path)
            k += 1

        writer = PdfWriter()
        writer.add_page(reader.pages[idx_last])

        with open(out_path_long, "wb") as f:
            writer.write(f)

        usados.add(out_path_long.lower())
        exportadas += 1
        logging.info("  - Exportada última de %s (pág. %d) -> %s", fid, idx_last + 1, out_name)

    logging.info("Total exportadas desde %s: %d", os.path.basename(pdf_path), exportadas)
    return exportadas


# ====================== Limpiezas / mover ======================
def limpiar_destino(dst_dir: str) -> int:
    """Borra PDFs del destino antes de procesar (si existe). NO crea nada."""
    if not os.path.isdir(_win_long_path(dst_dir)):
        return 0

    borrados = 0
    for f in _listdir_safe(dst_dir):
        if f.lower().endswith(".pdf"):
            path_long = _win_long_path(os.path.join(dst_dir, f))
            if os.path.isfile(path_long):
                try:
                    os.remove(path_long)
                    borrados += 1
                except Exception as e:
                    logging.warning("No se pudo borrar %s: %s", f, e)

    if borrados:
        logging.info("🧹 Limpieza destino: %d PDF(s) borrados en '%s'", borrados, dst_dir)
    return borrados

def mover_origen(pdf_path: str, src_dir: str) -> bool:
    """
    Mueve el PDF original a <src_dir>/Procesados SOLO si esa carpeta ya existe.
    - Busca 'Procesados' tolerante (mayúsculas/espacios).
    - Usa long path para evitar problemas en rutas largas/UNC.
    """
    try:
        proc_dir = _find_dir_case_insensitive(src_dir, ["Procesados"])
        if not proc_dir or not os.path.isdir(_win_long_path(proc_dir)):
            logging.info("Procesados no existe; no se mueve el origen.")
            return False

        base = os.path.basename(pdf_path)
        src_long = _win_long_path(pdf_path)

        dest = os.path.join(proc_dir, base)
        dest_long = _win_long_path(dest)

        if os.path.exists(dest_long):
            name, ext = os.path.splitext(base)
            k = 1
            while True:
                dest_try = os.path.join(proc_dir, f"{name}_{k}{ext}")
                dest_try_long = _win_long_path(dest_try)
                if not os.path.exists(dest_try_long):
                    dest_long = dest_try_long
                    break
                k += 1

        shutil.move(src_long, dest_long)
        logging.info("📦 Origen movido a Procesados: %s", os.path.basename(dest_long))
        return True

    except Exception as e:
        logging.warning("No se pudo mover el origen '%s': %s", pdf_path, e)
        return False

def enviar_notificacion(destinatarios, asunto, cuerpo):
    if envioMensaje is None and enviarMailLog is None:
        return
    for to in destinatarios:
        try:
            if envioMensaje:
                envioMensaje(to, f"{asunto}\n\n{cuerpo}")
            else:
                enviarMailLog(to, f"{asunto}\n\n{cuerpo}")
        except Exception:
            pass


# ====================== Logging ======================
def configurar_logging(base_dir: str):
    """
    Si existe la carpeta 'Log' dentro de base, escribe fichero allí (no la crea).
    Si no existe, solo consola.
    """
    for h in logging.root.handlers[:]:
        logging.root.removeHandler(h)

    handlers = [logging.StreamHandler()]
    log_dir = _find_dir_case_insensitive(base_dir, ["Log"])
    if log_dir and os.path.isdir(_win_long_path(log_dir)):
        log_path = os.path.join(log_dir, "batchLaVozdeGaliciaMesActual.log")
        try:
            handlers.insert(0, logging.FileHandler(_win_long_path(log_path), mode="a", encoding="utf-8"))
            logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s",
                                handlers=handlers)
            logging.info("✅ Logging fichero: %s", log_path)
            return
        except Exception:
            pass

    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s",
                        handlers=handlers)
    logging.info("ℹ️ Logging solo consola (no existe carpeta 'Log').")


# ====================== MAIN ======================
def main():
    # Mes Actual + fallback (NO se crean si faltan)
    SRC_NAMES = ["Facturas PDF completo La Voz Mes Actual", "Facturas PDF completo La Voz"]
    DST_NAMES = ["Facturas La Voz de Galicia Mes Actual", "Facturas La Voz de Galicia"]

    try:
        probable_base = os.path.dirname(os.path.abspath(__file__))
    except Exception:
        probable_base = os.getcwd()

    base = _find_base(probable_base, SRC_NAMES, DST_NAMES)
    configurar_logging(base)

    logging.info("--------------- INICIO PROCESO LAVOZ ------------------")
    logging.info("Base: %s", base)
    try:
        logging.info("Contenido base: %s", _listdir_safe(base))
    except Exception:
        pass

    src_dir = _find_dir_case_insensitive(base, SRC_NAMES)
    dst_dir = _find_dir_case_insensitive(base, DST_NAMES)

    if not src_dir:
        logging.error("No se encontró carpeta de ORIGEN. Buscadas: %s", SRC_NAMES)
        print("❌ Falta la carpeta de ORIGEN. Crea una de estas EXACTAMENTE:")
        for n in SRC_NAMES:
            print("   -", n)
        return

    if not dst_dir:
        logging.error("No se encontró carpeta de DESTINO. Buscadas: %s", DST_NAMES)
        print("❌ Falta la carpeta de DESTINO. Crea una de estas EXACTAMENTE:")
        for n in DST_NAMES:
            print("   -", n)
        return

    logging.info("Origen : %s", src_dir)
    logging.info("Destino: %s", dst_dir)

    # limpiar destino antes
    limpiar_destino(dst_dir)

    # procesar PDFs del origen
    pdfs = [f for f in _listdir_safe(src_dir) if f.lower().endswith(".pdf")]
    if not pdfs:
        logging.warning("No hay PDFs en la carpeta de origen.")
        print("⚠️ No hay PDFs en la carpeta de origen.")
        return

    total_paginas = 0
    procesados_ok = 0
    errores = []

    for fname in sorted(pdfs):
        path = os.path.join(src_dir, fname)
        try:
            n = procesar_pdf_compuesto(path, dst_dir)
            total_paginas += n
            procesados_ok += 1
            logging.info("✓ %s → últimas páginas exportadas: %d", fname, n)

            # mover a Procesados SOLO si no hubo error
            mov = mover_origen(path, src_dir)
            if not mov:
                logging.info("No se movió a Procesados (no existe o no accesible).")

        except Exception as e:
            err = f"Error procesando {fname}: {e}"
            logging.error(err)
            logging.error(traceback.format_exc())
            errores.append(err)

    logging.info(
        "--------------- FIN PROCESO LAVOZ (total páginas: %d; OK: %d; errores: %d) ------------------",
        total_paginas, procesados_ok, len(errores)
    )

    asunto = f"[La Voz] Proceso finalizado — {datetime.now().strftime('%Y-%m-%d %H:%M')}"
    cuerpo = (
        f"Archivos procesados OK: {procesados_ok}\n"
        f"Últimas páginas exportadas: {total_paginas}\n"
        f"Destino: {dst_dir}\n"
        f"Base: {base}\n"
        f"Errores: {len(errores)}\n"
        + ("\n".join(errores[:20]) if errores else "Sin errores.")
    )
    enviar_notificacion(["david.casalsuarez@galuresa.com"], asunto, cuerpo)

    print(f"--------------- FIN PROCESO LAVOZ (total: {total_paginas} páginas) ------------------")


if __name__ == "__main__":
    main()