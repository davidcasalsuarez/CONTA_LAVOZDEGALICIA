# -*- coding: utf-8 -*-
"""
La Voz de Galicia — Extraer ÚLTIMAS páginas de cada factura desde PDFs compuestos (sin crear carpetas)
------------------------------------------------------------------------------------------------------
- Origen (debe existir):  <BASE>/Facturas PDF completo La Voz Mes Actual    (fallback: Facturas PDF completo La Voz)
- Destino (debe existir): <BASE>/Facturas La Voz de Galicia Mes Actual      (fallback: Facturas La Voz de Galicia)

Reglas:
- NO CREA carpetas (ni Log, ni Destino, ni Procesados).
- Si falta una carpeta requerida, aborta con mensaje claro y lista de directorios existentes.
- Limpia PDFs previos del destino (si existe).
- Exporta la ÚLTIMA página de cada factura detectada.
- Si existe <origen>/Procesados, mueve allí el PDF procesado; si no existe, no mueve y avisa.
"""

import os
import re
import sys
import shutil
import logging
import traceback
from datetime import datetime
from typing import List, Optional  # ✅ compatibilidad Python < 3.9 / < 3.10

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
    """Ruta extendida (\\?\\) para evitar MAX_PATH en Windows."""
    p = os.path.abspath(p)
    if os.name != "nt":
        return p
    p = p.replace("/", "\\")
    if p.startswith("\\\\?\\"):
        return p
    if p.startswith("\\\\"):         # UNC
        return "\\\\?\\UNC\\" + p.lstrip("\\")
    return "\\\\?\\" + p

def _normalize_name(s: str) -> str:
    """Normaliza nombres de carpeta: colapsa espacios, strip y casefold."""
    return re.sub(r"\s+", " ", (s or "")).strip().casefold()

def _find_dir_case_insensitive(base: str, names: List[str]) -> Optional[str]:
    """
    Busca en 'base' el primer nombre de 'names' que exista (case-insensitive, ignora espacios múltiples).
    NO crea nada. Solo primer nivel; si quieres más tolerancia, baja el depth a 1..3.
    """
    if not os.path.isdir(base):
        return None
    wanted = {_normalize_name(n) for n in names}
    entries = os.listdir(base)
    norm_map = {_normalize_name(e): e for e in entries}
    for w in wanted:
        if w in norm_map:
            return os.path.join(base, norm_map[w])
    return None

def _find_base(start: str, src_names: List[str], dst_names: List[str]) -> str:
    """
    Sube hasta 6 niveles buscando un directorio que contenga alguna de las carpetas de origen o destino.
    NO crea nada.
    """
    cur = os.path.abspath(start)
    wanted = {_normalize_name(n) for n in (src_names + dst_names)}
    for _ in range(6):
        try:
            entries = os.listdir(cur)
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
RE_DFORM = re.compile(r"\bD\d{2}[\s./-]?\d{5,6}\b", re.IGNORECASE)
RE_NFACT_HEAD = re.compile(r"(?:N[\.\°º]?\s*o?\.?|Nº|No\.?|N\.)\s*(?:de\s*)?factura\b", re.IGNORECASE)
RE_PAG_X_DE_Y = re.compile(r"(?:p[aá]gina|hoja|page)\s*(\d{1,3})\s*(?:/|de|of)\s*(\d{1,3})", re.IGNORECASE)
STOPWORDS = {
    "VENCIMIENTOS", "VENCIMIENTO", "FACTURAS", "FACTURA", "TOTAL", "BASE",
    "CLIENTE", "IMPORTE", "IVA", "ALBARAN", "ALBARÁN", "CODIGO", "CÓDIGO",
    "PAGINA", "HOJA", "PAGE", "DATOS"
}

def _text(page):
    try:
        return page.extract_text() or ""
    except Exception:
        return ""

def _limpio(token):
    token = (token or "").strip().strip(" :#.-")
    token = re.sub(r"\s+", "", token)
    return token.upper()

def detectar_id_factura(texto: str) -> Optional[str]:
    """ID por patrón DISGASA o justo tras 'Nº factura'."""
    t = " ".join((texto or "").split())

    m = RE_DFORM.search(t)
    if m:
        return _limpio(m.group(0))

    m = RE_NFACT_HEAD.search(t)
    if m:
        ventana = t[m.end(): m.end() + 50]
        m2 = RE_DFORM.search(ventana)
        if m2:
            return _limpio(m2.group(0))
        trozos = re.split(r"[\s,:;|]+", ventana.strip())
        if trozos:
            token = _limpio(re.sub(r"[^\w/.\-]", "", trozos[0]))
            if token and any(ch.isdigit() for ch in token) and token not in STOPWORDS:
                return token
    return None

def es_ultima_por_marca(texto: str) -> bool:
    m = RE_PAG_X_DE_Y.search(texto or "")
    if not m:
        return False
    try:
        x, y = int(m.group(1)), int(m.group(2))
        return y > 0 and x == y
    except Exception:
        return False

def nombre_seguro(s: str, maxlen=120) -> str:
    s = re.sub(r"[^A-Za-z0-9\-_\.]+", "_", s).strip("_")
    return (s or "factura")[:maxlen]


# ====================== Lógica por PDF ======================
def procesar_pdf_compuesto(pdf_path: str, dst_dir: str) -> int:
    """Exporta la ÚLTIMA página de cada factura detectada. Devuelve cuántas páginas exportó."""
    logging.info("Procesando PDF compuesto: %s", os.path.basename(pdf_path))

    pdf_path_long = _win_long_path(pdf_path)
    dst_dir_long = _win_long_path(dst_dir)

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

    primera_pagina = {}
    ultima_pagina = {}
    orden_aparicion = {}
    secuencia = 0
    current_id = None

    for i in range(n):
        page = reader.pages[i]
        tx = _text(page)

        if current_id and es_ultima_por_marca(tx):
            ultima_pagina[current_id] = i

        found = detectar_id_factura(tx)
        if found:
            if current_id is None:
                current_id = found
                secuencia += 1
                orden_aparicion[current_id] = secuencia
                primera_pagina[current_id] = i
            elif found != current_id:
                prev = current_id
                if prev not in ultima_pagina:
                    ultima_pagina[prev] = max(i - 1, primera_pagina.get(prev, i - 1))
                current_id = found
                if current_id not in orden_aparicion:
                    secuencia += 1
                    orden_aparicion[current_id] = secuencia
                    primera_pagina[current_id] = i

    if current_id and current_id not in ultima_pagina:
        ultima_pagina[current_id] = n - 1

    base_name = os.path.splitext(os.path.basename(pdf_path))[0]
    pares = sorted(ultima_pagina.items(), key=lambda kv: orden_aparicion.get(kv[0], 10**9))

    exportadas = 0
    usados = set()

    for fid, idx_last in pares:
        writer = PdfWriter()
        writer.add_page(reader.pages[idx_last])

        orden = orden_aparicion.get(fid, 0)
        fid_safe = nombre_seguro(fid)
        out_name = f"{orden:03d}_{fid_safe}__{base_name}_ULTIMA.pdf"
        out_path = os.path.join(dst_dir, out_name)
        out_path_long = _win_long_path(out_path)

        k = 1
        while out_path_long.lower() in usados or os.path.exists(out_path_long):
            out_name = f"{orden:03d}_{fid_safe}__{base_name}_ULTIMA_{k}.pdf"
            out_path = os.path.join(dst_dir, out_name)
            out_path_long = _win_long_path(out_path)
            k += 1

        with open(out_path_long, "wb") as f:
            writer.write(f)

        usados.add(out_path_long.lower())
        exportadas += 1
        logging.info("  - Exportada última de %s (pág. %d) -> %s", fid, idx_last + 1, out_name)

    logging.info("Total de últimas páginas exportadas desde %s: %d", os.path.basename(pdf_path), exportadas)
    return exportadas


# ====================== Limpiezas / mover ======================
def limpiar_destino(dst_dir: str) -> int:
    """Borra PDFs del destino antes de procesar (si existe). NO crea nada."""
    if not os.path.isdir(dst_dir):
        return 0
    borrados = 0
    for f in os.listdir(dst_dir):
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
    Si no existe, NO crea nada y lo avisa.
    """
    try:
        proc_dir = os.path.join(src_dir, "Procesados")
        if not os.path.isdir(proc_dir):
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
                dest_try_long = _win_long_path(os.path.join(proc_dir, f"{name}_{k}{ext}"))
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
    # Limpia handlers previos
    for h in logging.root.handlers[:]:
        logging.root.removeHandler(h)

    handlers = [logging.StreamHandler()]
    log_dir = _find_dir_case_insensitive(base_dir, ["Log"])
    if log_dir and os.path.isdir(log_dir):
        log_path = os.path.join(log_dir, "batchLaVozdeGaliciaMesActual.log")
        try:
            handlers.insert(0, logging.FileHandler(log_path, mode="a", encoding="utf-8"))
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
    # Nombres nuevos (Mes Actual) y antiguos (fallback). NO se crean si faltan.
    SRC_NAMES = ["Facturas PDF completo La Voz", "Facturas PDF completo La Voz"]
    DST_NAMES = ["Facturas La Voz de Galicia", "Facturas La Voz de Galicia"]

    # 1) Base: directorio del script o cwd; luego sube hasta encontrar alguna de las carpetas.
    try:
        probable_base = os.path.dirname(os.path.abspath(__file__))
    except Exception:
        probable_base = os.getcwd()

    base = _find_base(probable_base, SRC_NAMES, DST_NAMES)
    configurar_logging(base)

    logging.info("--------------- INICIO PROCESO LAVOZ ------------------")
    logging.info("Base: %s", base)
    try:
        logging.info("Contenido base: %s", os.listdir(base))
    except Exception:
        pass

    # 2) Origen / destino (deben EXISTIR)
    src_dir = _find_dir_case_insensitive(base, SRC_NAMES)
    dst_dir = _find_dir_case_insensitive(base, DST_NAMES)

    if not src_dir:
        logging.error("No se encontró carpeta de ORIGEN. Buscadas (insensible a mayúsculas/espacios): %s", SRC_NAMES)
        print("❌ Falta la carpeta de ORIGEN. Crea una de estas EXACTAMENTE (con o sin mayúsculas):")
        for n in SRC_NAMES:
            print("   -", n)
        return

    if not dst_dir:
        logging.error("No se encontró carpeta de DESTINO. Buscadas (insensible a mayúsculas/espacios): %s", DST_NAMES)
        print("❌ Falta la carpeta de DESTINO. Crea una de estas EXACTAMENTE (con o sin mayúsculas):")
        for n in DST_NAMES:
            print("   -", n)
        return

    logging.info("Origen : %s", src_dir)
    logging.info("Destino: %s", dst_dir)

    # 3) Limpiar destino ANTES de procesar
    limpiar_destino(dst_dir)

    # 4) Procesar todos los PDFs del origen
    pdfs = [f for f in os.listdir(src_dir) if f.lower().endswith(".pdf")]
    if not pdfs:
        logging.warning("No hay PDFs en la carpeta de origen.")
        print("⚠️ No hay PDFs en la carpeta de origen.")
        return

    total_paginas = 0
    procesados = 0
    errores = []

    for fname in sorted(pdfs):
        path = os.path.join(src_dir, fname)
        try:
            n = procesar_pdf_compuesto(path, dst_dir)
            total_paginas += n
            procesados += 1
            logging.info("✓ %s → últimas páginas exportadas: %d", fname, n)
            # 5) Mover a Procesados SOLO si ya existe
            mover_origen(path, src_dir)
        except Exception as e:
            err = f"Error procesando {fname}: {e}"
            logging.error(err)
            logging.error(traceback.format_exc())
            errores.append(err)

    logging.info("--------------- FIN PROCESO LAVOZ (total páginas: %d; archivos: %d; errores: %d) ------------------",
                 total_paginas, procesados, len(errores))

    asunto = f"[La Voz] Proceso finalizado — {datetime.now().strftime('%Y-%m-%d %H:%M')}"
    cuerpo = (
        f"Archivos procesados: {procesados}\n"
        f"Últimas páginas exportadas: {total_paginas}\n"
        f"Destino: {dst_dir}\n"
        f"Base: {base}\n"
        f"Errores: {len(errores)}\n"
        + ("\n".join(errores[:10]) if errores else "Sin errores.")
    )
    enviar_notificacion(["david.casalsuarez@galuresa.com"], asunto, cuerpo)

    print(f"--------------- FIN PROCESO LAVOZ (total: {total_paginas} páginas) ------------------")


if __name__ == "__main__":
    main()
