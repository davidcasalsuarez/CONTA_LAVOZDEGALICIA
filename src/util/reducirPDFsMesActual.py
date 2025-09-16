# -*- coding: utf-8 -*-
"""
La Voz de Galicia — Extraer ÚLTIMAS páginas de cada factura desde PDFs compuestos
=================================================================================

Qué hace:
- Recorre la carpeta "Facturas PDF completo La Voz".
- Cada PDF ahí dentro contiene varias facturas seguidas.
- Detecta el ID de factura (ej.: D24.14318, D24/14318, D24-14318, etc.).
- Cuando cambia el ID o la página dice "Página/Hoja X de Y" con X==Y, considera
  que esa es la última página de esa factura.
- Exporta SOLO esa última página a "Facturas La Voz de Galicia".

Extra:
- Antes de procesar, limpia (borra) los PDFs ya cortados del destino.
- Tras procesar cada PDF del origen, MUEVE el PDF original a la subcarpeta
  "Procesados" dentro de "Facturas PDF completo La Voz".
- Registra log en <BASE>/Log/la_voz_extract.log (archivo + consola).
- Envía un correo de notificación al finalizar (si mail.envioMail está disponible).

Rutas:
- El script se ejecuta desde cualquier ubicación (por ejemplo, dentro de PROGRAMA/...).
- Sube hasta 6 niveles buscando la carpeta "Facturas PDF completo La Voz".
- Usa esas dos subcarpetas (búsqueda **ignore case**):
    Origen : <BASE>/Facturas PDF completo La Voz
    Destino: <BASE>/Facturas La Voz de Galicia
"""

import os
import re
import sys
import shutil
import logging
import traceback
from datetime import datetime

# ------------------ Dependencias opcionales de correo ------------------
try:
    # Se intentan importar; si no existen, se sigue sin enviar correo
    from mail.envioMail import enviarMailLog, envioMensaje
except Exception:
    enviarMailLog = None
    envioMensaje = None

# ------------------ Dependencia PDF (usa pypdf o PyPDF2) ------------------
try:
    from pypdf import PdfReader, PdfWriter
except Exception:
    try:
        from PyPDF2 import PdfReader, PdfWriter
    except Exception as e:
        raise RuntimeError("Instala 'pypdf' o 'PyPDF2' (ej.: pip install pypdf)") from e


# ------------------ Patrones sencillos y efectivos ------------------
# 1) Formato DISGASA típico: D24.14318 / D24-14318 / D24 14318 / D24/14318
RE_DFORM = re.compile(r"\bD\d{2}[\s./-]?\d{5,6}\b", re.IGNORECASE)

# 2) Cabecera “Nº factura” (Nº, No, N., N.o) → nos asomamos a lo que viene después
RE_NFACT_HEAD = re.compile(r"(?:N[\.\°º]?\s*o?\.?|Nº|No\.?|N\.)\s*(?:de\s*)?factura\b", re.IGNORECASE)

# 3) Paginación que marca fin de factura si X==Y (Página/Hoja X de Y / X/Y / Page X of Y)
RE_PAG_X_DE_Y = re.compile(r"(?:p[aá]gina|hoja|page)\s*(\d{1,3})\s*(?:/|de|of)\s*(\d{1,3})", re.IGNORECASE)

# Palabras que NO son un id de factura (evitar falsos positivos tipo “Vencimientos”)
STOPWORDS = {
    "VENCIMIENTOS", "VENCIMIENTO", "FACTURAS", "FACTURA", "TOTAL", "BASE",
    "CLIENTE", "IMPORTE", "IVA", "ALBARAN", "ALBARÁN", "CODIGO", "CÓDIGO",
    "PAGINA", "HOJA", "PAGE", "DATOS"
}


# ------------------ Logging ------------------
def configurar_logging(base_dir):
    """
    Configura logging a archivo + consola en <base_dir>/Log/la_voz_extract.log
    """
    log_dir = os.path.join(base_dir, "Log")
    os.makedirs(log_dir, exist_ok=True)
    log_path = os.path.join(log_dir, "batchreducirPDFSLaVozMesActual.log")

    # Limpiar handlers anteriores
    for h in logging.root.handlers[:]:
        logging.root.removeHandler(h)

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[
            logging.FileHandler(log_path, mode='a', encoding='utf-8'),
            logging.StreamHandler()
        ]
    )
    logging.info("✅ Logging configurado en: %s", log_path)


# ------------------ Utilidades de ruta ------------------
def _find_base(start):
    """Sube por los padres hasta 6 niveles buscando la carpeta 'Facturas PDF completo La Voz'."""
    cur = os.path.abspath(start)
    objetivo = "facturas pdf completo la voz"
    for _ in range(6):
        try:
            entries = {e.lower(): e for e in os.listdir(cur)}
            if objetivo in entries:
                return cur
        except Exception:
            pass
        parent = os.path.dirname(cur)
        if parent == cur:
            break
        cur = parent
    return os.path.abspath(start)  # fallback: donde está el .py o CWD


def _find_dir_case_insensitive(base, name):
    """Devuelve la ruta de 'name' dentro de 'base' ignorando mayúsculas/minúsculas (no crea)."""
    if not os.path.isdir(base):
        return os.path.join(base, name)
    entries = {e.lower(): e for e in os.listdir(base)}
    real = entries.get(name.lower(), name)
    return os.path.join(base, real)


# ------------------ Utilidades de texto ------------------
def _text(page):
    """Extrae texto de una página, devolviendo siempre string (aunque esté vacío)."""
    try:
        return page.extract_text() or ""
    except Exception:
        return ""


def _limpio(token):
    """Limpia un token: quita espacios, signos sueltos y lo pone en mayúsculas sin espacios."""
    token = (token or "").strip().strip(" :#.-")
    token = re.sub(r"\s+", "", token)
    return token.upper()


def detectar_id_factura(texto):
    """
    Intenta sacar un ID de factura de una página:
    1) Primero formato DISGASA (muy robusto).
    2) Si ve “Nº factura”, mira ~50 chars siguientes buscando DISGASA o un token con dígitos.
    """
    t = " ".join((texto or "").split())

    # 1) D24.14318 / D24/14318 / D24-14318 / D24 14318
    m = RE_DFORM.search(t)
    if m:
        return _limpio(m.group(0))

    # 2) “Nº factura” → mirar un poco después para evitar palabras trampa
    m = RE_NFACT_HEAD.search(t)
    if m:
        ventana = t[m.end(): m.end() + 50]
        # a) intentar DISGASA dentro de la ventanita
        m2 = RE_DFORM.search(ventana)
        if m2:
            return _limpio(m2.group(0))
        # b) o tomar el primer “trozo” con dígitos y que no sea stopword
        trozos = re.split(r"[\s,:;|]+", ventana.strip())
        if trozos:
            token = _limpio(re.sub(r"[^\w/.\-]", "", trozos[0]))
            if token and any(ch.isdigit() for ch in token) and token not in STOPWORDS:
                return token

    # 3) Nada fiable
    return None


def es_ultima_por_marca(texto):
    """Devuelve True si la página dice 'Página/Hoja X de Y' y X==Y."""
    m = RE_PAG_X_DE_Y.search(texto or "")
    if not m:
        return False
    try:
        x, y = int(m.group(1)), int(m.group(2))
        return y > 0 and x == y
    except Exception:
        return False


def nombre_seguro(s, maxlen=120):
    """Limpia un string para usarlo como nombre de archivo."""
    s = re.sub(r"[^A-Za-z0-9\-_\.]+", "_", s).strip("_")
    return (s or "factura")[:maxlen]


# ------------------ Núcleo por archivo ------------------
def procesar_pdf_compuesto(pdf_path, dst_dir):
    """
    Lee un PDF compuesto y exporta la ÚLTIMA página de cada factura detectada.
    Devuelve cuántas páginas se exportaron.
    """
    logging.info("Procesando PDF compuesto: %s", os.path.basename(pdf_path))
    reader = PdfReader(pdf_path)

    # Intento de desencriptado trivial (algunos permiten "")
    try:
        if getattr(reader, "is_encrypted", False):
            try:
                reader.decrypt("")
                logging.info("PDF desencriptado (contraseña vacía): %s", os.path.basename(pdf_path))
            except Exception:
                logging.warning("No se pudo desencriptar (se intentó ''); se continua si es posible.")
    except Exception:
        pass

    n = len(reader.pages)
    if n == 0:
        logging.warning("PDF sin páginas: %s", os.path.basename(pdf_path))
        return 0

    # Seguimiento de cada factura
    primera_pagina = {}          # id_factura -> índice primera página
    ultima_pagina = {}           # id_factura -> índice última (cuando la sepamos)
    orden_aparicion = {}         # id_factura -> orden (1,2,3...) para nombrar archivos
    secuencia = 0                # contador de facturas detectadas en el PDF
    current_id = None            # id actual mientras recorro páginas

    for i in range(n):
        page = reader.pages[i]
        tx = _text(page)

        # Si hay marca "Página X de Y" y ya tenemos current_id, marca esta como última del bloque
        if current_id and es_ultima_por_marca(tx):
            ultima_pagina[current_id] = i

        # ¿En esta página aparece un nuevo id de factura?
        found = detectar_id_factura(tx)

        if found:
            if current_id is None:
                # 1ª factura detectada
                current_id = found
                secuencia += 1
                orden_aparicion[current_id] = secuencia
                primera_pagina[current_id] = i
                logging.debug("Detectada factura %s (inicio pág %d)", current_id, i + 1)
            elif found != current_id:
                # Cambio de factura: si la anterior no tenía "última", usar la página anterior
                prev = current_id
                if prev not in ultima_pagina:
                    ultima_pagina[prev] = max(i - 1, primera_pagina.get(prev, i - 1))
                # Nueva factura
                current_id = found
                if current_id not in orden_aparicion:
                    secuencia += 1
                    orden_aparicion[current_id] = secuencia
                    primera_pagina[current_id] = i
                logging.debug("Cambio a factura %s (inicio pág %d)", current_id, i + 1)
        # Si no hay id: no decidir nada; se cerrará al ver el siguiente id o al terminar el PDF

    # Si el PDF acabó y la última factura no tiene “última”, usar la última página
    if current_id and current_id not in ultima_pagina:
        ultima_pagina[current_id] = n - 1

    # Exportar últimas páginas en el orden en que aparecieron las facturas
    base_name = os.path.splitext(os.path.basename(pdf_path))[0]
    pares = sorted(ultima_pagina.items(), key=lambda kv: orden_aparicion.get(kv[0], 10**9))

    exportadas = 0
    ya_usados = set()

    for fid, idx_last in pares:
        writer = PdfWriter()
        writer.add_page(reader.pages[idx_last])

        orden = orden_aparicion.get(fid, 0)
        fid_safe = nombre_seguro(fid)
        out_name = f"{orden:03d}_{fid_safe}__{base_name}_ULTIMA.pdf"
        out_path = os.path.join(dst_dir, out_name)

        # Evitar sobreescrituras si el id se repite
        k = 1
        while out_path.lower() in ya_usados or os.path.exists(out_path):
            out_name = f"{orden:03d}_{fid_safe}__{base_name}_ULTIMA_{k}.pdf"
            out_path = os.path.join(dst_dir, out_name)
            k += 1

        with open(out_path, "wb") as f:
            writer.write(f)

        ya_usados.add(out_path.lower())
        exportadas += 1
        logging.info("  - Exportada última de %s (pág. %d) -> %s", fid, idx_last + 1, out_name)

    logging.info("Total de últimas páginas exportadas desde %s: %d", os.path.basename(pdf_path), exportadas)
    return exportadas


# ------------------ Limpiezas pedidas ------------------
def limpiar_destino(dst_dir):
    """
    Antes de procesar: borra todos los PDFs ya cortados en el destino.
    (Solo elimina archivos .pdf; no toca subcarpetas ni otros formatos.)
    """
    if not os.path.isdir(dst_dir):
        return 0
    borrados = 0
    for f in os.listdir(dst_dir):
        if f.lower().endswith(".pdf"):
            path = os.path.join(dst_dir, f)
            if os.path.isfile(path):
                try:
                    os.remove(path)
                    borrados += 1
                except Exception as e:
                    logging.warning("No se pudo borrar %s: %s", f, e)
    if borrados:
        logging.info("🧹 Limpieza destino: %d PDF(s) borrados en '%s'", borrados, dst_dir)
    return borrados


def mover_origen(pdf_path, src_dir):
    """
    Después de procesar: mueve el PDF compuesto original a src_dir/Procesados.
    Evita colisiones renombrando con sufijos _1, _2, ...
    """
    try:
        proc_dir = os.path.join(src_dir, "Procesados")
        os.makedirs(proc_dir, exist_ok=True)
        base = os.path.basename(pdf_path)
        dest = os.path.join(proc_dir, base)

        # Si ya existe, crea un nombre único
        if os.path.exists(dest):
            name, ext = os.path.splitext(base)
            k = 1
            while True:
                dest_try = os.path.join(proc_dir, f"{name}_{k}{ext}")
                if not os.path.exists(dest_try):
                    dest = dest_try
                    break
                k += 1

        shutil.move(pdf_path, dest)
        logging.info("📦 Origen movido a Procesados: %s", os.path.basename(dest))
        return True
    except Exception as e:
        logging.warning("No se pudo mover el origen '%s': %s", pdf_path, e)
        return False


def enviar_notificacion(destinatarios, asunto, cuerpo):
    """
    Envía notificación si hay funciones de correo disponibles; si no, solo loguea.
    """
    if envioMensaje is None and enviarMailLog is None:
        logging.info("Correo no disponible (mail.envioMail no importado); se omite envío.")
        return

    for to in destinatarios:
        try:
            if envioMensaje is not None:
                envioMensaje(to, f"{asunto}\n\n{cuerpo}")
            elif enviarMailLog is not None:
                enviarMailLog(to, f"{asunto}\n\n{cuerpo}")
            logging.info("Notificación enviada a: %s", to)
        except Exception as e:
            logging.warning("No se pudo enviar notificación a %s: %s", to, e)


# ------------------ MAIN (todo en el mismo archivo) ------------------
def main():
    """
    - Parte de la carpeta del .py (o CWD).
    - Sube hasta 6 niveles buscando la carpeta "Facturas PDF completo La Voz".
    - Origen:  <BASE>/Facturas PDF completo La Voz
    - Destino: <BASE>/Facturas La Voz de Galicia
    - Limpia destino antes; mueve cada origen a 'Procesados' después de procesarlo.
    - Log a fichero + consola. Notificación al finalizar.
    """
    # 1) Base: desde la carpeta del .py; si falla, desde CWD
    try:
        probable_base = os.path.dirname(os.path.abspath(__file__))
    except Exception:
        probable_base = os.getcwd()

    base = _find_base(probable_base)
    configurar_logging(base)

    logging.info("--------------- INICIO PROCESO LAVOZ ------------------")
    logging.info("Base: %s", base)

    # 2) Origen / destino (case-insensitive). Crea destino si no existe.
    src_dir = _find_dir_case_insensitive(base, "Facturas PDF completo La Voz")
    dst_dir = _find_dir_case_insensitive(base, "Facturas La Voz de Galicia")
    os.makedirs(dst_dir, exist_ok=True)

    logging.info("Origen : %s", src_dir)
    logging.info("Destino: %s", dst_dir)

    if not os.path.isdir(src_dir):
        msg = f"No existe la carpeta de origen: {src_dir}"
        logging.error(msg)
        print("❌", msg)
        return

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
            # 5) Mover el archivo compuesto de ORIGEN a 'Procesados'
            mover_origen(path, src_dir)
        except Exception as e:
            err = f"Error procesando {fname}: {e}"
            logging.error(err)
            logging.error(traceback.format_exc())
            errores.append(err)
            # Si falla, NO movemos el origen (para que puedas revisarlo)

    logging.info("--------------- FIN PROCESO LAVOZ (total páginas: %d; archivos: %d; errores: %d) ------------------",
                 total_paginas, procesados, len(errores))

    print(f"--------------- FIN PROCESO LAVOZ (total: {total_paginas} páginas) ------------------")


if __name__ == "__main__":
    main()
