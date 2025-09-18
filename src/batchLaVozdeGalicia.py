import pandas as pd
import logging
import os
import traceback
from openpyxl import load_workbook
from mail.envioMail import *

class FacturasLaVozMesActual:
    
    def __init__(self, ruta):
        self.ruta = ruta

    # ------------------------ Utilidades ------------------------

    def _to_str(self, x):
        try:
            if pd.isna(x):
                return ""
        except:
            pass
        return "" if x is None else str(x)

    def _norm_float(self, raw):
        """Convierte un valor a float manejando formatos tipo: '1.234,56', '123,45', '123.45', ' 1.234,56 €', '-15,2'"""
        s = self._to_str(raw).strip()
        if s == "":
            return 0.0
        s = s.replace("€", "").replace(" ", "")
        s_norm = s.replace(".", "").replace(",", ".")
        try:
            return float(s_norm)
        except:
            return 0.0

    def _norm(self, raw, forzar_negativo=False):
        """Devuelve string con coma decimal y 2 decimales."""
        val = raw if isinstance(raw, (int, float)) else self._norm_float(raw)
        if forzar_negativo and val > 0:
            val = -val
        return f"{val:.2f}".replace(".", ",")

    # ------------------------ EXTRA ------------------------

    def generarFicheroExtraLaVoz(self):
        try:
            listaFicheroExtraIva = []
            listaFacturasUnicas = {}

            diccionario = self.leer_cuentas()

            archivo = os.path.join(self.ruta, 'Excel Facturas La Voz Mes Actual', 'FacturasLaVozMesActual.xlsx')
            logging.info("batchLaVoz.py-. generarFicheroExtraLaVoz: Leyendo Excel FacturasLaVozMesActual.xlsx")
            print(f"Se procede a leer el Excel de Facturas La Voz de Galicia en: {archivo}")

            pf = pd.read_excel(archivo, dtype=str, engine="openpyxl").fillna("")
            print("Contenido inicial del Excel:")
            print(pf.head())

            numeroFactura = 0
            contador = 0

            logging.info("Recorremos el excel de Facturas La Voz de Galicia")
            print("Recorremos el excel de Facturas La Voz de Galicia")

            proveedor_nombre = "DISTR.GALLEGA DE PUBLIC,S.L."

            for _, facturas in pf.iterrows():
                codigo_cliente = self._to_str(facturas.get('CodigoCliente', ''))
                # normalizar por si viene como '13197.0'
                if codigo_cliente.endswith(".0"):
                    codigo_cliente = codigo_cliente[:-2]

                numFactura = self._to_str(facturas.get('NumFactura', ''))
                fecha_emision = self._to_str(facturas.get('Fecha', '')).replace("-", "/")

                cuenta_contable, empresa_nombre = diccionario.get(codigo_cliente, ("Cuenta no encontrada", ""))

                # Importes
                base4  = self._norm_float(facturas.get('BaseImponible4', ''))
                iva4   = self._norm_float(facturas.get('Iva4', ''))
                base21 = self._norm_float(facturas.get('BaseImponible21', ''))
                iva21  = self._norm_float(facturas.get('Iva21', ''))

                base4_str  = self._norm(base4)
                iva4_str   = self._norm(iva4)
                base21_str = self._norm(base21)
                iva21_str  = self._norm(iva21)

                total_factura_str = self._norm(facturas.get('TotalFactura', ''), forzar_negativo=True)

                if (numFactura == "0" or numFactura != self._to_str(facturas.get('NumFactura', ''))):
                    numFactura = self._to_str(facturas.get('NumFactura', ''))

                if (numFactura == "0"):
                    contador += 1
                else:
                    contador += 2

                    descripcion = f"{numFactura}, {proveedor_nombre}, {empresa_nombre}"

                    # Proveedor con TOTAL negativo
                    listaFicheroExtraIva.append([
                        fecha_emision, "40000615", str(numFactura), "", "0", contador,
                        descripcion, "2", total_factura_str,
                        "", "", "", "", "", "0", "10"
                    ])

                    # Tramo 21%: base a cuenta estación, IVA a 47200021
                    if (base21 > 0) or (iva21 > 0):
                        listaFicheroExtraIva.append([
                            fecha_emision, cuenta_contable, str(numFactura), "", "0", contador,
                            descripcion, "1", base21_str,
                            "", "", "", "", "", "0", "10"
                        ])
                        listaFicheroExtraIva.append([
                            fecha_emision, "47200021", str(numFactura), "", "0", contador,
                            descripcion, "1", iva21_str,
                            "", "", "", "", "", "0", "10"
                        ])

                    # Tramo 4%: base a cuenta estación, IVA a 47200004
                    if (base4 > 0) or (iva4 > 0):
                        listaFicheroExtraIva.append([
                            fecha_emision, cuenta_contable, str(numFactura), "", "0", contador,
                            descripcion, "1", base4_str,
                            "", "", "", "", "", "0", "10"
                        ])
                        listaFicheroExtraIva.append([
                            fecha_emision, "47200004", str(numFactura), "", "0", contador,
                            descripcion, "1", iva4_str,
                            "", "", "", "", "", "0", "10"
                        ])

                listaFacturasUnicas[numFactura] = facturas

            dfExtraIva = pd.DataFrame(listaFicheroExtraIva)
            print("Exportando los datos al EXTRA01.csv")
            logging.info("batchLaVoz.py.- generarFicheroExtraLaVoz: Exportando los datos al EXTRA01.csv")
            dfExtraIva.to_csv(os.path.join(self.ruta, 'Contabilidad Mes Actual', 'EXTRA01.csv'),
                              index=False, header=False, sep=';')

            self.generarFicheroIvaLaVoz(listaFacturasUnicas)

            logging.info("batchLaVoz.py.- generarFicheroExtraLaVoz: Fichero EXTRA IVA La Voz generado!")
            print("Fichero EXTRA IVA LA VOZ generado!")

        except Exception:
            logging.error("batchLaVoz.py.- generarFicheroExtraLaVoz: Se ha producido un error: " + traceback.format_exc())
            enviarMailLog("david.casalsuarez@galuresa.com",
                          "batchLaVoz.py.- generarFicheroExtraLaVoz: Se ha producido un error: " + traceback.format_exc())

    # ------------------------ IVA ------------------------

    def generarFicheroIvaLaVoz(self, listaFacturasUnicas):
        try:
            listaFicheroIva = []
            numFactura = 0
            contador = 0
            logging.info("batchLaVoz.py- generarFicheroIvaLaVoz: Iniciando el método")
            print("Recorremos la lista de facturas unicas (La Voz)")

            proveedor_nombre = "DISTR.GALLEGA DE PUBLIC, S.L."
            proveedor_cif = "B15143688"

            for linea in listaFacturasUnicas.values():
                fecha = self._to_str(linea.get('Fecha', '')).replace("-", "/")

                if (numFactura == "0" or numFactura != self._to_str(linea.get('NumFactura', ''))):
                    numFactura = self._to_str(linea.get('NumFactura', ''))
                    contador += 2

                base4  = self._norm_float(linea.get('BaseImponible4', ''))
                iva4   = self._norm_float(linea.get('Iva4', ''))
                base21 = self._norm_float(linea.get('BaseImponible21', ''))
                iva21  = self._norm_float(linea.get('Iva21', ''))

                # --- Tramo 21% ---
                if (base21 > 0) or (iva21 > 0):
                    base21_str  = self._norm(base21)
                    iva21_str   = self._norm(iva21)
                    total21_str = self._norm(base21 + iva21)

                    # A..L..M..N..O..P..Q..R..S..T..U..V..W..X..Y  (25 columnas)
                    listaFicheroIva.append([
                        "40000615", proveedor_nombre, proveedor_cif,            # A,B,C
                        str(numFactura), base21_str, "", "", -2,               # D,E,F,G,H
                        "47200021", "S", fecha, "",                            # I,J,K,L
                        "21", "0",                                             # M,N  👈 M=21, N=0
                        total21_str, iva21_str, "0", "283",                    # O,P,Q,R  👈 R=283
                        fecha, "0", "1", "0", "", fecha, "0"                   # S,T,U,V,W,X,Y
                    ])

                # --- Tramo 4% ---
                if (base4 > 0) or (iva4 > 0):
                    base4_str  = self._norm(base4)
                    iva4_str   = self._norm(iva4)
                    total4_str = self._norm(base4 + iva4)

                    listaFicheroIva.append([
                        "40000615", proveedor_nombre, proveedor_cif,            # A,B,C
                        str(numFactura), base4_str, "", "", -2,                # D,E,F,G,H
                        "47200004", "S", fecha, "",                             # I,J,K,L
                        "4", "0",                                              # M,N  👈 M=4, N=0
                        total4_str, iva4_str, "0", "204",                      # O,P,Q,R  👈 R=204
                        fecha, "0", "1", "0", "", fecha, "0"                   # S,T,U,V,W,X,Y
                    ])

            print("Exportamos a csv el fichero de IVA0101")
            logging.info("batchLaVoz.py- generarFicheroIvaLaVoz: Exportamos a CSV")
            dfFacturas = pd.DataFrame(listaFicheroIva)
            dfFacturas.to_csv(os.path.join(self.ruta, 'Contabilidad Mes Actual', 'IVA0101.csv'),
                              sep=';', index=False, header=False)
            logging.info("batchLaVoz.py- generarFicheroIvaLaVoz: Fichero IVA La Voz generado!")
            print("Fichero IVA LA VOZ generado!")

        except:
            logging.error("batchLaVoz.py- generarFicheroIvaLaVoz: Se ha producido un error: " + traceback.format_exc())
            enviarMailLog("david.casalsuarez@galuresa.com",
                          "batchLaVoz.py- generarFicheroIvaLaVoz: Se ha producido un error: " + traceback.format_exc())

    # ------------------------ Cuentas ------------------------

    def leer_cuentas(self):
        try:
            base_dir = os.path.dirname(os.path.abspath(__file__))  
            archivo_cuentas = os.path.join(base_dir, 'excel', 'CodigosCuentasEstaciones.xlsx')
            logging.info("batchLaVoz.py.- leer_cuentas: Iniciando el metodo")
            print(f"Leemos el Excel CodigosCuentasEstaciones.xlsx en: {archivo_cuentas}")

            df = pd.read_excel(archivo_cuentas, dtype=str)
            df.columns = df.columns.str.strip()

            diccionario = {}
            for _, fila in df.iterrows():
                codigo  = self._to_str(fila.get("Codigo", "")).strip()
                if codigo.endswith(".0"):  # limpiar posibles floats
                    codigo = codigo[:-2]
                cuenta  = self._to_str(fila.get("Cuenta", "")).strip()
                empresa = self._to_str(fila.get("Empresa", "")).strip()
                if codigo:
                    diccionario[codigo] = (cuenta, empresa)

            print("Diccionario cargado (ejemplo 5 primeros):", list(diccionario.items())[:5])
            return diccionario

        except:
            logging.error("batchLaVoz.py.-leer_cuentas: Se ha producido un error: " + traceback.format_exc())
            enviarMailLog("david.casalsuarez@galuresa.com",
                          "batchLaVoz.py.- leer_cuentas: Se ha producido un error: " + traceback.format_exc())
