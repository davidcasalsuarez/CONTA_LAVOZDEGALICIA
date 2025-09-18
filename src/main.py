import configparser
import logging
import traceback
from batchLaVozdeGalicia import *
from mail.envioMail import *
from util.util import obtener_ruta_onedrive
import os


def configurar_logging(ruta_log):
    """
    Configura el sistema de logging para archivo + consola.
    :param ruta_log: Ruta donde se almacenará el archivo de log.
    """
    import os
    import logging

    log_path = os.path.join(ruta_log, "batchFacturasLaVozdeGalicia.log")
    os.makedirs(os.path.dirname(log_path), exist_ok=True)

    # Limpiar handlers anteriores si ya existían
    for handler in logging.root.handlers[:]:
        logging.root.removeHandler(handler)

    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_path, mode='a', encoding='utf-8'),
            logging.StreamHandler()  # Muestra logs también en consola
        ]
    )

    logging.info(f"✅ Logging configurado correctamente en: {log_path}")


def leer_properties():
    """
    Lee el archivo de configuración y configura variables globales.
    """
    try:
        config = configparser.ConfigParser()
        config.read(r'\\Vmapp\c\PROGRAMAS GALURESA\config.conf')

        
        rutaPadreOneDrive = config.get('ONEDRIVE', 'rutapadreonedrive')
        ruta_relativa = "Facturas La Voz de Galicia"
        
        #rutaPadreOneDrive = obtener_ruta_onedrive(ruta_relativa)
        rutaPadreOneDrive = r'C:\Users\david.casal\OneDrive - GASOLINAS, LUBRIFIC. Y REPUESTOS, S.A. (GALURESA) (1)\Facturas La Voz de Galicia'
        ruta_log = os.path.join(rutaPadreOneDrive, "Log")

        configurar_logging(ruta_log)

        logging.info("Archivo de configuración leído correctamente")

        setUserAndPass(config.get('MAIL', 'usuarioEnvio'), config.get('MAIL', 'passEnvio'))

        return rutaPadreOneDrive
    except FileNotFoundError:
        manejar_error("Archivo de configuración no encontrado")
    except KeyError as e:
        manejar_error(f"Error en el archivo de configuración: {e}")
    except Exception as e:
        manejar_error(f"Error inesperado al leer el archivo de configuración: {e}")
    
    except FileNotFoundError:
        manejar_error("Archivo de configuración no encontrado")
    except KeyError as e:
        manejar_error(f"Error en el archivo de configuración: {e}")
    except Exception as e:
        manejar_error(f"Error inesperado al leer el archivo de configuración: {e}")

def manejar_error(mensaje):
    """
    Maneja los errores, los registra y envía correos de notificación.
    :param mensaje: Mensaje de error a registrar y enviar.
    """
    logging.error(mensaje)
    logging.error(traceback.format_exc())
    enviarMailLog("david.casalsuarez@galuresa.com", mensaje)


def main():
    """
    Método principal que coordina la ejecución del proceso.
    """
    try:
        ruta = leer_properties()
        logging.info("--------------- INICIO PROCESO FACTURAS LA VOZ DE GALICIA MES ACTUAL ------------------")
        print("--------------- INICIO PROCESO FACTURAS LA VOZ MES ACTUAL ------------------")
        f_lavoz = FacturasLaVozMesActual(ruta)
        f_lavoz.generarFicheroExtraLaVoz()
        logging.info("--------------- FIN PROCESO FACTURAS LA VOZ MES ACTUAL ------------------")

        #Enviar notificación
        envioMensaje("david.casalsuarez@galuresa.com", "Las facturas han sido procesadas")

    except Exception as e:
        manejar_error(f"main.py-Error inesperado en el proceso principal: {e}")

if __name__ == "__main__":
    main()
