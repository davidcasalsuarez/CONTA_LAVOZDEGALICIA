import os

def obtener_ruta_onedrive(ruta_relativa):
    """
    Devuelve la ruta completa a un archivo o carpeta en OneDrive.
    
    :param ruta_relativa: Ruta relativa dentro de la carpeta de OneDrive.
    :return: Ruta completa al archivo o carpeta.
    """
    # Obtiene la carpeta principal de OneDrive
    carpeta_onedrive = os.environ.get("OneDrive") or os.environ.get("ONEDRIVE")

    if not carpeta_onedrive:
        raise EnvironmentError("No se encontró la carpeta de OneDrive en este sistema.")

    # Construye la ruta completa
    ruta_completa = os.path.join(carpeta_onedrive, ruta_relativa)
    return ruta_completa
