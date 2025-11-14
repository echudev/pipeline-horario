# Cargar la variable de entorno `GOOGLE_APPLICATION_CREDENTIALS` en tu entorno virtual (`venv`) de Python

Esta guía te mostrará cómo cargar la variable de entorno `GOOGLE_APPLICATION_CREDENTIALS` en tu entorno virtual (`venv`) de Python.

Basado en la [Documentación de Google Cloud](https://docs.cloud.google.com/docs/authentication/set-up-adc-local-dev-environment?hl=es)

## Descripción

Para cargar una variable de entorno como `GOOGLE_APPLICATION_CREDENTIALS` en tu entorno virtual (`venv`) de Python, simplemente la estableces en la **terminal activa** *después* de haber activado el `venv`.

Aquí tienes los pasos exactos para los sistemas operativos más comunes:

-----

## 💻 1. Activa tu Entorno Virtual

Primero, asegúrate de que tu `venv` esté activo.

```bash
# Si usas Linux o macOS (Bash/Zsh)
source /ruta/a/tu/venv/bin/activate

# Si usas Windows (Command Prompt)
\ruta\a\tu\venv\Scripts\activate

# Si usas Windows (PowerShell)
\ruta\a\tu\venv\Scripts\Activate.ps1
```

Una vez activo, verás el nombre de tu `venv` entre paréntesis al inicio de tu línea de comandos, por ejemplo: `(mi-venv) $`.

-----

## 🔑 2. Carga la Variable de Entorno

Con el `venv` activo, ejecuta el comando apropiado para tu sistema. Esto hace que la variable esté disponible para todos los procesos que se ejecuten dentro de esa sesión de terminal, incluyendo tu script de Python.

**En Linux o macOS (Bash/Zsh):**

```bash
export GOOGLE_APPLICATION_CREDENTIALS="/ruta/a/tu/archivo/mi-service-account-key.json"
```

**En Windows (Command Prompt):**

```bash
set GOOGLE_APPLICATION_CREDENTIALS="C:\ruta\a\tu\archivo\mi-service-account-key.json"
```

**En Windows (PowerShell):**

```powershell
$env:GOOGLE_APPLICATION_CREDENTIALS="C:\ruta\a\tu\archivo\mi-service-account-key.json"
```

-----

## 🚀 3. Ejecuta el Script

Ahora puedes ejecutar tu script de Python:

```bash
python tu_script.py
```

Tu script usará automáticamente la ruta a la llave JSON para autenticar el cliente de BigQuery:

```python
# Tu script lee la variable automáticamente:
client = bigquery.Client(project=PROJECT_ID)
```

### Importante sobre la Persistencia

Las variables de entorno establecidas con `export` o `set` son **temporales** y solo existen para la sesión de terminal actual. Si cierras la terminal o desactivas el `venv` (con el comando `deactivate`), tendrás que volver a cargar la variable la próxima vez que trabajes.

Si quieres que la variable se cargue automáticamente cada vez que activas el `venv`, puedes añadir la línea `export GOOGLE_APPLICATION_CREDENTIALS="..."` al archivo **`activate`** que se encuentra dentro del directorio `/ruta/a/tu/venv/bin/`
