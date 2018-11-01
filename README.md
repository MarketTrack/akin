# akin
akin provides an efficient text similarity query service with an optional web service and web user interface.

## Configuring the Web Application
By default, the web application starts with its instance path set as `instance/`.
From the instance path, two configuration files are ingested at start-up:
- `service.yml`, containing settings for the web application, and
- `brand_settings.yml`, containing settings for the underlying application.

### Settings of Note
**service.yml**
- `UPLOAD_FOLDER` specifies the path to store uploaded files.

**brand_settings.yml**
- `db_location` specifies the backing SQLite database file name.

## Visual Studio Code
Here is a completely reasonable Flask launch configuration for starting the application with debugging enabled:

```json
{
    "version": "0.2.0",
    "configurations": [
        {
            "name": "Python: Flask",
            "type": "python",
            "request": "launch",
            "module": "flask",
            "cwd": "${workspaceFolder}",
            "env": {
                "FLASK_APP": "akin.webapp",
                "FLASK_ENV": "development"
            },
            "args": [
                "run"
            ],
            "jinja": true
        }
}
```