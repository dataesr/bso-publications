import os

# Load the application environment
APP_ENV = os.getenv('APP_ENV')

# Export config
config = {
    'APP_ENV': APP_ENV
}
