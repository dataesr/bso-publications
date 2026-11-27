import shutil
import subprocess

from bso.server.main.logger import get_logger

logger = get_logger(__name__)


def _run(cmd: list, timeout: int = 30):
    res = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
    output = (res.stdout or '').strip() or (res.stderr or '').strip()
    return res.returncode, output, (res.stderr or '').strip()


def check_elasticdump() -> bool:
    """Check at startup that elasticdump is installed and runs with the current node version."""
    try:
        node_version = _run(['node', '--version'])[1] if shutil.which('node') else None
        if shutil.which('elasticdump') is None:
            logger.error(f'ELASTICDUMP CHECK FAILED: elasticdump not found in PATH (node version: {node_version})')
            return False
        returncode, output, stderr = _run(['elasticdump', '--version'])
        if returncode != 0:
            logger.error(f'ELASTICDUMP CHECK FAILED: `elasticdump --version` exited with code {returncode} '
                         f'(node version: {node_version}). Probably an incompatible node version. stderr: {stderr}')
            return False
        logger.debug(f'elasticdump check OK: elasticdump {output} with node {node_version}')
        return True
    except Exception as e:
        logger.error(f'ELASTICDUMP CHECK FAILED: {e}')
        return False
