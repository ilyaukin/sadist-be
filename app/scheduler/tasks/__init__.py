import importlib
import pkgutil


def load_task_modules():
    """Import all task implementation modules to populate ``Task`` registry."""
    for module in pkgutil.iter_modules(__path__):
        if not module.ispkg:
            importlib.import_module('%s.%s' % (__name__, module.name))
