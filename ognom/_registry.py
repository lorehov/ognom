import inspect

documents_registry = {}


class NoDocumentClassError(Exception):
    pass


def register_doc_class(doc_cls):
    global documents_registry
    module_fqn = inspect.getmodule(doc_cls).__name__
    documents_registry['{}.{}'.format(module_fqn, doc_cls.__name__)] = doc_cls


def get_doc_class(doc_name):
    if doc_name not in documents_registry:
        try:
            full_path = doc_name.split('.')
            module_path = '.'.join(full_path[:-1])
            cls_name = full_path[-1]
            module = __import__(module_path, {}, {}, [cls_name])
            documents_registry[doc_name] = getattr(module, cls_name)
        except ImportError:
            raise NoDocumentClassError(doc_name)
    return documents_registry[doc_name]
