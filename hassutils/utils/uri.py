def path_to_uri(path):
    import pathlib
    import urllib.parse
    path = pathlib.Path(path)
    if path.is_absolute():
        return path.as_uri()
    return 'file:' + urllib.parse.quote(path.as_posix(), safe=':/')
