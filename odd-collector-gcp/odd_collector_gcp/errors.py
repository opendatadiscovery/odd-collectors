class EmptyFolderError(Exception):
    message = "gcs object is an empty: {}"

    def __init__(self, path):
        super().__init__(self.message.format(path))


class InvalidFileFormatWarning(Exception):
    pass


class AccountIdError(Exception):
    message = (
        "Couldn't take account_id, pass it explicitly through config or check account"
    )

    def __init__(self):
        super().__init__(self.message)


class MappingError(Exception):
    message = "Error during mapping"

    def __init__(self):
        super().__init__(self.message)
