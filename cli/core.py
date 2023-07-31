from prompt_toolkit.validation import Validator, ValidationError
from prompt_toolkit.document import Document
from typing import Callable

class ChainValidator(Validator):
    """
    Validate input from a a chain of callables.
    """

    def __init__(self, move_cursor_to_end: bool, *funcs: Callable[[str], str]) -> None:
        self.funcs = list(*funcs)
        self.move_cursor_to_end = move_cursor_to_end

    def __repr__(self) -> str:
        return f"Chain validator {', '.join(self.funcs)}"

    def validate(self, document: Document) -> None:
        for validator_func in self.funcs:
            error_message = validator_func(document.text)
            if error_message is not None:
                if self.move_cursor_to_end:
                    index = len(document.text)
                else:
                    index = 0
                raise ValidationError(cursor_position=index, message=error_message)
