from typing import List


class ValidationResult:
    constraint: str
    column: str
    valid: bool
    expected: object
    actual: object
    message: str

    def __init__(self, constraint: str, column: str, valid: bool, expected: object, actual: object):
        self.constraint = constraint
        self.column = column
        self.valid = valid
        self.expected = expected
        self.actual = actual

    def to_string(self):
        return '{} {}: expected {} but found {}'.format(
            self.column,
            self.constraint,
            self.expected,
            self.actual
        )


def to_string(validation_results: List[ValidationResult]) -> str:
    result = 'Constraint validation has failed.\n'

    for validation_result in validation_results:
        result = result + '- ' + validation_result.to_string() + "\n"

    return result