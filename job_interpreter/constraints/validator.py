from typing import List

from .validation_result import ValidationResult


class ConstraintValidator:
    constraints: List

    def __init__(self, constraints: List):
        self.constraints = constraints

    def validate(self) -> List[ValidationResult]:
        validation_results = []

        for constraint in self.constraints:
            result = constraint.validate()
            if not result.valid:
                validation_results.append(constraint.validate())

        return validation_results
