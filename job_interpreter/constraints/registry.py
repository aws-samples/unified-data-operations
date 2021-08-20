from .exists import ExistsConstraint
from .not_null import NotNullConstraint
from .distinct import DistinctConstraint
from .regexp import RegExpConstraint


class ConstraintRegistry:
    constraints = {}

    def __init__(self):
        self.register('exist', ExistsConstraint)
        self.register('not_null', NotNullConstraint)
        self.register('distinct', DistinctConstraint)
        self.register('regexp', RegExpConstraint)

    def register(self, constraint_type, constraint):
        self.constraints[constraint_type] = constraint

    def get(self, constraint_type):
        return self.constraints.get(constraint_type)
