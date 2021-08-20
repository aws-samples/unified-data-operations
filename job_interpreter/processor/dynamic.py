class DynamicProcessor:
    function: str

    def __init__(self, function):
        self.function = function

    def process(self, datasets: [dict]) -> [dict]:
        # TODO: call dynamic function
        return datasets
