import importlib


class DynamicProcessor:
    function: str

    def __init__(self, function):
        self.function = function

    def process(self, datasets: [dict]) -> [dict]:
        function = importlib.import_module(self.function)
        return function(datasets)
1