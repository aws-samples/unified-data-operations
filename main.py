import os
import argparse
import driver

if __name__ == '__main__':
    print(f'PATH: {os.environ["PATH"]}')
    print(f'SPARK_HOME: {os.environ.get("SPARK_HOME")}')
    print(f'PYTHONPATH: {os.environ.get("PYTHONPATH")}')
    parser = argparse.ArgumentParser()
    driver.init()
    driver.process_product(f'{os.path.dirname(os.path.abspath(__file__))}/tests/assets/')
