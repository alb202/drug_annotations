"""Run a Dagster pipeline from normal Python script."""
from src.run import main

if __name__ == "__main__":
    result = main.execute_in_process()
