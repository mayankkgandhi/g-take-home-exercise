import argparse
import traceback
import sys
from python_json_config import ConfigBuilder
from pipeline import Pipeline


def main():
    parser = argparse.ArgumentParser(description='ETL Pipeline')
    parser.add_argument('config_path',
                        help='Configuration path')
    args = parser.parse_args()

    try:
        # loading json config
        builder = ConfigBuilder()
        pipeline_config = builder.parse_config(args.config_path)

        # pass extract, transform and load from pipeline_config object
        extractor = pipeline_config.extract
        transformer = pipeline_config.transform
        loader = pipeline_config.load

        # Run ETL Pipeline
        custom_pipeline = Pipeline(extractor, transformer, loader)
        custom_pipeline.run()

    except ValueError:
        print("Invalid config")
        traceback.print_exc(file=sys.stdout)
    except Exception:
        print("Some error occurred")
        traceback.print_exc(file=sys.stdout)


if __name__ == "__main__":
    main()
